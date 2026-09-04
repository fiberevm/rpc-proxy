package telemetry

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/DataDog/datadog-go/v5/statsd"
	"github.com/DataDog/dd-trace-go/v2/ddtrace/tracer"
	"github.com/DataDog/dd-trace-go/v2/profiler"
	"github.com/fiberevm/rpc-proxy/internal/config"
)

type Telemetry struct {
	stats      *statsd.Client
	trace      bool
	profile    bool
	activeHTTP atomic.Int64
	metricErrs atomic.Uint64
	redactor   *credentialRedactor
}

type credentialRedactor struct {
	credentialInURL *regexp.Regexp
	secretParameter *regexp.Regexp
}

// NewTelemetry creates optional Datadog metrics, tracing, and profiling clients from validated configuration.
func NewTelemetry(configuration config.DatadogConfig) (*Telemetry, error) {
	telemetryService := &Telemetry{redactor: newCredentialRedactor()}
	if !configuration.Enabled {
		return telemetryService, nil
	}
	client, err := statsd.New(configuration.StatsdAddress,
		statsd.WithNamespace("rpc_proxy."),
		statsd.WithTags([]string{"service:" + configuration.Service, "env:" + configuration.Environment, "version:" + configuration.Version}),
		statsd.WithExtendedClientSideAggregation(),
	)
	if err != nil {
		return nil, fmt.Errorf("create dogstatsd client: %w", err)
	}
	telemetryService.stats = client
	tracer.Start(tracer.WithService(configuration.Service), tracer.WithEnv(configuration.Environment), tracer.WithServiceVersion(configuration.Version), tracer.WithAgentAddr(configuration.AgentAddress))
	telemetryService.trace = true
	if configuration.ProfilingEnabled {
		if err := profiler.Start(profiler.WithService(configuration.Service), profiler.WithEnv(configuration.Environment), profiler.WithVersion(configuration.Version), profiler.WithAgentAddr(configuration.AgentAddress)); err != nil {
			closeErr := client.Close()
			tracer.Stop()
			if closeErr != nil {
				return nil, errors.Join(fmt.Errorf("start datadog profiler: %w", err), fmt.Errorf("close dogstatsd client: %w", closeErr))
			}
			return nil, fmt.Errorf("start datadog profiler: %w", err)
		}
		telemetryService.profile = true
	}
	return telemetryService, nil
}

// NewLogger creates the structured stdout logger and redacts endpoint URLs and error attributes.
func NewLogger() *slog.Logger {
	redactor := newCredentialRedactor()
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:       slog.LevelInfo,
		ReplaceAttr: redactor.replaceAttr,
	}))
}

func newCredentialRedactor() *credentialRedactor {
	return &credentialRedactor{
		credentialInURL: regexp.MustCompile(`(?i)(https?|wss?|rediss?)://[^\s"<>]+`),
		secretParameter: regexp.MustCompile(`(?i)(api[_-]?key|token|secret|password|authorization)=([^&\s]+)`),
	}
}

func (r *credentialRedactor) redact(message string) string {
	// Provider keys frequently live in URL paths, not just named query parameters.
	redacted := r.credentialInURL.ReplaceAllString(message, `[REDACTED_URL]`)
	return r.secretParameter.ReplaceAllString(redacted, `${1}=[REDACTED]`)
}

func (r *credentialRedactor) replaceAttr(_ []string, attr slog.Attr) slog.Attr {
	if attr.Value.Kind() == slog.KindAny {
		if err, ok := attr.Value.Any().(error); ok {
			attr.Value = slog.StringValue(r.redact(err.Error()))
		}
	}
	if attr.Value.Kind() == slog.KindString {
		attr.Value = slog.StringValue(r.redact(attr.Value.String()))
	}
	return attr
}

// Close flushes metrics and stops any tracer or profiler started by NewTelemetry.
func (t *Telemetry) Close() error {
	var closeErrors []error
	if t.profile {
		profiler.Stop()
	}
	if t.trace {
		tracer.Stop()
	}
	if t.stats != nil {
		if err := t.stats.Close(); err != nil {
			closeErrors = append(closeErrors, fmt.Errorf("close dogstatsd client: %w", err))
		}
	}
	return errors.Join(closeErrors...)
}

// Count submits a monotonic event count with bounded tags; UDP submission failures are tracked locally.
func (t *Telemetry) Count(name string, measurement int64, tags ...string) {
	if t.stats != nil {
		if err := t.stats.Count(name, measurement, tags, 1); err != nil {
			t.metricErrs.Add(1)
		}
	}
}

// Gauge submits the latest measurement with bounded tags; UDP submission failures are tracked locally.
func (t *Telemetry) Gauge(name string, measurement float64, tags ...string) {
	if t.stats != nil {
		if err := t.stats.Gauge(name, measurement, tags, 1); err != nil {
			t.metricErrs.Add(1)
		}
	}
}

// Distribution submits one latency or size sample with bounded tags; UDP submission failures are tracked locally.
func (t *Telemetry) Distribution(name string, measurement float64, tags ...string) {
	if t.stats != nil {
		if err := t.stats.Distribution(name, measurement, tags, 1); err != nil {
			t.metricErrs.Add(1)
		}
	}
}

// HTTPStarted increments and publishes the active inbound request gauge.
func (t *Telemetry) HTTPStarted() { t.Gauge("requests.active", float64(t.activeHTTP.Add(1))) }

// HTTPFinished decrements and publishes the active inbound request gauge.
func (t *Telemetry) HTTPFinished() { t.Gauge("requests.active", float64(t.activeHTTP.Add(-1))) }

// FinishSpan records a terminal error and bounded tags before closing a span.
type FinishSpan func(err error, tags ...string)

// Span starts a Datadog span named name and returns its derived context and completion callback.
func (t *Telemetry) Span(ctx context.Context, name string, tags ...string) (context.Context, FinishSpan) {
	if !t.trace {
		return ctx, func(error, ...string) {}
	}
	options := make([]tracer.StartSpanOption, 0, len(tags))
	for _, tag := range tags {
		tagName, tagValue, ok := strings.Cut(tag, ":")
		if ok {
			options = append(options, tracer.Tag(tagName, tagValue))
		}
	}
	span, next := tracer.StartSpanFromContext(ctx, name, options...)
	started := time.Now()
	return next, func(err error, extra ...string) {
		for _, tag := range extra {
			tagName, tagValue, ok := strings.Cut(tag, ":")
			if ok {
				span.SetTag(tagName, tagValue)
			}
		}
		if err != nil {
			span.SetTag("error", errors.New(t.Redact(err.Error())))
		}
		span.SetTag("duration_ms", float64(time.Since(started).Microseconds())/1000)
		span.Finish()
	}
}

// Redact removes endpoint URLs and recognizable credentials from operational error text.
func (t *Telemetry) Redact(message string) string { return t.redactor.redact(message) }

// MetricSubmissionErrors returns the local count of failed DogStatsD submissions for status reporting.
func (t *Telemetry) MetricSubmissionErrors() uint64 { return t.metricErrs.Load() }

// TraceAttrs provides Datadog's reserved correlation fields without logging
// JSON-RPC IDs, addresses, hashes, credentials, or client identities.
func (t *Telemetry) TraceAttrs(ctx context.Context) []any {
	if !t.trace {
		return nil
	}
	span, ok := tracer.SpanFromContext(ctx)
	if !ok {
		return nil
	}
	return []any{"dd.trace_id", span.Context().TraceID(), "dd.span_id", strconv.FormatUint(span.Context().SpanID(), 10)}
}
