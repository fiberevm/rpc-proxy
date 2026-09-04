package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/config"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
	"github.com/fiberevm/rpc-proxy/internal/telemetry"
)

const maxUpstreamResponseBytes = 64 << 20

type Client struct {
	ID            string
	HTTPURL       string
	WebsocketURL  string
	Headers       map[string]string
	Family        string
	Chain         string
	http          *http.Client
	sem           chan struct{}
	telemetry     *telemetry.Telemetry
	requestID     atomic.Uint64
	health        healthState
	eip1898       atomic.Bool
	unsupported   sync.Map
	pinnedMethods sync.Map
}

type healthState struct {
	mu          sync.RWMutex
	latencyEWMA time.Duration
	successEWMA float64
	failures    int
	openUntil   time.Time
	lastError   string
}

type Status struct {
	ID            string        `json:"id"`
	Healthy       bool          `json:"healthy"`
	LatencyEWMA   time.Duration `json:"latency_ewma"`
	LastError     string        `json:"last_error,omitempty"`
	EIP1898       bool          `json:"eip_1898"`
	SuccessRate   float64       `json:"success_rate"`
	Unsupported   []string      `json:"unsupported_methods,omitempty"`
	PinnedMethods []string      `json:"pinned_state_methods,omitempty"`
}

// RPCResponse preserves an upstream protocol result or protocol error separately from transport failures.
type RPCResponse struct {
	Result json.RawMessage
	Error  *jsonrpc.Error
}

// Options contains the validated upstream configuration and injected telemetry service.
type Options struct {
	Config    config.UpstreamConfig
	Chain     string
	Family    string
	Telemetry *telemetry.Telemetry
}

// NewClient creates a concurrency-limited JSON-RPC client for one configured upstream.
func NewClient(options Options) *Client {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 500 * time.Millisecond, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          1024,
		MaxIdleConnsPerHost:   256,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   2 * time.Second,
		ResponseHeaderTimeout: 4 * time.Second,
	}
	return &Client{
		ID:           options.Config.ID,
		HTTPURL:      options.Config.HTTPURL,
		WebsocketURL: options.Config.WebsocketURL,
		Headers:      options.Config.Headers,
		Family:       options.Family,
		Chain:        options.Chain,
		http:         &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
		sem:          make(chan struct{}, options.Config.MaxConcurrency),
		telemetry:    options.Telemetry,
	}
}

// SupportsEIP1898 reports whether capability probing proved exact block-hash state reads.
func (c *Client) SupportsEIP1898() bool { return c.eip1898.Load() }

// SetEIP1898 records whether capability probing proved exact block-hash state reads.
func (c *Client) SetEIP1898(supported bool) { c.eip1898.Store(supported) }

// SetPinnedMethod records positive and negative hash-probe results for one state method.
func (c *Client) SetPinnedMethod(method string, supported bool) {
	c.pinnedMethods.Store(method, supported)
}

// SupportsPinnedMethod reports whether this exact method has passed hash-pinning probes.
func (c *Client) SupportsPinnedMethod(method string) bool {
	supported, exists := c.pinnedMethods.Load(method)
	return exists && supported.(bool)
}

// SupportsMethod reports whether this process has observed a method-not-found response for method.
func (c *Client) SupportsMethod(method string) bool {
	_, unsupported := c.unsupported.Load(method)
	return !unsupported
}

// MarkMethodUnsupported prevents future attempts for a method rejected as unavailable by this upstream.
func (c *Client) MarkMethodUnsupported(method string) { c.unsupported.Store(method, struct{}{}) }

// Healthy reports whether the client's local failure circuit permits another request.
func (c *Client) Healthy() bool {
	c.health.mu.RLock()
	defer c.health.mu.RUnlock()
	return time.Now().After(c.health.openUntil)
}

// Score returns the latency and success-rate score used to rank healthy upstreams.
func (c *Client) Score() time.Duration {
	c.health.mu.RLock()
	defer c.health.mu.RUnlock()
	latency := c.health.latencyEWMA
	if latency == 0 {
		latency = time.Second
	}
	successRate := c.health.successEWMA
	if successRate < 0.1 {
		successRate = 0.1
	}
	return time.Duration(float64(latency) / successRate)
}

// Status returns a redacted operational snapshot for the private status endpoint.
func (c *Client) Status() Status {
	c.health.mu.RLock()
	defer c.health.mu.RUnlock()
	status := Status{ID: c.ID, Healthy: time.Now().After(c.health.openUntil), LatencyEWMA: c.health.latencyEWMA, LastError: c.health.lastError, EIP1898: c.eip1898.Load(), SuccessRate: c.health.successEWMA}
	c.unsupported.Range(func(key, _ any) bool {
		status.Unsupported = append(status.Unsupported, key.(string))
		return true
	})
	sort.Strings(status.Unsupported)
	c.pinnedMethods.Range(func(method, supported any) bool {
		if supported.(bool) {
			status.PinnedMethods = append(status.PinnedMethods, method.(string))
		}
		return true
	})
	sort.Strings(status.PinnedMethods)
	return status
}

// Call sends one idempotent JSON-RPC read and returns its protocol response separately from transport errors.
func (c *Client) Call(ctx context.Context, method string, parameters json.RawMessage) (RPCResponse, error) {
	ctx, finishSpan := c.telemetry.Span(ctx, "rpc.upstream", "chain:"+c.Chain, "upstream:"+c.ID, "method:"+method, "family:"+c.Family)
	var callErr error
	defer func() { finishSpan(callErr) }()
	if !c.Healthy() {
		callErr = errors.New("upstream circuit is open")
		return RPCResponse{}, callErr
	}
	select {
	case c.sem <- struct{}{}:
		defer func() { <-c.sem }()
	case <-ctx.Done():
		callErr = ctx.Err()
		return RPCResponse{}, callErr
	}
	id := c.requestID.Add(1)
	request := struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      uint64          `json:"id"`
		Method  string          `json:"method"`
		Params  json.RawMessage `json:"params"`
	}{JSONRPC: jsonrpc.Version, ID: id, Method: method, Params: parameters}
	if len(request.Params) == 0 {
		request.Params = json.RawMessage("[]")
	}
	body, err := json.Marshal(request)
	if err != nil {
		callErr = fmt.Errorf("encode upstream request: %w", err)
		return RPCResponse{}, callErr
	}
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, c.HTTPURL, bytes.NewReader(body))
	if err != nil {
		callErr = errors.New("create upstream request: invalid endpoint")
		return RPCResponse{}, callErr
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	for headerName, headerValue := range c.Headers {
		httpRequest.Header.Set(headerName, headerValue)
	}
	started := time.Now()
	tags := []string{"chain:" + c.Chain, "upstream:" + c.ID, "method:" + method, "family:" + c.Family}
	outcome := "transport_error"
	defer func() {
		c.telemetry.Distribution("upstream.request.duration", float64(time.Since(started).Microseconds())/1000, tags...)
		c.telemetry.Count("upstream.request", 1, append(tags, "outcome:"+outcome)...)
	}()
	response, err := c.http.Do(httpRequest)
	if err != nil {
		var endpointErr *url.Error
		if errors.As(err, &endpointErr) {
			err = endpointErr.Err
		}
		callErr = fmt.Errorf("send upstream request: %w", err)
		c.recordFailure(callErr)
		return RPCResponse{}, callErr
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		err := fmt.Errorf("upstream http status %d", response.StatusCode)
		callErr = err
		c.recordFailure(err)
		outcome = "http_error"
		return RPCResponse{}, err
	}
	limited := io.LimitReader(response.Body, maxUpstreamResponseBytes+1)
	responseBody, err := io.ReadAll(limited)
	if err != nil {
		callErr = fmt.Errorf("read upstream response: %w", err)
		c.recordFailure(callErr)
		return RPCResponse{}, callErr
	}
	if len(responseBody) > maxUpstreamResponseBytes {
		err := errors.New("upstream response exceeds limit")
		callErr = err
		c.recordFailure(err)
		return RPCResponse{}, err
	}
	outcome = "invalid_response"
	var rpcResponse jsonrpc.Response
	if err := json.Unmarshal(responseBody, &rpcResponse); err != nil {
		callErr = err
		c.recordFailure(err)
		return RPCResponse{}, fmt.Errorf("decode upstream response: %w", err)
	}
	if rpcResponse.JSONRPC != jsonrpc.Version {
		err := errors.New("invalid jsonrpc version from upstream")
		callErr = err
		c.recordFailure(err)
		return RPCResponse{}, err
	}
	if string(rpcResponse.ID) != fmt.Sprint(id) {
		err := errors.New("upstream returned a mismatched jsonrpc id")
		callErr = err
		c.recordFailure(err)
		return RPCResponse{}, err
	}
	if (len(rpcResponse.Result) > 0) == (rpcResponse.Error != nil) || (rpcResponse.Error != nil && rpcResponse.Error.Message == "") {
		callErr = errors.New("upstream must return exactly one result or valid error")
		c.recordFailure(callErr)
		return RPCResponse{}, callErr
	}
	c.recordSuccess(time.Since(started))
	outcome = "success"
	if rpcResponse.Error != nil {
		outcome = "rpc_error"
		// Mark the span without exporting arbitrary provider error text or payloads.
		callErr = fmt.Errorf("upstream JSON-RPC error %d", rpcResponse.Error.Code)
	}
	return RPCResponse{Result: rpcResponse.Result, Error: rpcResponse.Error}, nil
}

func (c *Client) recordSuccess(duration time.Duration) {
	c.health.mu.Lock()
	defer c.health.mu.Unlock()
	if c.health.latencyEWMA == 0 {
		c.health.latencyEWMA = duration
	} else {
		c.health.latencyEWMA = (c.health.latencyEWMA*8 + duration*2) / 10
	}
	if c.health.successEWMA == 0 {
		c.health.successEWMA = 1
	} else {
		c.health.successEWMA = c.health.successEWMA*0.9 + 0.1
	}
	c.health.failures = 0
	c.health.lastError = ""
	c.health.openUntil = time.Time{}
}

func (c *Client) recordFailure(err error) {
	c.health.mu.Lock()
	defer c.health.mu.Unlock()
	c.health.failures++
	if c.health.successEWMA == 0 {
		c.health.successEWMA = 0.5
	} else {
		c.health.successEWMA *= 0.9
	}
	c.health.lastError = c.sanitizeError(err.Error())
	if c.health.failures >= 5 {
		c.health.openUntil = time.Now().Add(10 * time.Second)
		c.health.failures = 0
	}
}

func (c *Client) sanitizeError(message string) string {
	message = strings.ReplaceAll(message, c.HTTPURL, "[REDACTED_URL]")
	for _, credential := range c.Headers {
		if credential != "" {
			message = strings.ReplaceAll(message, credential, "[REDACTED]")
		}
	}
	for _, marker := range []string{"api_key=", "apikey=", "token=", "key="} {
		if index := strings.Index(strings.ToLower(message), marker); index >= 0 {
			return message[:index] + marker + "[REDACTED]"
		}
	}
	return message
}
