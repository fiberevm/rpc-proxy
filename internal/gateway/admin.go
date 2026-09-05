package gateway

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/head"
)

// AdminHandler returns private liveness, readiness, and redacted status endpoints.
func (g *Gateway) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health/live", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		if _, err := w.Write([]byte("ok\n")); err != nil {
			g.logger.Error("write liveness response", "error", err)
		}
	})
	mux.HandleFunc("GET /health/ready", g.ready)
	mux.HandleFunc("GET /status", g.status)
	return mux
}

func (g *Gateway) ready(w http.ResponseWriter, request *http.Request) {
	ctx := request.Context()
	if err := g.store.Ping(ctx); err != nil {
		g.telemetry.Count("redis.failure", 1, "operation:health_ping")
		http.Error(w, "head store unavailable", http.StatusServiceUnavailable)
		return
	}
	for name, runtime := range g.runtimes {
		snapshot, err := g.store.Snapshot(ctx, name)
		if err != nil {
			g.telemetry.Count("redis.failure", 1, "chain:"+name, "operation:health_snapshot")
			http.Error(w, "head store unavailable", http.StatusServiceUnavailable)
			return
		}
		commitments := []string{head.Latest}
		if runtime.Config.Family == "solana" {
			commitments = []string{head.Processed, head.Confirmed, head.Finalized}
		}
		for _, commitment := range commitments {
			acceptedHead, ok := snapshot.Get(commitment)
			if acceptedHead.ReorgPending {
				http.Error(w, "reorg recovery in progress", http.StatusServiceUnavailable)
				return
			}
			if !ok || acceptedHead.IsZero() || (runtime.Config.MaxHeadAge.Value() > 0 && time.Since(acceptedHead.ObservedAt) > runtime.Config.MaxHeadAge.Value()) {
				http.Error(w, "accepted head is stale", http.StatusServiceUnavailable)
				return
			}
		}
		if len(runtime.Candidates(runtime.Config.Family == "evm")) == 0 {
			http.Error(w, "no eligible upstream", http.StatusServiceUnavailable)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
	if _, err := w.Write([]byte("ready\n")); err != nil {
		g.logger.Error("write readiness response", "error", err)
	}
}

func (g *Gateway) status(w http.ResponseWriter, request *http.Request) {
	type chainStatus struct {
		Runtime any                  `json:"runtime"`
		Heads   map[string]head.Head `json:"heads,omitempty"`
		Error   string               `json:"error,omitempty"`
	}
	statuses := map[string]chainStatus{}
	for name, runtime := range g.runtimes {
		status := chainStatus{Runtime: runtime.Status()}
		snapshot, err := g.store.Snapshot(request.Context(), name)
		if err != nil {
			status.Error = g.telemetry.Redact(err.Error())
		} else {
			status.Heads = make(map[string]head.Head)
			for commitment, accepted := range snapshot.Heads {
				if runtime.Config.Family == "evm" && commitment != head.Latest {
					continue
				}
				accepted.Header = nil
				status.Heads[commitment] = accepted
			}
		}
		statuses[name] = status
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{"chains": statuses, "metric_submission_errors": g.telemetry.MetricSubmissionErrors()}); err != nil {
		g.logger.ErrorContext(request.Context(), "write status response", "error", err)
	}
}
