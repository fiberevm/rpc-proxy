package gateway

import (
	"context"
	"net/http"
	"slices"

	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
)

type clientMethod struct {
	client    string
	transport string
	runtime   *chain.Runtime
	request   jsonrpc.Request
}

func (g *Gateway) getClient(request *http.Request) string {
	client := request.Header.Get("X-RPC-Client")
	if client == "" {
		// Browser WebSocket clients and URL-only RPC clients cannot set headers.
		client = request.URL.Query().Get("client")
	}
	if client == "" {
		return "anonymous"
	}
	if slices.Contains(g.config.Clients, client) {
		return client
	}
	return "unknown"
}

func (g *Gateway) recordClientMethod(ctx context.Context, usage clientMethod) {
	method := g.metricMethod(usage.request.Method, usage.runtime.Config.Family)
	if usage.request.Validate() != nil {
		method = "invalid"
	}
	// Count incoming methods before routing so cache hits, notifications, and
	// rejected calls remain attributable without counting upstream retries twice.
	g.telemetry.Count("client.method", 1,
		"client:"+usage.client, "chain:"+usage.runtime.Config.Name,
		"family:"+usage.runtime.Config.Family, "method:"+method, "transport:"+usage.transport)
	attributes := []any{"client", usage.client, "chain", usage.runtime.Config.Name,
		"family", usage.runtime.Config.Family, "method", method, "transport", usage.transport}
	attributes = append(attributes, g.telemetry.TraceAttrs(ctx)...)
	g.logger.InfoContext(ctx, "rpc method requested", attributes...)
}
