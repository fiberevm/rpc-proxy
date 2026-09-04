package coordinator

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/upstream"
)

func (c *Coordinator) hasFreshSubscription(upstreamID string) bool {
	c.subscriptionMu.RLock()
	lastHead, active := c.subscriptions[upstreamID]
	c.subscriptionMu.RUnlock()
	return active && time.Since(lastHead) < c.runtime.Config.WebsocketIdleTimeout.Value()
}

func (c *Coordinator) setSubscriptionHead(upstreamID string, observedAt time.Time) {
	c.subscriptionMu.Lock()
	c.subscriptions[upstreamID] = observedAt
	c.subscriptionMu.Unlock()
	c.telemetry.Gauge("coordinator.subscription_active", 1, "chain:"+c.runtime.Config.Name, "upstream:"+upstreamID)
}

func (c *Coordinator) clearSubscription(upstreamID string) {
	c.subscriptionMu.Lock()
	delete(c.subscriptions, upstreamID)
	c.subscriptionMu.Unlock()
	c.telemetry.Gauge("coordinator.subscription_active", 0, "chain:"+c.runtime.Config.Name, "upstream:"+upstreamID)
}

func (c *Coordinator) watchEVMSession(ctx context.Context, candidate *upstream.Client, output chan<- observation) {
	defer c.clearSubscription(candidate.ID)
	headers := http.Header{}
	for headerName, headerValue := range candidate.Headers {
		headers.Set(headerName, headerValue)
	}
	setupCtx, cancelSetup := context.WithTimeout(ctx, c.runtime.Config.HeadRequestTimeout.Value())
	defer cancelSetup()
	conn, _, err := websocket.Dial(setupCtx, candidate.WebsocketURL, &websocket.DialOptions{
		HTTPHeader: headers,
		HTTPClient: &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }},
	})
	if err != nil {
		return
	}
	defer conn.CloseNow()
	conn.SetReadLimit(1 << 20)
	request := map[string]any{"jsonrpc": "2.0", "id": 1, "method": "eth_subscribe", "params": []any{"newHeads"}}
	if c.writeWebsocketJSON(setupCtx, conn, request) != nil {
		return
	}
	_, payload, err := conn.Read(setupCtx)
	if err != nil {
		return
	}
	var acknowledgment struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Result  string          `json:"result"`
		Error   json.RawMessage `json:"error"`
	}
	if json.Unmarshal(payload, &acknowledgment) != nil || acknowledgment.JSONRPC != "2.0" || string(acknowledgment.ID) != "1" || acknowledgment.Result == "" || len(acknowledgment.Error) != 0 {
		c.telemetry.Count("coordinator.subscription_error", 1, "chain:"+c.runtime.Config.Name, "upstream:"+candidate.ID, "failure_class:acknowledgment")
		return
	}
	cancelSetup()

	// Subscribe before bootstrapping so heads arriving during the HTTP read are not lost.
	queryCtx, cancelQuery := context.WithTimeout(ctx, c.runtime.Config.HeadRequestTimeout.Value())
	lastHead, err := c.pollOne(queryCtx, candidate, head.Latest)
	cancelQuery()
	if err != nil {
		return
	}
	c.recordProviderHead(lastHead)
	select {
	case output <- observation{head: lastHead}:
	case <-ctx.Done():
		return
	}
	c.setSubscriptionHead(candidate.ID, lastHead.ObservedAt)
	for ctx.Err() == nil {
		// Only a verified different head extends the deadline, not duplicates or unrelated frames.
		readCtx, cancelRead := context.WithDeadline(ctx, lastHead.ObservedAt.Add(c.runtime.Config.WebsocketIdleTimeout.Value()))
		_, payload, err := conn.Read(readCtx)
		cancelRead()
		if err != nil {
			if ctx.Err() == nil && time.Since(lastHead.ObservedAt) >= c.runtime.Config.WebsocketIdleTimeout.Value() {
				c.telemetry.Count("coordinator.subscription_error", 1, "chain:"+c.runtime.Config.Name, "upstream:"+candidate.ID, "failure_class:idle")
			}
			return
		}
		var notification struct {
			JSONRPC string `json:"jsonrpc"`
			Method  string `json:"method"`
			Params  struct {
				Subscription string          `json:"subscription"`
				Result       json.RawMessage `json:"result"`
			} `json:"params"`
		}
		if json.Unmarshal(payload, &notification) != nil || notification.JSONRPC != "2.0" || notification.Method != "eth_subscription" || notification.Params.Subscription != acknowledgment.Result {
			continue
		}
		observedHead, err := c.runtime.ParseEVMHead(chain.EVMHeadParseRequest{Commitment: head.Latest, Origin: candidate.ID, Header: notification.Params.Result})
		if err != nil {
			continue
		}
		if strings.EqualFold(observedHead.Hash, lastHead.Hash) {
			c.telemetry.Count("ws.duplicate_suppressed", 1, "chain:"+c.runtime.Config.Name, "upstream:"+candidate.ID)
			continue
		}
		if !c.runtime.IsValid(candidate.ID) || !candidate.Healthy() {
			return
		}
		queryCtx, cancelQuery := context.WithTimeout(ctx, c.runtime.Config.HeadRequestTimeout.Value())
		available, err := c.runtime.HasEVMBlock(queryCtx, candidate, observedHead)
		cancelQuery()
		if err != nil || !available {
			c.telemetry.Count("coordinator.ws_availability_error", 1, "chain:"+c.runtime.Config.Name, "upstream:"+candidate.ID)
			return
		}
		c.recordProviderHead(observedHead)
		select {
		case output <- observation{head: observedHead}:
		case <-ctx.Done():
			return
		}
		lastHead = observedHead
		c.setSubscriptionHead(candidate.ID, observedHead.ObservedAt)
	}
}
