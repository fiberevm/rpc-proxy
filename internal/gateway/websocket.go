package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/fiberevm/rpc-proxy/internal/chain"
	"github.com/fiberevm/rpc-proxy/internal/head"
	"github.com/fiberevm/rpc-proxy/internal/jsonrpc"
)

type wsSession struct {
	client      string
	gateway     *Gateway
	runtime     *chain.Runtime
	conn        *websocket.Conn
	ctx         context.Context
	cancel      context.CancelFunc
	writes      chan []byte
	mu          sync.RWMutex
	subs        map[string]bool
	pumpStarted bool
}

// WebsocketHandler returns the EVM-only accepted-head subscription handler mounted at /ws/{chain}.
func (g *Gateway) WebsocketHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		chainName := request.PathValue("chain")
		if chainName == "" {
			chainName = strings.TrimPrefix(request.URL.Path, "/ws/")
		}
		runtime, ok := g.runtimes[chainName]
		if !ok || runtime.Config.Family != "evm" {
			http.Error(w, "websocket newHeads is available only for configured EVM chains", http.StatusNotFound)
			return
		}
		conn, err := websocket.Accept(w, request, &websocket.AcceptOptions{CompressionMode: websocket.CompressionDisabled})
		if err != nil {
			return
		}
		ctx, cancel := context.WithCancel(request.Context())
		client := g.getClient(request)
		session := &wsSession{client: client, gateway: g, runtime: runtime, conn: conn, ctx: ctx, cancel: cancel, writes: make(chan []byte, g.config.Server.WebsocketQueueSize), subs: map[string]bool{}}
		g.telemetry.Count("ws.connection", 1, "chain:"+chainName, "state:opened", "client:"+client)
		defer func() {
			cancel()
			conn.CloseNow()
			g.telemetry.Count("ws.connection", 1, "chain:"+chainName, "state:closed", "client:"+client)
		}()
		go session.writeLoop()
		session.readLoop()
	})
}

func (s *wsSession) readLoop() {
	for s.ctx.Err() == nil {
		_, payload, err := s.conn.Read(s.ctx)
		if err != nil {
			return
		}
		var request jsonrpc.Request
		if err := json.Unmarshal(payload, &request); err != nil {
			s.enqueueResponse(jsonrpc.Failure(nil, jsonrpc.ParseError(err.Error())))
			continue
		}
		s.gateway.recordClientMethod(s.ctx, clientMethod{client: s.client, transport: "websocket", runtime: s.runtime, request: request})
		if rpcErr := request.Validate(); rpcErr != nil {
			s.enqueueResponse(jsonrpc.Failure(request.ID, rpcErr))
			continue
		}
		if request.IsNotification() {
			if request.Method == "eth_unsubscribe" {
				parameters, rpcErr := jsonrpc.ParseParams(request.Params)
				var id string
				if rpcErr == nil && len(parameters) == 1 && json.Unmarshal(parameters[0], &id) == nil {
					s.mu.Lock()
					delete(s.subs, id)
					s.mu.Unlock()
				}
			}
			continue
		}
		switch request.Method {
		case "eth_subscribe":
			s.subscribe(request)
		case "eth_unsubscribe":
			s.unsubscribe(request)
		default:
			s.enqueueResponse(jsonrpc.Failure(request.ID, jsonrpc.UnsupportedConsistency(request.Method, "only newHeads subscriptions are supported over WebSocket")))
		}
	}
}

func (s *wsSession) subscribe(request jsonrpc.Request) {
	parameters, rpcErr := jsonrpc.ParseParams(request.Params)
	if rpcErr != nil {
		s.enqueueResponse(jsonrpc.Failure(request.ID, rpcErr))
		return
	}
	if len(parameters) != 1 {
		s.enqueueResponse(jsonrpc.Failure(request.ID, jsonrpc.InvalidParams("eth_subscribe requires exactly [\"newHeads\"]")))
		return
	}
	var kind string
	if err := json.Unmarshal(parameters[0], &kind); err != nil {
		s.enqueueResponse(jsonrpc.Failure(request.ID, jsonrpc.InvalidParams("eth_subscribe requires a string subscription kind")))
		return
	}
	if kind != "newHeads" {
		s.enqueueResponse(jsonrpc.Failure(request.ID, jsonrpc.UnsupportedConsistency("eth_subscribe", "only newHeads is supported")))
		return
	}
	var checkpoint head.Event
	if !s.pumpStarted {
		queryCtx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
		redisCtx, finish := s.gateway.telemetry.Span(queryCtx, "redis.stream_start", "chain:"+s.runtime.Config.Name, "operation:stream_start")
		var err error
		checkpoint, err = s.gateway.store.StreamStart(redisCtx, s.runtime.Config.Name)
		finish(err)
		var accepted head.Head
		if err == nil {
			var snapshot head.Snapshot
			snapshot, err = s.gateway.store.Snapshot(queryCtx, s.runtime.Config.Name)
			accepted, _ = snapshot.Get(head.Latest)
		}
		cancel()
		if err != nil {
			s.gateway.telemetry.Count("redis.failure", 1, "chain:"+s.runtime.Config.Name, "operation:stream_start")
			s.enqueueResponse(jsonrpc.Failure(request.ID, jsonrpc.ConsistencyUnavailable("head stream unavailable")))
			return
		}
		if accepted.ReorgPending || s.runtime.Config.MaxHeadAge.Value() > 0 && (accepted.IsZero() || time.Since(accepted.ObservedAt) > s.runtime.Config.MaxHeadAge.Value()) {
			s.enqueueResponse(jsonrpc.Failure(request.ID, jsonrpc.ConsistencyUnavailable("accepted head is stale or reorg pending")))
			return
		}
	}
	id, err := s.newSubscriptionID()
	if err != nil {
		s.enqueueResponse(jsonrpc.Failure(request.ID, jsonrpc.InternalError("create subscription id")))
		return
	}
	encodedID, err := json.Marshal(id)
	if err != nil {
		s.enqueueResponse(jsonrpc.Failure(request.ID, jsonrpc.InternalError("encode subscription id")))
		return
	}
	s.mu.Lock()
	if len(s.subs) >= 128 {
		s.mu.Unlock()
		s.enqueueResponse(jsonrpc.Failure(request.ID, jsonrpc.InvalidRequest("subscription limit reached")))
		return
	}
	s.subs[id] = true
	s.enqueueResponse(jsonrpc.Success(request.ID, encodedID))
	s.mu.Unlock()
	if !s.pumpStarted {
		s.pumpStarted = true
		go s.eventLoop(checkpoint)
	}
}

func (s *wsSession) unsubscribe(request jsonrpc.Request) {
	parameters, rpcErr := jsonrpc.ParseParams(request.Params)
	if rpcErr != nil {
		s.enqueueResponse(jsonrpc.Failure(request.ID, rpcErr))
		return
	}
	if len(parameters) != 1 {
		s.enqueueResponse(jsonrpc.Failure(request.ID, jsonrpc.InvalidParams("eth_unsubscribe requires one subscription id")))
		return
	}
	var id string
	if err := json.Unmarshal(parameters[0], &id); err != nil {
		s.enqueueResponse(jsonrpc.Failure(request.ID, jsonrpc.InvalidParams("invalid subscription id")))
		return
	}
	s.mu.Lock()
	existed := s.subs[id]
	delete(s.subs, id)
	s.mu.Unlock()
	encodedStatus, err := json.Marshal(existed)
	if err != nil {
		s.enqueueResponse(jsonrpc.Failure(request.ID, jsonrpc.InternalError("encode unsubscribe status")))
		return
	}
	s.enqueueResponse(jsonrpc.Success(request.ID, encodedStatus))
}

func (s *wsSession) eventLoop(checkpoint head.Event) {
	cursor := checkpoint.ID
	seen := map[string]bool{}
	history := map[string]head.Head{}
	historyOrder := make([]string, 0, max(16, s.runtime.Config.ReorgDepth*2))
	last := checkpoint.Head
	if !last.IsZero() {
		history[last.Hash] = last
		historyOrder = append(historyOrder, last.Hash)
	}
	for s.ctx.Err() == nil {
		redisCtx, finishRedis := s.gateway.telemetry.Span(s.ctx, "redis.read_events", "chain:"+s.runtime.Config.Name, "operation:read_events")
		events, err := s.gateway.store.ReadEvents(redisCtx, s.runtime.Config.Name, cursor, 2*time.Second, 64)
		finishRedis(err)
		if err != nil {
			s.gateway.telemetry.Count("redis.failure", 1, "chain:"+s.runtime.Config.Name, "operation:read_events")
			s.gateway.telemetry.Count("ws.recovery_failure", 1, "chain:"+s.runtime.Config.Name)
			s.closeWithError("head stream read failed")
			return
		}
		snapshot, err := s.gateway.store.Snapshot(s.ctx, s.runtime.Config.Name)
		accepted, ok := snapshot.Get(head.Latest)
		if err != nil || accepted.ReorgPending || s.runtime.Config.MaxHeadAge.Value() > 0 && (!ok || time.Since(accepted.ObservedAt) > s.runtime.Config.MaxHeadAge.Value()) {
			s.closeWithError("accepted head is stale, unavailable, or reorg pending")
			return
		}
		for _, event := range events {
			cursor = event.ID
			path, err := s.expandPath(last, event.Head, history)
			if err != nil {
				s.gateway.telemetry.Count("ws.recovery_failure", 1, "chain:"+s.runtime.Config.Name)
				s.closeWithError("head history gap exceeds recovery window")
				return
			}
			if len(path) > 1 {
				s.gateway.telemetry.Count("ws.backfilled_heads", int64(len(path)-1), "chain:"+s.runtime.Config.Name)
			}
			for _, acceptedHead := range path {
				last = acceptedHead
				if seen[acceptedHead.Hash] {
					s.gateway.telemetry.Count("ws.duplicate_suppressed", 1, "chain:"+s.runtime.Config.Name)
					continue
				}
				if len(seen) >= 65536 {
					s.closeWithError("subscription deduplication window exhausted; reconnect")
					return
				}
				seen[acceptedHead.Hash] = true
				if _, exists := history[acceptedHead.Hash]; !exists {
					historyOrder = append(historyOrder, acceptedHead.Hash)
				}
				history[acceptedHead.Hash] = acceptedHead
				if limit := max(16, s.runtime.Config.ReorgDepth*2); len(historyOrder) > limit {
					delete(history, historyOrder[0])
					historyOrder = historyOrder[1:]
				}
				s.notify(acceptedHead)
			}
		}
	}
}

func (s *wsSession) expandPath(last, next head.Head, history map[string]head.Head) ([]head.Head, error) {
	if last.IsZero() {
		return []head.Head{next}, nil
	}
	if next.Hash == last.Hash {
		return nil, nil
	}
	// A one-block reorg legitimately emits a different hash at the same height.
	// The shared parent proves that no intermediate header is missing.
	if next.Number == last.Number && strings.EqualFold(next.ParentHash, last.ParentHash) {
		return []head.Head{next}, nil
	}
	if next.Number > last.Number && next.Number-last.Number == 1 && strings.EqualFold(next.ParentHash, last.Hash) {
		return []head.Head{next}, nil
	}
	path := []head.Head{next}
	cursor := next
	for depth := 0; depth < s.runtime.Config.ReorgDepth; depth++ {
		if strings.EqualFold(cursor.ParentHash, last.Hash) {
			if cursor.Number <= last.Number || cursor.Number-last.Number != 1 {
				return nil, errors.New("invalid header height linkage")
			}
			s.reverseHeads(path)
			return path, nil
		}
		if ancestor, ok := history[cursor.ParentHash]; ok {
			if cursor.Number <= ancestor.Number || cursor.Number-ancestor.Number != 1 {
				return nil, errors.New("invalid ancestor height linkage")
			}
			s.reverseHeads(path)
			return path, nil
		}
		parent, err := s.fetchBlock(cursor.ParentHash, cursor.Origin)
		if err != nil {
			return nil, err
		}
		if parent.Number >= cursor.Number || cursor.Number-parent.Number != 1 {
			return nil, errors.New("invalid backfill parent height")
		}
		path = append(path, parent)
		cursor = parent
	}
	return nil, errors.New("common ancestor not found")
}

func (s *wsSession) fetchBlock(hashValue, preferredID string) (head.Head, error) {
	ctx, cancel := context.WithTimeout(s.ctx, 2*time.Second)
	defer cancel()
	blockRequest := chain.EVMBlockByHashRequest{Hash: hashValue, Commitment: head.Latest, PreferredUpstreamID: preferredID}
	block, err := s.runtime.EVMBlockByHash(ctx, blockRequest)
	if err != nil {
		return head.Head{}, fmt.Errorf("block %s unavailable: %w", hashValue, err)
	}
	return block, nil
}

func (s *wsSession) reverseHeads(heads []head.Head) {
	for left, right := 0, len(heads)-1; left < right; left, right = left+1, right-1 {
		heads[left], heads[right] = heads[right], heads[left]
	}
}

func (s *wsSession) notify(acceptedHead head.Head) {
	s.mu.RLock()
	ids := make([]string, 0, len(s.subs))
	for id := range s.subs {
		ids = append(ids, id)
	}
	s.mu.RUnlock()
	sort.Strings(ids)
	for _, id := range ids {
		message := map[string]any{"jsonrpc": "2.0", "method": "eth_subscription", "params": map[string]any{"subscription": id, "result": json.RawMessage(acceptedHead.Header)}}
		encodedMessage, err := json.Marshal(message)
		if err != nil {
			s.gateway.logger.Error("encode websocket notification", "chain", s.runtime.Config.Name, "error", err)
			s.cancel()
			return
		}
		if !s.enqueue(encodedMessage) {
			s.gateway.telemetry.Count("ws.slow_client_disconnect", 1, "chain:"+s.runtime.Config.Name)
			s.cancel()
			return
		}
		s.gateway.telemetry.Count("ws.head", 1, "chain:"+s.runtime.Config.Name)
	}
}

func (s *wsSession) enqueueResponse(response jsonrpc.Response) {
	encodedResponse, err := json.Marshal(response)
	if err != nil {
		s.gateway.logger.Error("encode websocket response", "chain", s.runtime.Config.Name, "error", err)
		s.cancel()
		return
	}
	if !s.enqueue(encodedResponse) {
		s.cancel()
	}
}
func (s *wsSession) enqueue(payload []byte) bool {
	select {
	case s.writes <- payload:
		return true
	default:
		return false
	}
}

func (s *wsSession) writeLoop() {
	for {
		select {
		case <-s.ctx.Done():
			return
		case payload := <-s.writes:
			writeCtx, cancel := context.WithTimeout(s.ctx, 5*time.Second)
			err := s.conn.Write(writeCtx, websocket.MessageText, payload)
			cancel()
			if err != nil {
				s.cancel()
				return
			}
		}
	}
}

func (s *wsSession) closeWithError(reason string) {
	// Recovery and retention failures require reconnecting with a fresh checkpoint.
	if err := s.conn.Close(websocket.StatusTryAgainLater, reason); err != nil {
		s.gateway.logger.Debug("close websocket with error", "chain", s.runtime.Config.Name, "error", err)
	}
	s.cancel()
}

func (s *wsSession) newSubscriptionID() (string, error) {
	randomBytes := make([]byte, 16)
	if _, err := rand.Read(randomBytes); err != nil {
		return "", fmt.Errorf("read cryptographic randomness: %w", err)
	}
	return "0x" + hex.EncodeToString(randomBytes), nil
}
