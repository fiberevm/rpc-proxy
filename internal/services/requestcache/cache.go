// Package requestcache stores bounded RPC results and coalesces concurrent cache misses.
package requestcache

import (
	"bytes"
	"container/list"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/config"
	"golang.org/x/sync/singleflight"
)

// Response holds only the RPC result and its origin, never a caller's JSON-RPC ID.
type Response struct {
	Payload  json.RawMessage
	Upstream string
	// SkipCache allows a successful lookup to be returned without retaining an unverified result.
	SkipCache bool
}

// Lookup describes how a successful response was obtained for cache telemetry.
type Lookup struct {
	Response Response
	Status   string
}

// Request supplies a complete semantic key and the operation to run on a miss.
type Request struct {
	Key string
	// FlightKey separates concurrent loads whose positive cache entries are reusable
	// but whose uncached answers depend on different snapshots. Empty uses Key.
	FlightKey string
	Load      func(context.Context) (Response, error)
}

// Options supplies validated capacity settings and a deadline for shared upstream work.
type Options struct {
	Config      config.CacheConfig
	LoadTimeout time.Duration
}

// Service owns an LRU cache and the in-flight requests shared by its callers.
type Service struct {
	config      config.CacheConfig
	loadTimeout time.Duration
	mu          sync.Mutex
	entries     map[string]*list.Element
	recent      list.List
	bytes       int
	loads       singleflight.Group
}

type entry struct {
	key      string
	response Response
	expires  time.Time
	size     int
}

// NewService constructs a process-local response cache from validated options.
func NewService(options Options) *Service {
	return &Service{config: options.Config, loadTimeout: options.LoadTimeout, entries: make(map[string]*list.Element)}
}

// Enabled reports whether the validated configuration permits caching and shared loads.
func (s *Service) Enabled() bool {
	return !s.config.Disabled && s.config.MaxEntries > 0
}

// GetOrLoad returns a cached result or shares request.Load with concurrent callers
// using request.FlightKey, which defaults to request.Key when omitted.
// Each caller keeps its own deadline; shared work uses the configured load timeout.
func (s *Service) GetOrLoad(ctx context.Context, request Request) (Lookup, error) {
	if err := ctx.Err(); err != nil {
		return Lookup{Status: "bypass"}, fmt.Errorf("read response cache: %w", err)
	}
	if !s.Enabled() {
		response, err := request.Load(ctx)
		return Lookup{Response: response, Status: "bypass"}, err
	}
	if response, exists := s.get(request.Key); exists {
		return Lookup{Response: response, Status: "hit"}, nil
	}

	flightKey := request.FlightKey
	if flightKey == "" {
		flightKey = request.Key
	}
	loading := s.loads.DoChan(flightKey, func() (any, error) {
		// Another load can finish between the first lookup and joining the flight.
		if response, exists := s.get(request.Key); exists {
			return Lookup{Response: response, Status: "hit"}, nil
		}
		loadCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.loadTimeout)
		defer cancel()
		response, err := request.Load(loadCtx)
		if err != nil {
			return Lookup{Response: response, Status: "miss"}, fmt.Errorf("load cached response: %w", err)
		}
		if err := loadCtx.Err(); err != nil {
			return Lookup{Status: "miss"}, fmt.Errorf("complete cached response: %w", err)
		}
		s.put(request.Key, response)
		return Lookup{Response: response, Status: "miss"}, nil
	})
	select {
	case <-ctx.Done():
		return Lookup{Status: "miss"}, fmt.Errorf("wait for cached response: %w", ctx.Err())
	case completed := <-loading:
		lookup, ok := completed.Val.(Lookup)
		if !ok {
			return Lookup{Status: "miss"}, errors.New("response cache load returned an invalid response")
		}
		if completed.Shared {
			lookup.Status = "shared"
		}
		// Callers own their bytes; mutations cannot affect the cache or other waiters.
		lookup.Response.Payload = bytes.Clone(lookup.Response.Payload)
		return lookup, completed.Err
	}
}

func (s *Service) get(key string) (Response, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	element, exists := s.entries[key]
	if !exists {
		return Response{}, false
	}
	cached := element.Value.(entry)
	if !time.Now().Before(cached.expires) {
		s.remove(element)
		return Response{}, false
	}
	s.recent.MoveToFront(element)
	cached.response.Payload = bytes.Clone(cached.response.Payload)
	return cached.response, true
}

func (s *Service) put(key string, response Response) {
	// Null can mean an upstream has not indexed a block yet. Never retain absence.
	size := len(key) + len(response.Payload) + len(response.Upstream)
	if response.SkipCache || len(response.Payload) == 0 || bytes.Equal(bytes.TrimSpace(response.Payload), []byte("null")) || size > s.config.MaxEntryBytes || !json.Valid(response.Payload) {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if previous, exists := s.entries[key]; exists {
		s.remove(previous)
	}
	for s.recent.Len() >= s.config.MaxEntries || s.bytes > s.config.MaxBytes-size {
		s.remove(s.recent.Back())
	}
	response.Payload = bytes.Clone(response.Payload)
	cached := entry{key: key, response: response, expires: time.Now().Add(s.config.TTL.Value()), size: size}
	s.entries[key] = s.recent.PushFront(cached)
	s.bytes += size
}

func (s *Service) remove(element *list.Element) {
	cached := element.Value.(entry)
	delete(s.entries, cached.key)
	s.bytes -= cached.size
	s.recent.Remove(element)
}
