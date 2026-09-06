package requestcache

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/fiberevm/rpc-proxy/internal/config"
)

func TestCacheCapacityAndOwnership(t *testing.T) {
	for _, limit := range []string{"entries", "bytes"} {
		t.Run(limit, func(t *testing.T) {
			cacheConfig := config.CacheConfig{MaxEntries: 2, MaxBytes: 100, MaxEntryBytes: 10, TTL: config.Duration(time.Minute)}
			if limit == "bytes" {
				cacheConfig.MaxEntries = 100
				cacheConfig.MaxBytes = 10 // Each key, payload and upstream uses five bytes.
			}
			cache := NewService(Options{Config: cacheConfig, LoadTimeout: time.Second})
			calls := 0
			payload := json.RawMessage(`"x"`)
			load := func(context.Context) (Response, error) {
				calls++
				return Response{Payload: payload, Upstream: "p"}, nil
			}
			for _, key := range []string{"a", "b", "a", "c", "a", "b"} {
				lookup, err := cache.GetOrLoad(context.Background(), Request{Key: key, Load: load})
				if err != nil || string(lookup.Response.Payload) != `"x"` {
					t.Fatalf("load %s: %+v, %v", key, lookup, err)
				}
				lookup.Response.Payload[1] = 'z'
			}
			if calls != 4 || string(payload) != `"x"` {
				t.Fatalf("LRU or byte ownership failed: calls=%d payload=%s", calls, payload)
			}
		})
	}
}

func TestCacheDoesNotRetainFailuresAbsenceOrOversizedResponses(t *testing.T) {
	loadErr := errors.New("upstream failure")
	for _, test := range []struct {
		name     string
		payload  string
		err      error
		disabled bool
	}{
		{name: "error", err: loadErr},
		{name: "null", payload: " null "},
		{name: "missing"},
		{name: "invalid JSON", payload: "{"},
		{name: "oversized", payload: `"` + strings.Repeat("x", 100) + `"`},
		{name: "disabled", payload: `"x"`, disabled: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			cache := NewService(Options{Config: config.CacheConfig{Disabled: test.disabled, MaxEntries: 2, MaxBytes: 100, MaxEntryBytes: 20, TTL: config.Duration(time.Minute)}, LoadTimeout: time.Second})
			calls := 0
			request := Request{Key: "k", Load: func(context.Context) (Response, error) {
				calls++
				return Response{Payload: json.RawMessage(test.payload), Upstream: "p"}, test.err
			}}
			for range 2 {
				lookup, err := cache.GetOrLoad(context.Background(), request)
				if !errors.Is(err, test.err) || lookup.Response.Upstream != "p" {
					t.Fatalf("lost response or error: %+v %v", lookup, err)
				}
			}
			if calls != 2 {
				t.Fatalf("unexpected retained response: calls=%d", calls)
			}
		})
	}
}

func TestCacheExpiryDoesNotExtendOnHits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cache := NewService(Options{Config: config.CacheConfig{MaxEntries: 2, MaxBytes: 100, MaxEntryBytes: 20, TTL: config.Duration(time.Minute)}, LoadTimeout: time.Second})
		calls := 0
		request := Request{Key: "k", Load: func(context.Context) (Response, error) {
			calls++
			return Response{Payload: json.RawMessage(`[]`)}, nil
		}}
		for _, delay := range []time.Duration{0, 30 * time.Second, 30 * time.Second} {
			time.Sleep(delay)
			if _, err := cache.GetOrLoad(context.Background(), request); err != nil {
				t.Fatal(err)
			}
		}
		if calls != 2 {
			t.Fatalf("expected empty-array reuse followed by expiry, calls=%d", calls)
		}
	})
}

func TestCacheSharesLoadsWithIndependentCallerCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cache := NewService(Options{Config: config.CacheConfig{MaxEntries: 2, MaxBytes: 100, MaxEntryBytes: 20, TTL: config.Duration(time.Minute)}, LoadTimeout: time.Second})
		var calls atomic.Int32
		release := make(chan struct{})
		request := Request{Key: "k", Load: func(ctx context.Context) (Response, error) {
			calls.Add(1)
			select {
			case <-release:
				return Response{Payload: json.RawMessage(`"ok"`)}, nil
			case <-ctx.Done():
				return Response{}, ctx.Err()
			}
		}}
		firstCtx, cancel := context.WithCancel(context.Background())
		firstDone := make(chan error, 1)
		go func() { _, err := cache.GetOrLoad(firstCtx, request); firstDone <- err }()
		synctest.Wait()
		secondDone := make(chan Lookup, 1)
		go func() {
			lookup, err := cache.GetOrLoad(context.Background(), request)
			if err != nil {
				t.Errorf("second caller: %v", err)
			}
			secondDone <- lookup
		}()
		synctest.Wait()
		cancel()
		if err := <-firstDone; !errors.Is(err, context.Canceled) {
			t.Fatalf("caller cancellation lost: %v", err)
		}
		close(release)
		lookup := <-secondDone
		if string(lookup.Response.Payload) != `"ok"` || lookup.Status != "shared" || calls.Load() != 1 {
			t.Fatalf("shared load failed: %+v calls=%d", lookup, calls.Load())
		}
	})
}

func TestCacheBoundsSharedWorkDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		cache := NewService(Options{Config: config.CacheConfig{MaxEntries: 2, MaxBytes: 100, MaxEntryBytes: 20, TTL: config.Duration(time.Minute)}, LoadTimeout: time.Second})
		_, err := cache.GetOrLoad(context.Background(), Request{Key: "k", Load: func(ctx context.Context) (Response, error) {
			<-ctx.Done()
			return Response{}, ctx.Err()
		}})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shared work was not bounded: %v", err)
		}
	})
}
