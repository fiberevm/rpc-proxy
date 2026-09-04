package head

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

type RedisStore struct {
	client        redis.UniversalClient
	prefix        string
	streamMax     int64
	acquireScript *redis.Script
	renewScript   *redis.Script
	releaseScript *redis.Script
	publishScript *redis.Script
}

// RedisStoreOptions contains the validated connection and retention settings for shared head storage.
type RedisStoreOptions struct {
	URL             string
	KeyPrefix       string
	StreamMaxLength int64
}

const acquireScriptSource = `
if redis.call('EXISTS', KEYS[1]) == 1 then return {0, ''} end
local fence = redis.call('INCR', KEYS[2])
local token = ARGV[1] .. ':' .. fence
redis.call('PSETEX', KEYS[1], ARGV[2], token)
return {1, token}
`

const renewScriptSource = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return 0 end
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
`

const releaseScriptSource = `
if redis.call('GET', KEYS[1]) == ARGV[1] then return redis.call('DEL', KEYS[1]) end
return 0
`

const publishScriptSource = `
if redis.call('GET', KEYS[1]) ~= ARGV[1] then return {-1, ''} end
local incoming = cjson.decode(ARGV[2])
local currentRaw = redis.call('HGET', KEYS[2], incoming['commitment'])
if currentRaw then
  local current = cjson.decode(currentRaw)
  local same = false
  if incoming['hash'] and incoming['hash'] ~= '' then same = current['hash'] == incoming['hash']
  else same = current['number'] == incoming['number'] end
  if same then
    incoming['generation'] = current['generation']
    if current['changed_at'] then incoming['changed_at'] = current['changed_at'] end
    local refreshed = cjson.encode(incoming)
    redis.call('HSET', KEYS[2], incoming['commitment'], refreshed)
    return {current['generation'], refreshed}
  end
end
local generation = redis.call('INCR', KEYS[3])
incoming['generation'] = generation
local encoded = cjson.encode(incoming)
redis.call('HSET', KEYS[2], incoming['commitment'], encoded)
if incoming['family'] == 'evm' and incoming['commitment'] == 'latest' then
  redis.call('XADD', KEYS[4], 'MAXLEN', ARGV[3], '*', 'head', encoded)
end
return {generation, encoded}
`

// NewRedisStore creates the Redis-backed fenced head store used across proxy replicas.
func NewRedisStore(options RedisStoreOptions) (*RedisStore, error) {
	redisOptions, err := redis.ParseURL(options.URL)
	if err != nil {
		return nil, fmt.Errorf("parse redis url: %w", err)
	}
	return &RedisStore{
		client:        redis.NewClient(redisOptions),
		prefix:        options.KeyPrefix,
		streamMax:     options.StreamMaxLength,
		acquireScript: redis.NewScript(acquireScriptSource),
		renewScript:   redis.NewScript(renewScriptSource),
		releaseScript: redis.NewScript(releaseScriptSource),
		publishScript: redis.NewScript(publishScriptSource),
	}, nil
}

// Close flushes and closes the injected Redis client.
func (r *RedisStore) Close() error { return r.client.Close() }

// Ping checks whether Redis is reachable for startup and readiness decisions.
func (r *RedisStore) Ping(ctx context.Context) error {
	if err := r.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("ping redis: %w", err)
	}
	return nil
}

func (r *RedisStore) key(chain, suffix string) string {
	return fmt.Sprintf("%s:{%s}:%s", r.prefix, chain, suffix)
}

// Acquire creates a fenced leadership token for chain when no unexpired leader exists.
func (r *RedisStore) Acquire(ctx context.Context, chain, owner string, ttl time.Duration) (string, bool, error) {
	response, err := r.acquireScript.Run(ctx, r.client, []string{r.key(chain, "leader"), r.key(chain, "fence")}, owner, ttl.Milliseconds()).Slice()
	if err != nil {
		return "", false, fmt.Errorf("acquire redis leadership for %s: %w", chain, err)
	}
	if len(response) != 2 {
		return "", false, fmt.Errorf("acquire redis leadership for %s: invalid script response", chain)
	}
	acquired, ok := response[0].(int64)
	if !ok {
		return "", false, fmt.Errorf("acquire redis leadership for %s: invalid acquisition flag", chain)
	}
	token, ok := response[1].(string)
	if !ok {
		return "", false, fmt.Errorf("acquire redis leadership for %s: invalid fencing token", chain)
	}
	return token, acquired == 1, nil
}

// Renew extends chain leadership only when token still owns the fenced lease.
func (r *RedisStore) Renew(ctx context.Context, chain, token string, ttl time.Duration) (bool, error) {
	renewed, err := r.renewScript.Run(ctx, r.client, []string{r.key(chain, "leader")}, token, ttl.Milliseconds()).Int64()
	if err != nil {
		return false, fmt.Errorf("renew redis leadership for %s: %w", chain, err)
	}
	return renewed == 1, nil
}

// Release removes chain leadership only when token still owns the fenced lease.
func (r *RedisStore) Release(ctx context.Context, chain, token string) error {
	if err := r.releaseScript.Run(ctx, r.client, []string{r.key(chain, "leader")}, token).Err(); err != nil {
		return fmt.Errorf("release redis leadership for %s: %w", chain, err)
	}
	return nil
}

// Publish stores one accepted head only while token remains the current fencing token.
func (r *RedisStore) Publish(ctx context.Context, token string, acceptedHead Head) (Head, error) {
	if acceptedHead.ChangedAt.IsZero() {
		acceptedHead.ChangedAt = acceptedHead.ObservedAt
	}
	encodedHead, err := json.Marshal(acceptedHead)
	if err != nil {
		return Head{}, fmt.Errorf("encode accepted head: %w", err)
	}
	keys := []string{r.key(acceptedHead.Chain, "leader"), r.key(acceptedHead.Chain, "heads"), r.key(acceptedHead.Chain, "generation"), r.key(acceptedHead.Chain, "events")}
	response, err := r.publishScript.Run(ctx, r.client, keys, token, encodedHead, r.streamMax).Slice()
	if err != nil {
		return Head{}, fmt.Errorf("publish accepted head for %s: %w", acceptedHead.Chain, err)
	}
	if len(response) != 2 {
		return Head{}, fmt.Errorf("publish accepted head for %s: invalid script response", acceptedHead.Chain)
	}
	generation, ok := response[0].(int64)
	if !ok {
		return Head{}, fmt.Errorf("publish accepted head for %s: invalid generation", acceptedHead.Chain)
	}
	if generation < 0 {
		return Head{}, ErrLeadershipLost
	}
	refreshed, ok := response[1].(string)
	if !ok {
		return Head{}, fmt.Errorf("publish accepted head for %s: invalid encoded head", acceptedHead.Chain)
	}
	if err := json.Unmarshal([]byte(refreshed), &acceptedHead); err != nil {
		return Head{}, fmt.Errorf("decode published head for %s: %w", acceptedHead.Chain, err)
	}
	return acceptedHead, nil
}

// Snapshot reads all accepted commitment heads for chain in one Redis operation.
func (r *RedisStore) Snapshot(ctx context.Context, chain string) (Snapshot, error) {
	values, err := r.client.HGetAll(ctx, r.key(chain, "heads")).Result()
	if err != nil {
		return Snapshot{}, fmt.Errorf("read redis head snapshot for %s: %w", chain, err)
	}
	snapshot := Snapshot{Chain: chain, Heads: map[string]Head{}}
	for commitment, raw := range values {
		var acceptedHead Head
		if err := json.Unmarshal([]byte(raw), &acceptedHead); err != nil {
			return Snapshot{}, fmt.Errorf("decode %s head: %w", commitment, err)
		}
		snapshot.Heads[commitment] = acceptedHead
	}
	return snapshot, nil
}

// StreamCursor returns the latest retained event ID for a newly connected WebSocket client.
func (r *RedisStore) StreamCursor(ctx context.Context, chain string) (string, error) {
	checkpoint, err := r.StreamStart(ctx, chain)
	return checkpoint.ID, err
}

// StreamStart atomically reads the cursor and header preceding a new subscription.
func (r *RedisStore) StreamStart(ctx context.Context, chain string) (Event, error) {
	messages, err := r.client.XRevRangeN(ctx, r.key(chain, "events"), "+", "-", 1).Result()
	if errors.Is(err, redis.Nil) {
		return Event{ID: "0-0"}, nil
	}
	if err != nil {
		return Event{}, fmt.Errorf("read redis stream cursor for %s: %w", chain, err)
	}
	if len(messages) == 0 {
		return Event{ID: "0-0"}, nil
	}
	return r.getEvent(messages[0])
}

// ReadEvents waits up to block and returns at most count accepted head events after the supplied cursor.
func (r *RedisStore) ReadEvents(ctx context.Context, chain, afterCursor string, blockTimeout time.Duration, count int64) ([]Event, error) {
	streams, err := r.client.XRead(ctx, &redis.XReadArgs{Streams: []string{r.key(chain, "events"), afterCursor}, Count: count, Block: blockTimeout}).Result()
	if err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read redis head events for %s: %w", chain, err)
	}
	// Check after XREAD too: trimming while it blocked must not silently skip heads.
	if afterCursor != "0-0" {
		checkpoint, checkErr := r.client.XRangeN(ctx, r.key(chain, "events"), afterCursor, afterCursor, 1).Result()
		if checkErr != nil {
			return nil, fmt.Errorf("check stream retention: %w", checkErr)
		}
		if len(checkpoint) == 0 {
			return nil, ErrStreamGap
		}
	}
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read redis head events for %s: %w", chain, err)
	}
	var events []Event
	for _, stream := range streams {
		for _, message := range stream.Messages {
			event, err := r.getEvent(message)
			if err != nil {
				return nil, err
			}
			events = append(events, event)
		}
	}
	return events, nil
}

func (r *RedisStore) getEvent(message redis.XMessage) (Event, error) {
	encodedHead, ok := message.Values["head"].(string)
	if !ok {
		return Event{}, errors.New("head stream event has no encoded header")
	}
	var acceptedHead Head
	if err := json.Unmarshal([]byte(encodedHead), &acceptedHead); err != nil {
		return Event{}, fmt.Errorf("decode redis head event %s: %w", message.ID, err)
	}
	return Event{ID: message.ID, Head: acceptedHead}, nil
}
