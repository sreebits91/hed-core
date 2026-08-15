package plugin

import (
    "context"
    "fmt"
    "time"

    "github.com/go-redis/redis/v8"
)

type KeyDBEngine struct { client *redis.Client; addr string }

func NewKeyDBEngine(addr string, poolSize int) *KeyDBEngine {
    return &KeyDBEngine{client: redis.NewClient(&redis.Options{Addr: addr, PoolSize: poolSize, MinIdleConns: 64, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second}), addr: addr}
}
func (k *KeyDBEngine) Name() string { return "KeyDB-Production-Incr" }
func (k *KeyDBEngine) Init(config map[string]string) error { ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second); defer cancel(); return k.client.Ping(ctx).Err() }
func (k *KeyDBEngine) GetState(channelID, key string) ([]byte, error) { v, err := k.client.Get(context.Background(), fmt.Sprintf("%s:%s", channelID, key)).Bytes(); if err == redis.Nil { return nil, nil }; return v, err }
func (k *KeyDBEngine) PutState(channelID, key string, value []byte) error { return k.client.Set(context.Background(), fmt.Sprintf("%s:%s", channelID, key), value, 0).Err() }

func (k *KeyDBEngine) BatchWrite(channelID string, updates map[string]int64) error { return k.batchWrite(context.Background(), "", channelID, updates) }

// BatchWriteWithID makes every individual key mutation idempotent. Each Lua
// invocation atomically checks the request/key marker and performs INCRBY, so a
// timeout can be retried without double-applying already completed keys.
func (k *KeyDBEngine) BatchWriteWithID(requestID, channelID string, updates map[string]int64) error {
    if requestID == "" { return fmt.Errorf("request ID is required for idempotent write") }
    return k.batchWrite(context.Background(), requestID, channelID, updates)
}

const idempotentIncrementScript = `
local seen = redis.call('HGET', KEYS[1], ARGV[1])
if seen then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], '1')
redis.call('EXPIRE', KEYS[1], ARGV[2])
return redis.call('INCRBY', KEYS[2], ARGV[3])
`

func (k *KeyDBEngine) batchWrite(ctx context.Context, requestID, channelID string, updates map[string]int64) error {
    if len(updates) == 0 { return nil }
    ctx, cancel := context.WithTimeout(ctx, 10*time.Second); defer cancel()

    if requestID != "" {
        markerKey := fmt.Sprintf("hed:idempotency:%s", requestID)
        for key, val := range updates {
            if val == 0 { continue }
            stateKey := fmt.Sprintf("%s:%s", channelID, key)
            if _, err := k.client.Eval(ctx, idempotentIncrementScript, []string{markerKey, stateKey}, key, int64(24*time.Hour/time.Second), val).Result(); err != nil {
                return fmt.Errorf("idempotent KeyDB write failed for %s: %w", key, err)
            }
        }
        return nil
    }

    pipe := k.client.Pipeline()
    count := 0
    for key, val := range updates {
        if val == 0 { continue }
        pipe.IncrBy(ctx, fmt.Sprintf("%s:%s", channelID, key), val)
        count++
        if count%1000 == 0 {
            if _, err := pipe.Exec(ctx); err != nil { return fmt.Errorf("KeyDB pipeline failed (outcome may be unknown): %w", err) }
            pipe = k.client.Pipeline()
        }
    }
    if count > 0 && count%1000 != 0 { if _, err := pipe.Exec(ctx); err != nil { return fmt.Errorf("KeyDB final pipeline failed (outcome may be unknown): %w", err) } }
    return nil
}

func (k *KeyDBEngine) Close() error { if k.client != nil { return k.client.Close() }; return nil }
