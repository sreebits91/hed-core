package plugin

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type KeyDBEngine struct {
	client *redis.Client
	addr string
}

func NewKeyDBEngine(addr string, poolSize int) *KeyDBEngine {
	return &KeyDBEngine{client: redis.NewClient(&redis.Options{Addr: addr, PoolSize: poolSize, MinIdleConns: 64, ReadTimeout: 3 * time.Second, WriteTimeout: 3 * time.Second}), addr: addr}
}

func (k *KeyDBEngine) Name() string { return "KeyDB-Production-Incr" }

func (k *KeyDBEngine) Init(config map[string]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second); defer cancel()
	return k.client.Ping(ctx).Err()
}

func (k *KeyDBEngine) GetState(channelID, key string) ([]byte, error) {
	val, err := k.client.Get(context.Background(), fmt.Sprintf("%s:%s", channelID, key)).Bytes()
	if err == redis.Nil { return nil, nil }
	return val, err
}

func (k *KeyDBEngine) PutState(channelID, key string, value []byte) error {
	return k.client.Set(context.Background(), fmt.Sprintf("%s:%s", channelID, key), value, 0).Err()
}

func (k *KeyDBEngine) BatchWrite(channelID string, updates map[string]int64) error {
	return k.batchWrite(context.Background(), "", channelID, updates)
}

// BatchWriteWithID is replay-safe for a logical batch. The request marker is
// created atomically with SETNX. If the marker already exists, the request has
// already been accepted and is treated as successful. This prevents a client
// from replaying an acknowledged logical request after an ambiguous timeout.
func (k *KeyDBEngine) BatchWriteWithID(requestID, channelID string, updates map[string]int64) error {
	if requestID == "" { return fmt.Errorf("request ID is required for idempotent write") }
	return k.batchWrite(context.Background(), requestID, channelID, updates)
}

func (k *KeyDBEngine) batchWrite(ctx context.Context, requestID, channelID string, updates map[string]int64) error {
	if len(updates) == 0 { return nil }
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second); defer cancel()

	if requestID != "" {
		marker := fmt.Sprintf("hed:idempotency:%s", requestID)
		created, err := k.client.SetNX(ctx, marker, "1", 24*time.Hour).Result()
		if err != nil { return fmt.Errorf("create idempotency marker: %w", err) }
		if !created { return nil }
	}

	pipe := k.client.Pipeline()
	count := 0
	for key, val := range updates {
		if val == 0 { continue }
		pipe.IncrBy(ctx, fmt.Sprintf("%s:%s", channelID, key), val)
		count++
		if count%1000 == 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				return fmt.Errorf("KeyDB pipeline failed after idempotency reservation; outcome must be reconciled: %w", err)
			}
			pipe = k.client.Pipeline()
		}
	}
	if count > 0 && count%1000 != 0 {
		if _, err := pipe.Exec(ctx); err != nil { return fmt.Errorf("KeyDB final pipeline failed after idempotency reservation; outcome must be reconciled: %w", err) }
	}
	return nil
}

func (k *KeyDBEngine) Close() error { if k.client != nil { return k.client.Close() }; return nil }
