package plugin

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type KeyDBEngine struct {
	client *redis.Client
	addr   string
}

func NewKeyDBEngine(addr string, poolSize int) *KeyDBEngine {
	client := redis.NewClient(&redis.Options{
		Addr:         addr,
		PoolSize:     poolSize,
		MinIdleConns: 64,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
	return &KeyDBEngine{client: client, addr: addr}
}

func (k *KeyDBEngine) Name() string { return "KeyDB-Production-Incr" }

func (k *KeyDBEngine) Init(config map[string]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return k.client.Ping(ctx).Err()
}

func (k *KeyDBEngine) GetState(channelID string, key string) ([]byte, error) {
	ctx := context.Background()
	fullKey := fmt.Sprintf("%s:%s", channelID, key)
	val, err := k.client.Get(ctx, fullKey).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	return val, err
}

func (k *KeyDBEngine) PutState(channelID string, key string, value []byte) error {
	ctx := context.Background()
	fullKey := fmt.Sprintf("%s:%s", channelID, key)
	return k.client.Set(ctx, fullKey, value, 0).Err()
}

// BatchWrite applies a batch of relative updates. It intentionally does not
// retry an INCRBY pipeline: after a network error the server outcome may be
// unknown, and replaying INCRBY can double-apply a successfully executed batch.
// The caller must retain/retry the logical batch at a higher level only when it
// has an idempotency mechanism, or use a transactional store with request IDs.
func (k *KeyDBEngine) BatchWrite(channelID string, updates map[string]int64) error {
	if len(updates) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pipe := k.client.Pipeline()
	count := 0
	for key, val := range updates {
		if val == 0 {
			continue
		}
		pipe.IncrBy(ctx, fmt.Sprintf("%s:%s", channelID, key), val)
		count++

		if count%1000 == 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				return fmt.Errorf("KeyDB pipeline chunk failed (outcome may be unknown): %w", err)
			}
			pipe = k.client.Pipeline()
		}
	}

	if count > 0 && count%1000 != 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			return fmt.Errorf("KeyDB final pipeline failed (outcome may be unknown): %w", err)
		}
	}
	return nil
}

func (k *KeyDBEngine) Close() error {
	if k.client != nil {
		return k.client.Close()
	}
	return nil
}
