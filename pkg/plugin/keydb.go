package plugin

import (
	"context"
	"fmt"
	"log"
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
	return &KeyDBEngine{
		client: client,
		addr:   addr,
	}
}

func (k *KeyDBEngine) Name() string {
	return "KeyDB-Production-Incr"
}

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

// BatchWrite executes INCRBY operations in 1,000-command pipeline chunks with exponential backoff retries
func (k *KeyDBEngine) BatchWrite(channelID string, updates map[string]int64) error {
	if len(updates) == 0 {
		return nil
	}

	ctx := context.Background()
	pipe := k.client.Pipeline()
	count := 0

	for key, val := range updates {
		if val == 0 {
			continue
		}

		fullKey := channelID + ":" + key
		// INCRBY ensures relative updates accumulate safely without overwriting state
		pipe.IncrBy(ctx, fullKey, val)
		count++

		if count%1000 == 0 {
			if err := k.execWithRetry(ctx, pipe); err != nil {
				return fmt.Errorf("pipeline chunk failed after retries: %w", err)
			}
			pipe = k.client.Pipeline()
		}
	}

	if count > 0 && count%1000 != 0 {
		if err := k.execWithRetry(ctx, pipe); err != nil {
			return fmt.Errorf("final pipeline chunk failed after retries: %w", err)
		}
	}

	return nil
}

// execWithRetry handles network failures with 3 retry attempts using exponential backoff
func (k *KeyDBEngine) execWithRetry(ctx context.Context, pipe redis.Pipeliner) error {
	maxRetries := 3
	backoff := 10 * time.Millisecond

	for i := 0; i < maxRetries; i++ {
		_, err := pipe.Exec(ctx)
		if err == nil {
			return nil
		}

		log.Printf("⚠️ [STORAGE RETRY] KeyDB pipeline write failed (attempt %d/%d): %v", i+1, maxRetries, err)
		time.Sleep(backoff)
		backoff *= 2
	}

	return fmt.Errorf("exceeded max pipeline retry attempts")
}

func (k *KeyDBEngine) Close() error {
	if k.client != nil {
		return k.client.Close()
	}
	return nil
}