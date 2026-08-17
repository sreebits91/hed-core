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
	if poolSize <= 0 {
		poolSize = 128
	}
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

func (k *KeyDBEngine) GetState(channelID, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	val, err := k.client.Get(ctx, fmt.Sprintf("%s:%s", channelID, key)).Bytes()
	if err == redis.Nil {
		return nil, nil
	}
	return val, err
}

func (k *KeyDBEngine) PutState(channelID, key string, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return k.client.Set(ctx, fmt.Sprintf("%s:%s", channelID, key), value, 0).Err()
}

// BatchWrite performs ordinary state replacement in pipeline chunks.
func (k *KeyDBEngine) BatchWrite(channelID string, updates map[string][]byte) error {
	if len(updates) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pipe := k.client.Pipeline()
	count := 0
	for key, value := range updates {
		pipe.Set(ctx, channelID+":"+key, value, 0)
		count++
		if count%1000 == 0 {
			if err := k.execWithRetry(ctx, pipe); err != nil {
				return fmt.Errorf("state pipeline chunk failed: %w", err)
			}
			pipe = k.client.Pipeline()
		}
	}
	if count%1000 != 0 {
		if err := k.execWithRetry(ctx, pipe); err != nil {
			return fmt.Errorf("final state pipeline chunk failed: %w", err)
		}
	}
	return nil
}

// BatchWriteDeltas atomically accumulates relative balance changes with INCRBY.
func (k *KeyDBEngine) BatchWriteDeltas(channelID string, updates map[string]int64) error {
	if len(updates) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pipe := k.client.Pipeline()
	count := 0
	for key, value := range updates {
		if value == 0 {
			continue
		}
		pipe.IncrBy(ctx, channelID+":"+key, value)
		count++
		if count%1000 == 0 {
			if err := k.execWithRetry(ctx, pipe); err != nil {
				return fmt.Errorf("delta pipeline chunk failed: %w", err)
			}
			pipe = k.client.Pipeline()
		}
	}
	if count%1000 != 0 {
		if err := k.execWithRetry(ctx, pipe); err != nil {
			return fmt.Errorf("final delta pipeline chunk failed: %w", err)
		}
	}
	return nil
}

func (k *KeyDBEngine) execWithRetry(ctx context.Context, pipe redis.Pipeliner) error {
	const maxRetries = 3
	backoff := 10 * time.Millisecond
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if _, err := pipe.Exec(ctx); err == nil {
			return nil
		} else {
			log.Printf("[STORAGE RETRY] KeyDB pipeline failed (attempt %d/%d): %v", attempt, maxRetries, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
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
