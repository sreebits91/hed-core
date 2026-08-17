package plugin

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type YugabyteEngine struct {
	pool *pgxpool.Pool
}

func NewYugabyteEngine(connString string) (*YugabyteEngine, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("failed to parse connection config: %w", err)
	}
	config.MaxConns = 128
	config.MinConns = 32
	config.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return nil, fmt.Errorf("failed to create pgx pool: %w", err)
	}
	return &YugabyteEngine{pool: pool}, nil
}

func (y *YugabyteEngine) Name() string { return "YugabyteDB-Distributed-SQL" }

func (y *YugabyteEngine) Init(config map[string]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := y.pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS channel_states (
			channel_id VARCHAR(64),
			account_id VARCHAR(64),
			balance BIGINT,
			PRIMARY KEY (channel_id, account_id)
		);`)
	return err
}

// BatchWrite performs ordinary state replacement.
func (y *YugabyteEngine) BatchWrite(channelID string, updates map[string][]byte) error {
	if len(updates) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	batch := &pgx.Batch{}
	count := 0
	for key, value := range updates {
		batch.Queue(`
			INSERT INTO channel_states (channel_id, account_id, balance)
			VALUES ($1, $2, $3)
			ON CONFLICT (channel_id, account_id)
			DO UPDATE SET balance = EXCLUDED.balance;`, channelID, key, value)
		count++
		if count%500 == 0 {
			if err := y.sendBatchWithRetry(ctx, batch); err != nil {
				return err
			}
			batch = &pgx.Batch{}
		}
	}
	if count%500 != 0 {
		return y.sendBatchWithRetry(ctx, batch)
	}
	return nil
}

// BatchWriteDeltas atomically accumulates relative balance changes.
func (y *YugabyteEngine) BatchWriteDeltas(channelID string, updates map[string]int64) error {
	if len(updates) == 0 {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	batch := &pgx.Batch{}
	count := 0
	for key, value := range updates {
		if value == 0 {
			continue
		}
		batch.Queue(`
			INSERT INTO channel_states (channel_id, account_id, balance)
			VALUES ($1, $2, $3)
			ON CONFLICT (channel_id, account_id)
			DO UPDATE SET balance = channel_states.balance + EXCLUDED.balance;`, channelID, key, value)
		count++
		if count%500 == 0 {
			if err := y.sendBatchWithRetry(ctx, batch); err != nil {
				return err
			}
			batch = &pgx.Batch{}
		}
	}
	if count%500 != 0 {
		return y.sendBatchWithRetry(ctx, batch)
	}
	return nil
}

func (y *YugabyteEngine) sendBatchWithRetry(ctx context.Context, batch *pgx.Batch) error {
	const maxRetries = 3
	backoff := 15 * time.Millisecond
	for attempt := 1; attempt <= maxRetries; attempt++ {
		br := y.pool.SendBatch(ctx, batch)
		err := br.Close()
		if err == nil {
			return nil
		}
		log.Printf("[YUGABYTE RETRY] Batch write failed (attempt %d/%d): %v", attempt, maxRetries, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff *= 2
	}
	return fmt.Errorf("exceeded max Yugabyte batch retry attempts")
}

func (y *YugabyteEngine) Close() error {
	if y.pool != nil {
		y.pool.Close()
	}
	return nil
}
