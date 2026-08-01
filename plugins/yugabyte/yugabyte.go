package plugin

import (
	"context"
	"fmt"
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
		return nil, fmt.Errorf("invalid YugabyteDB config: %w", err)
	}

	// Tune pool parameters for high-concurrency throughput
	config.MaxConns = 128
	config.MinConns = 32
	config.MaxConnIdleTime = 5 * time.Minute
	config.MaxConnLifetime = 30 * time.Minute

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("unable to connect to YugabyteDB: %w", err)
	}

	// Create table schema if not exists
	query := `
	CREATE TABLE IF NOT EXISTS state_store (
		shard_id VARCHAR(64),
		state_key VARCHAR(256),
		state_val BYTEA,
		updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (shard_id, state_key)
	);`

	_, err = pool.Exec(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize YugabyteDB schema: %w", err)
	}

	return &YugabyteEngine{pool: pool}, nil
}

func (y *YugabyteEngine) Name() string {
	return "YugabyteDB (Distributed SQL)"
}

func (y *YugabyteEngine) PutState(shard, key string, value []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	query := `
		INSERT INTO state_store (shard_id, state_key, state_val, updated_at) 
		VALUES ($1, $2, $3, NOW()) 
		ON CONFLICT (shard_id, state_key) 
		DO UPDATE SET state_val = EXCLUDED.state_val, updated_at = NOW();`

	_, err := y.pool.Exec(ctx, query, shard, key, value)
	return err
}

func (y *YugabyteEngine) GetState(shard, key string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var val []byte
	query := `SELECT state_val FROM state_store WHERE shard_id = $1 AND state_key = $2;`
	err := y.pool.QueryRow(ctx, query, shard, key).Scan(&val)
	if err != nil {
		return nil, err
	}
	return val, nil
}

// BatchWrite uses pgx.Batch to pipeline all inserts into 1 single network round-trip!
func (y *YugabyteEngine) BatchWrite(shard string, kvs map[string][]byte) error {
	if len(kvs) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	batch := &pgx.Batch{}
	query := `
		INSERT INTO state_store (shard_id, state_key, state_val, updated_at) 
		VALUES ($1, $2, $3, NOW()) 
		ON CONFLICT (shard_id, state_key) 
		DO UPDATE SET state_val = EXCLUDED.state_val, updated_at = NOW();`

	for k, v := range kvs {
		batch.Queue(query, shard, k, v)
	}

	br := y.pool.SendBatch(ctx, batch)
	defer br.Close()

	// Flush and evaluate batch responses
	for i := 0; i < len(kvs); i++ {
		_, err := br.Exec()
		if err != nil {
			return fmt.Errorf("batch write failed at item %d: %w", i, err)
		}
	}

	return nil
}