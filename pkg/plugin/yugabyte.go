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

    // High-throughput connection pool settings
    config.MaxConns = 128
    config.MinConns = 32
    config.MaxConnIdleTime = 5 * time.Minute

    // FIX 1: Changed context.Background()d() -> context.Background()
    pool, err := pgxpool.NewWithConfig(context.Background(), config)
    if err != nil {
        return nil, fmt.Errorf("failed to create pgx pool: %w", err)
    }

    return &YugabyteEngine{pool: pool}, nil
}

func (y *YugabyteEngine) Name() string {
    return "YugabyteDB-Distributed-SQL"
}

func (y *YugabyteEngine) Init(config map[string]string) error {
    // FIX 2: Changed context.Background()d() -> context.Background()
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    defer cancel()

    // Ensure table schema exists
    query := `
    CREATE TABLE IF NOT EXISTS channel_states (
        channel_id VARCHAR(64),
        account_id VARCHAR(64),
        balance BIGINT,
        PRIMARY KEY (channel_id, account_id)
    );`

    _, err := y.pool.Exec(ctx, query)
    return err
}

// BatchWrite converts RAM deltas into high-throughput Yugabyte UPSERT batch statements
func (y *YugabyteEngine) BatchWrite(channelID string, updates map[string]int64) error {
    if len(updates) == 0 {
        return nil
    }

    // FIX 3: Changed context.Background()d() -> context.Background()
    ctx := context.Background()
    batch := &pgx.Batch{}

    // SQL Atomic Delta Accumulation: ON CONFLICT DO UPDATE balance = balance + EXCLUDED.balance
    sqlStmt := `
        INSERT INTO channel_states (channel_id, account_id, balance)
        VALUES ($1, $2, $3)
        ON CONFLICT (channel_id, account_id)
        DO UPDATE SET balance = channel_states.balance + EXCLUDED.balance;
    `

    count := 0
    for key, val := range updates {
        if val == 0 {
            continue
        }
        batch.Queue(sqlStmt, channelID, key, val)
        count++

        // Send in 500-statement batch micro-chunks optimized for Yugabyte Raft consensus
        if count%500 == 0 {
            if err := y.sendBatchWithRetry(ctx, batch); err != nil {
                return err
            }
            batch = &pgx.Batch{}
        }
    }

    if count > 0 && count%500 != 0 {
        if err := y.sendBatchWithRetry(ctx, batch); err != nil {
            return err
        }
    }

    return nil
}

// FIX 4: Changed param type from context.Background()d -> context.Context
func (y *YugabyteEngine) sendBatchWithRetry(ctx context.Context, batch *pgx.Batch) error {
    maxRetries := 3
    backoff := 15 * time.Millisecond

    for i := 0; i < maxRetries; i++ {
        br := y.pool.SendBatch(ctx, batch)
        err := br.Close()
        if err == nil {
            return nil
        }

        log.Printf("⚠️ [YUGABYTE RETRY] Batch write failed (attempt %d/%d): %v", i+1, maxRetries, err)
        time.Sleep(backoff)
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