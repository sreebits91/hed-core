package plugin

import (
    "context"
    "fmt"
    "time"

    "github.com/jackc/pgx/v5"
    "github.com/jackc/pgx/v5/pgxpool"
)

type YugabyteEngine struct { pool *pgxpool.Pool }

func NewYugabyteEngine(connString string) (*YugabyteEngine, error) {
    config, err := pgxpool.ParseConfig(connString)
    if err != nil { return nil, fmt.Errorf("failed to parse connection config: %w", err) }
    config.MaxConns = 128
    config.MinConns = 32
    config.MaxConnIdleTime = 5 * time.Minute
    pool, err := pgxpool.NewWithConfig(context.Background(), config)
    if err != nil { return nil, fmt.Errorf("failed to create pgx pool: %w", err) }
    return &YugabyteEngine{pool: pool}, nil
}

func (y *YugabyteEngine) Name() string { return "YugabyteDB-Distributed-SQL" }

func (y *YugabyteEngine) Init(config map[string]string) error {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second); defer cancel()
    _, err := y.pool.Exec(ctx, `
        CREATE TABLE IF NOT EXISTS channel_states (
            channel_id VARCHAR(64), account_id VARCHAR(64), balance BIGINT,
            PRIMARY KEY (channel_id, account_id)
        );
        CREATE TABLE IF NOT EXISTS hed_processed_batches (
            request_id VARCHAR(128) PRIMARY KEY,
            processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );`)
    return err
}

func (y *YugabyteEngine) BatchWrite(channelID string, updates map[string]int64) error {
    return y.batchWrite(context.Background(), "", channelID, updates)
}

// BatchWriteWithID uses one SQL transaction for the idempotency marker and all
// balance mutations. A retry either sees the committed marker and becomes a
// no-op, or re-executes the whole transaction; partial batches cannot be left
// behind by a client timeout.
func (y *YugabyteEngine) BatchWriteWithID(requestID, channelID string, updates map[string]int64) error {
    if requestID == "" { return fmt.Errorf("request ID is required for idempotent write") }
    return y.batchWrite(context.Background(), requestID, channelID, updates)
}

func (y *YugabyteEngine) batchWrite(ctx context.Context, requestID, channelID string, updates map[string]int64) error {
    if len(updates) == 0 { return nil }
    ctx, cancel := context.WithTimeout(ctx, 10*time.Second); defer cancel()

    tx, err := y.pool.Begin(ctx)
    if err != nil { return fmt.Errorf("begin state batch: %w", err) }
    defer tx.Rollback(ctx)

    if requestID != "" {
        var inserted bool
        err = tx.QueryRow(ctx, `
            INSERT INTO hed_processed_batches(request_id) VALUES($1)
            ON CONFLICT DO NOTHING RETURNING true`, requestID).Scan(&inserted)
        if err == pgx.ErrNoRows { return nil }
        if err != nil { return fmt.Errorf("reserve idempotency request: %w", err) }
    }

    const sqlStmt = `
        INSERT INTO channel_states (channel_id, account_id, balance)
        VALUES ($1, $2, $3)
        ON CONFLICT (channel_id, account_id)
        DO UPDATE SET balance = channel_states.balance + EXCLUDED.balance;`

    batch := &pgx.Batch{}
    count := 0
    for key, val := range updates {
        if val == 0 { continue }
        batch.Queue(sqlStmt, channelID, key, val)
        count++
    }
    if count > 0 {
        if err := tx.SendBatch(ctx, batch).Close(); err != nil { return fmt.Errorf("apply state batch: %w", err) }
    }
    if err := tx.Commit(ctx); err != nil { return fmt.Errorf("commit state batch: %w", err) }
    return nil
}

func (y *YugabyteEngine) GetState(channelID, key string) ([]byte, error) {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second); defer cancel()
    var balance int64
    err := y.pool.QueryRow(ctx, `SELECT balance FROM channel_states WHERE channel_id=$1 AND account_id=$2`, channelID, key).Scan(&balance)
    if err == pgx.ErrNoRows { return nil, nil }
    if err != nil { return nil, err }
    return []byte(fmt.Sprintf("%d", balance)), nil
}

func (y *YugabyteEngine) PutState(channelID, key string, value []byte) error {
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second); defer cancel()
    _, err := y.pool.Exec(ctx, `
        INSERT INTO channel_states(channel_id, account_id, balance) VALUES($1,$2,$3)
        ON CONFLICT(channel_id,account_id) DO UPDATE SET balance=EXCLUDED.balance`, channelID, key, stringToInt64(value))
    return err
}

func stringToInt64(value []byte) int64 { var n int64; fmt.Sscanf(string(value), "%d", &n); return n }
func (y *YugabyteEngine) Close() error { if y.pool != nil { y.pool.Close() }; return nil }
