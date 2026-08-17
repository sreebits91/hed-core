package benchmark

import (
	"context"
	"os"
	"testing"
	"time"

	"hed-core/pkg/plugin"
)

func TestKeyDBPersistenceBenchmark(t *testing.T) {
	addr := os.Getenv("HED_KEYDB_ADDR")
	if addr == "" { t.Skip("HED_KEYDB_ADDR not configured") }
	db := plugin.NewKeyDBEngine(addr, 128)
	defer db.Close()
	if err := db.Init(nil); err != nil { t.Fatalf("KeyDB unavailable: %v", err) }
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	result := RunStorageBenchmarkWithBatches(ctx, db, 100000)
	if result.Transactions != 100000 { t.Fatalf("transactions=%d, want 100000", result.Transactions) }
	if result.ErrorCount != 0 { t.Fatalf("errors=%d, want 0", result.ErrorCount) }
	t.Logf("KeyDB logical TPS=%0.2f duration=%s workers=%d batch=%d", result.TPS, result.Duration, result.Workers, result.BatchSize)
}

func TestYugabytePersistenceBenchmark(t *testing.T) {
	conn := os.Getenv("HED_YUGABYTE_URL")
	if conn == "" { t.Skip("HED_YUGABYTE_URL not configured") }
	db, err := plugin.NewYugabyteEngine(conn)
	if err != nil { t.Fatalf("Yugabyte unavailable: %v", err) }
	defer db.Close()
	if err := db.Init(nil); err != nil { t.Fatalf("Yugabyte init failed: %v", err) }
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	result := RunStorageBenchmarkWithBatches(ctx, db, 100000)
	if result.Transactions != 100000 { t.Fatalf("transactions=%d, want 100000", result.Transactions) }
	if result.ErrorCount != 0 { t.Fatalf("errors=%d, want 0", result.ErrorCount) }
	t.Logf("Yugabyte logical TPS=%0.2f duration=%s workers=%d batch=%d", result.TPS, result.Duration, result.Workers, result.BatchSize)
}
