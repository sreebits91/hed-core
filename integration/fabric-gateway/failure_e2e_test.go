package fabricgateway

import (
	"context"
	"os"
	"testing"
	"time"

	"hed-core/pkg/v2"
)

func TestLiveFabricFailure(t *testing.T) {
	if os.Getenv("HED_FABRIC_E2E") != "1" || os.Getenv("HED_FABRIC_FAILURE") == "" { t.Skip("live Fabric failure test is environment-gated") }
	cfg := DefaultConfigFromEnv()
	b, err := New(cfg); if err != nil { t.Fatal(err) }
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second); defer cancel()
	tx := v2.Tx{ID: "hed-failure-" + time.Now().UTC().Format("20060102T150405.000000000Z"), Key: "failure", Payload: []byte("failure-test")}
	if err := b.Commit(ctx, tx); err == nil { t.Fatal("expected Fabric commit to fail while the configured peer is unavailable") }
}
