package fabricgateway

import (
	"context"
	"os"
	"testing"
	"time"

	"hed-core/pkg/v2"
)

func TestLiveFabricE2E(t *testing.T) {
	if os.Getenv("HED_FABRIC_E2E") != "1" { t.Skip("set HED_FABRIC_E2E=1 to run against a live Fabric network") }
	cfg := DefaultConfigFromEnv()
	b, err := New(cfg); if err != nil { t.Fatal(err) }
	defer b.Close()

	tx := v2.Tx{ID: "hed-e2e-" + time.Now().UTC().Format("20060102T150405.000000000Z"), Key: "e2e", Payload: []byte("hed-e2e-payload")}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second); defer cancel()
	if err := b.Commit(ctx, tx); err != nil { t.Fatal(err) }
	status, err := b.Status(ctx, tx.ID); if err != nil { t.Fatal(err) }
	if status != v2.LedgerCommitted { t.Fatalf("status=%s, want %s", status, v2.LedgerCommitted) }
}

func TestLiveFabricReconciliation(t *testing.T) {
	if os.Getenv("HED_FABRIC_E2E") != "1" { t.Skip("set HED_FABRIC_E2E=1 to run against a live Fabric network") }
	cfg := DefaultConfigFromEnv(); b, err := New(cfg); if err != nil { t.Fatal(err) }; defer b.Close()
	reconciler := v2.NewReconciler(b)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second); defer cancel()
	status, err := reconciler.Reconcile(ctx, "hed-nonexistent-reconciliation-id")
	if err != nil { t.Fatal(err) }
	if status != v2.LedgerNotCommitted { t.Fatalf("status=%s, want %s", status, v2.LedgerNotCommitted) }
}
