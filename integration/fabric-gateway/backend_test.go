package fabricgateway

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"hed-core/pkg/v2"
)

func TestConfigValidation(t *testing.T) {
	var c Config
	if err := c.Validate(); err == nil { t.Fatal("expected incomplete Fabric configuration to fail") }
	c = DefaultConfigFromEnv()
	if os.Getenv("HED_FABRIC_E2E") != "1" { return }
	if err := c.Validate(); err != nil { t.Fatalf("configured Fabric environment invalid: %v", err) }
}

func TestDefaultLocalConfig(t *testing.T) {
	c := DefaultLocalConfig("/tmp/fabric-samples/test-network")
	if err := c.Validate(); err != nil { t.Fatalf("local config invalid: %v", err) }
	if c.Channel == "" || c.Chaincode == "" || c.Function == "" { t.Fatal("local Fabric defaults incomplete") }
}

func TestUninitializedBackendCommitFailsWithoutNetwork(t *testing.T) {
	b := &Backend{}
	err := b.Commit(context.Background(), v2.Tx{ID: "tx-test", Payload: []byte("x")})
	if err == nil { t.Fatal("expected uninitialized backend failure") }
}

func TestContextCancellationDoesNotSubmit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	b := &Backend{contract: nil, cfg: Config{SubmitTimeout: time.Second}}
	err := b.Commit(ctx, v2.Tx{ID: "tx-test"})
	if err == nil { t.Fatal("expected cancellation/uninitialized error") }
	if !errors.Is(err, context.Canceled) && err.Error() == "" { t.Fatal("expected meaningful failure") }
}
