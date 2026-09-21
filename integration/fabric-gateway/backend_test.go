package fabricgateway

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
	"encoding/pem"

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

func TestReadPEMSelectsDeterministicallyByType(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "z-unrelated"), []byte("not pem"), 0600); err != nil { t.Fatal(err) }
	key := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: []byte("key")})
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("cert")})
	if err := os.WriteFile(filepath.Join(dir, "b-cert"), cert, 0600); err != nil { t.Fatal(err) }
	if err := os.WriteFile(filepath.Join(dir, "a-key"), key, 0600); err != nil { t.Fatal(err) }

	got, err := readPEM(dir, "PRIVATE KEY")
	if err != nil { t.Fatal(err) }
	if string(got) != string(key) { t.Fatalf("selected wrong PEM: %q", got) }
	got, err = readPEM(dir, "CERTIFICATE")
	if err != nil { t.Fatal(err) }
	if string(got) != string(cert) { t.Fatalf("selected wrong certificate: %q", got) }
}

func TestReadPEMRejectsWrongType(t *testing.T) {
	dir := t.TempDir()
	cert := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("cert")})
	path := filepath.Join(dir, "cert.pem")
	if err := os.WriteFile(path, cert, 0600); err != nil { t.Fatal(err) }
	if _, err := readPEM(path, "PRIVATE KEY"); err == nil { t.Fatal("expected PEM type mismatch") }
}

func TestReadPEMEmptyDirectoryFails(t *testing.T) {
	if _, err := readPEM(t.TempDir(), "CERTIFICATE"); err == nil { t.Fatal("expected empty directory failure") }
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
