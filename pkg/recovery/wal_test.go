package recovery

import (
	"path/filepath"
	"testing"
	"hed-core/pkg/engine"
)

func TestWALAppendReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "hed.wal")
	w, err := Open(path); if err != nil { t.Fatal(err) }
	defer w.Close()
	in := &engine.TxPayload{TxUUID: "tx-1", AccountID: "acct", Amount: 42}
	if err := w.Append(in); err != nil { t.Fatal(err) }
	if err := w.Sync(); err != nil { t.Fatal(err) }
	var out engine.TxPayload
	if err := w.Replay(func(tx *engine.TxPayload) error { out = *tx; return nil }); err != nil { t.Fatal(err) }
	if out.TxUUID != in.TxUUID || out.Amount != in.Amount { t.Fatalf("replay mismatch: %#v", out) }
}
