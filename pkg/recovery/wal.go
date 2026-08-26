package recovery

import (
	"bufio"
	"encoding/json"
	"os"
	"sync"

	"hed-core/pkg/engine"
)

type WAL struct { mu sync.Mutex; file *os.File }

func Open(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil { return nil, err }
	return &WAL{file: f}, nil
}

func (w *WAL) Append(tx *engine.TxPayload) error {
	w.mu.Lock(); defer w.mu.Unlock()
	enc := json.NewEncoder(w.file)
	return enc.Encode(tx)
}

func (w *WAL) Sync() error { w.mu.Lock(); defer w.mu.Unlock(); return w.file.Sync() }

func (w *WAL) Replay(fn func(*engine.TxPayload) error) error {
	w.mu.Lock(); defer w.mu.Unlock()
	if _, err := w.file.Seek(0, 0); err != nil { return err }
	s := bufio.NewScanner(w.file)
	for s.Scan() {
		var tx engine.TxPayload
		if err := json.Unmarshal(s.Bytes(), &tx); err != nil { return err }
		if err := fn(&tx); err != nil { return err }
	}
	return s.Err()
}

func (w *WAL) Close() error { w.mu.Lock(); defer w.mu.Unlock(); return w.file.Close() }
