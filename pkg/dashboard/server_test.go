package dashboard

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPerTickTxCountScalesWithConfig(t *testing.T) {
	s := &Server{workers: 64, batchSize: 200, dbOpsPerTx: 3}
	got := s.perTickTxCount()
	want := uint64(200)
	if got != want {
		t.Fatalf("perTickTxCount() = %d, want %d", got, want)
	}
}

func TestPerTickTxCountHasFloor(t *testing.T) {
	s := &Server{workers: 1, batchSize: 1, dbOpsPerTx: 1}
	got := s.perTickTxCount()
	if got != 1 {
		t.Fatalf("perTickTxCount() = %d, want 1", got)
	}
}

func TestNewServerStartsPausedUntilTrackingIsEnabled(t *testing.T) {
	s := NewServer(nil, nil, nil, nil)
	if s.isTesting {
		t.Fatal("expected new server to start paused until tracking is enabled")
	}
}

func TestHLFServerLogsExposeTransactionEvents(t *testing.T) {
	s := NewHLFServer(nil)
	s.addTxLog("TX", "REQ account_1 payload=v")
	s.addTxLog("TX", "ACK account_1")

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/hlf/logs", nil)
	s.handleLogs(recorder, req)

	if recorder.Code != 200 {
		t.Fatalf("handleLogs() status = %d, want 200", recorder.Code)
	}

	var payload struct {
		Logs   []map[string]string `json:"logs"`
		TxLogs []map[string]string `json:"txLogs"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
		t.Fatalf("failed to decode logs payload: %v", err)
	}
	if len(payload.TxLogs) != 2 {
		t.Fatalf("expected 2 tx logs, got %d", len(payload.TxLogs))
	}
	if payload.TxLogs[0]["message"] != "ACK account_1" {
		t.Fatalf("unexpected tx log message: %s", payload.TxLogs[0]["message"])
	}
}

func TestHLFServerLifecycleSimulationMarksInstalledAndDeployed(t *testing.T) {
	s := NewHLFServer(nil)
	s.BeginLifecycleSimulation()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if s.IsDeployed() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}

	t.Fatalf("expected lifecycle simulation to mark deployment as ready")
}
