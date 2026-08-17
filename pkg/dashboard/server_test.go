package dashboard

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNewServerStartsPausedUntilBenchmarkStarts(t *testing.T) {
	s := NewServer(nil, nil, nil, nil)
	if s.isTesting != 0 {
		t.Fatal("expected new server to start with benchmark inactive")
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
	// HLFServer intentionally returns newest events first.
	if payload.TxLogs[0]["message"] != "ACK account_1" {
		t.Fatalf("unexpected newest tx log message: %s", payload.TxLogs[0]["message"])
	}
	if payload.TxLogs[1]["message"] != "REQ account_1 payload=v" {
		t.Fatalf("unexpected oldest tx log message: %s", payload.TxLogs[1]["message"])
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
