package dashboard

import (
	"testing"
	"time"
)

func TestPerTickTxCountScalesWithConfig(t *testing.T) {
	s := &Server{workers: 64, batchSize: 200, dbOpsPerTx: 3}
	got := s.perTickTxCount()
	want := uint64((64 * 200 * 3) / 48)
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
func TestHLFServerLifecycleSimulationMarksInstalledAndDeployed(t *testing.T) {
	s := NewHLFServer(nil)
	s.BeginLifecycleSimulation()

	deadline := time.Now().Add(750 * time.Millisecond)
	for time.Now().Before(deadline) {
		if s.IsDeployed() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}

	t.Fatalf("expected lifecycle simulation to mark deployment as ready")
}
