package hlf

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDeployerExecuteStagePropagatesFailure(t *testing.T) {
	d := NewDeployer(DefaultOptions())
	err := d.executeStage(context.Background(), 0, func(context.Context) error {
		return errors.New("boom")
	})
	if err == nil || !strings.Contains(err.Error(), "Pre-requisites Check") {
		t.Fatalf("expected stage error, got %v", err)
	}
	if d.stages[0].Status != StatusFailed {
		t.Fatalf("status=%s, want failed", d.stages[0].Status)
	}
	if d.stages[1].Status != StatusPending {
		t.Fatalf("next stage status=%s, want pending", d.stages[1].Status)
	}
}

func TestDeployerExecuteStageHonorsCancellation(t *testing.T) {
	d := NewDeployer(DefaultOptions())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := d.executeStage(ctx, 0, func(context.Context) error {
		t.Fatal("stage function must not run after cancellation")
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want context canceled", err)
	}
	if d.stages[0].Status != StatusFailed {
		t.Fatalf("status=%s, want failed", d.stages[0].Status)
	}
}

func TestDeployerRunDeploymentContextStopsAfterFirstFailure(t *testing.T) {
	// A cancelled context must fail before any external Fabric command is started.
	d := NewDeployer(DefaultOptions())
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()
	time.Sleep(time.Millisecond)

	err := d.RunDeploymentContext(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want deadline exceeded", err)
	}
	if d.stages[0].Status != StatusPending {
		t.Fatalf("stage status=%s, want pending when deadline exists before execution", d.stages[0].Status)
	}
}
