package hlf

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRegisterListenerDoesNotBlockOnReplay(t *testing.T) {
	d := NewDeployer(DeployOptions{})
	for i := 0; i < 100; i++ {
		d.broadcast(fmt.Sprintf("history-%d", i))
	}

	ch := make(chan string)
	done := make(chan struct{})
	go func() {
		d.RegisterListener(ch)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("RegisterListener blocked while replaying to a full listener")
	}
}

func TestBroadcastConcurrentWithListenerRegistration(t *testing.T) {
	d := NewDeployer(DeployOptions{})

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := make(chan string, 8)
			d.RegisterListener(ch)
			d.broadcast("message")
			d.UnregisterListener(ch)
		}()
	}

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			d.broadcast("concurrent")
		}()
	}
	wg.Wait()
}

func TestExecuteStageFailsFast(t *testing.T) {
	d := NewDeployer(DeployOptions{})
	called := false

	err := d.executeStage(context.Background(), 0, func(context.Context) error {
		called = true
		return context.Canceled
	})
	if err == nil {
		t.Fatal("expected executeStage to return the stage error")
	}
	if !called {
		t.Fatal("stage callback was not invoked")
	}
	if d.stages[0].Status != StatusFailed {
		t.Fatalf("expected failed stage, got %q", d.stages[0].Status)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called = false
	err = d.executeStage(ctx, 1, func(context.Context) error {
		called = true
		return nil
	})
	if err == nil {
		t.Fatal("expected canceled context to stop stage execution")
	}
	if called {
		t.Fatal("stage callback ran after context cancellation")
	}
}

func TestChannelsResolution(t *testing.T) {
	cases := []struct {
		name string
		opts DeployOptions
		want []string
	}{
		{name: "explicit channels", opts: DeployOptions{Channels: []string{"a", "b"}}, want: []string{"a", "b"}},
		{name: "legacy channel", opts: DeployOptions{ChannelID: "legacy"}, want: []string{"legacy"}},
		{name: "default", opts: DeployOptions{}, want: []string{DefaultChannelID}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := NewDeployer(tc.opts).channels()
			if fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("channels() = %v, want %v", got, tc.want)
			}
		})
	}
}
