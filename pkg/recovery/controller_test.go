package recovery

import (
	"testing"
	"time"
)

func TestControllerOpensCircuitAfterThreshold(t *testing.T) {
	now := time.Unix(100, 0)
	c := New(Config{FailureThreshold: 3, Window: time.Second, Cooldown: 2 * time.Second})
	if c.RecordFailure(now) != ActionRetry { t.Fatal("first failure should retry") }
	if c.RecordFailure(now.Add(10 * time.Millisecond)) != ActionRetry { t.Fatal("second failure should retry") }
	if c.RecordFailure(now.Add(20 * time.Millisecond)) != ActionOpenCircuit { t.Fatal("threshold should open circuit") }
	if c.Allow(now.Add(time.Second)) { t.Fatal("circuit should remain open during cooldown") }
	if !c.Allow(now.Add(3 * time.Second)) { t.Fatal("circuit should allow recovery after cooldown") }
}

func TestControllerBackoffIsBounded(t *testing.T) {
	c := New(Config{MaxBackoff: 100 * time.Millisecond})
	if got := c.Backoff(20); got != 100*time.Millisecond { t.Fatalf("backoff=%v, want 100ms", got) }
}
