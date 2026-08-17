package recovery

import (
	"sync"
	"time"
)

// Action describes a bounded, deterministic recovery step. This controller is
// intentionally rule-based: it is self-healing automation, not an AI model.
type Action string

const (
	ActionRetry Action = "retry"
	ActionBackoff Action = "backoff"
	ActionOpenCircuit Action = "open_circuit"
	ActionRecover Action = "recover"
)

type Config struct {
	FailureThreshold int
	Window           time.Duration
	Cooldown         time.Duration
	MaxBackoff       time.Duration
}

type Controller struct {
	mu          sync.Mutex
	cfg         Config
	failures    int
	windowStart time.Time
	openUntil   time.Time
}

func New(cfg Config) *Controller {
	if cfg.FailureThreshold <= 0 { cfg.FailureThreshold = 5 }
	if cfg.Window <= 0 { cfg.Window = 10 * time.Second }
	if cfg.Cooldown <= 0 { cfg.Cooldown = 5 * time.Second }
	if cfg.MaxBackoff <= 0 { cfg.MaxBackoff = time.Second }
	return &Controller{cfg: cfg}
}

func (c *Controller) RecordSuccess(now time.Time) Action {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.openUntil.IsZero() && !now.Before(c.openUntil) {
		c.openUntil = time.Time{}
		c.failures = 0
		c.windowStart = now
		return ActionRecover
	}
	c.failures = 0
	c.windowStart = now
	return ActionRecover
}

func (c *Controller) RecordFailure(now time.Time) Action {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.openUntil.IsZero() && now.Before(c.openUntil) { return ActionOpenCircuit }
	if c.windowStart.IsZero() || now.Sub(c.windowStart) > c.cfg.Window {
		c.windowStart = now
		c.failures = 0
	}
	c.failures++
	if c.failures >= c.cfg.FailureThreshold {
		c.openUntil = now.Add(c.cfg.Cooldown)
		return ActionOpenCircuit
	}
	return ActionRetry
}

func (c *Controller) Allow(now time.Time) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.openUntil.IsZero() || !now.Before(c.openUntil)
}

func (c *Controller) Backoff(attempt int) time.Duration {
	if attempt < 1 { attempt = 1 }
	backoff := 10 * time.Millisecond
	for i := 1; i < attempt; i++ {
		backoff *= 2
		if backoff >= c.cfg.MaxBackoff { return c.cfg.MaxBackoff }
	}
	if backoff > c.cfg.MaxBackoff { return c.cfg.MaxBackoff }
	return backoff
}
