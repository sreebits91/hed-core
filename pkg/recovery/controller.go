package recovery

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Action string

const (
	ActionRetry Action = "retry"
	ActionBackoff Action = "backoff"
	ActionOpenCircuit Action = "open_circuit"
	ActionRecover Action = "recover"
)

type Config struct {
	FailureThreshold int
	Window time.Duration
	Cooldown time.Duration
	MaxBackoff time.Duration
	RecoveryTimeout time.Duration
	MaxRecoveryAttempts int
}

type Controller struct {
	mu sync.Mutex
	cfg Config
	failures int
	windowStart time.Time
	openUntil time.Time
}

// Recoverer contains only idempotent dependency operations. The controller
// never creates or replays transactions; it only restores dependencies.
type Recoverer interface {
	ReconnectStorage(context.Context) error
	ReconnectFabric(context.Context) error
	Drain(context.Context) error
	Resume(context.Context) error
}

func New(cfg Config) *Controller {
	if cfg.FailureThreshold <= 0 { cfg.FailureThreshold = 5 }
	if cfg.Window <= 0 { cfg.Window = 10*time.Second }
	if cfg.Cooldown <= 0 { cfg.Cooldown = 5*time.Second }
	if cfg.MaxBackoff <= 0 { cfg.MaxBackoff = time.Second }
	if cfg.RecoveryTimeout <= 0 { cfg.RecoveryTimeout = 10*time.Second }
	if cfg.MaxRecoveryAttempts <= 0 { cfg.MaxRecoveryAttempts = 3 }
	return &Controller{cfg: cfg}
}

func (c *Controller) RecordSuccess(now time.Time) Action {
	c.mu.Lock(); defer c.mu.Unlock()
	wasOpen := !c.openUntil.IsZero() && !now.Before(c.openUntil)
	c.openUntil = time.Time{}
	c.failures = 0
	c.windowStart = now
	if wasOpen { return ActionRecover }
	return ActionRecover
}

func (c *Controller) RecordFailure(now time.Time) Action {
	c.mu.Lock(); defer c.mu.Unlock()
	if !c.openUntil.IsZero() && now.Before(c.openUntil) { return ActionOpenCircuit }
	if c.windowStart.IsZero() || now.Sub(c.windowStart) > c.cfg.Window { c.windowStart=now; c.failures=0 }
	c.failures++
	if c.failures >= c.cfg.FailureThreshold { c.openUntil=now.Add(c.cfg.Cooldown); return ActionOpenCircuit }
	return ActionRetry
}

func (c *Controller) Allow(now time.Time) bool {
	c.mu.Lock(); defer c.mu.Unlock()
	return c.openUntil.IsZero() || !now.Before(c.openUntil)
}

func (c *Controller) Backoff(attempt int) time.Duration {
	if attempt < 1 { attempt=1 }
	backoff:=10*time.Millisecond
	for i:=1; i<attempt; i++ { if backoff >= c.cfg.MaxBackoff/2 { return c.cfg.MaxBackoff }; backoff*=2 }
	if backoff>c.cfg.MaxBackoff { return c.cfg.MaxBackoff }
	return backoff
}

// Recover performs bounded, ordered recovery: drain -> reconnect storage ->
// reconnect Fabric -> resume. Every operation is guarded by a total timeout.
func (c *Controller) Recover(parent context.Context, r Recoverer) error {
	if r == nil { return errors.New("recovery: nil recoverer") }
	ctx, cancel := context.WithTimeout(parent, c.cfg.RecoveryTimeout)
	defer cancel()
	if err:=r.Drain(ctx); err!=nil { return err }
	for attempt:=1; attempt<=c.cfg.MaxRecoveryAttempts; attempt++ {
		if err:=r.ReconnectStorage(ctx); err!=nil { if !sleep(ctx,c.Backoff(attempt)){return ctx.Err()}; continue }
		if err:=r.ReconnectFabric(ctx); err!=nil { if !sleep(ctx,c.Backoff(attempt)){return ctx.Err()}; continue }
		if err:=r.Resume(ctx); err!=nil { if !sleep(ctx,c.Backoff(attempt)){return ctx.Err()}; continue }
		return nil
	}
	return errors.New("recovery: maximum attempts exceeded")
}

func sleep(ctx context.Context,d time.Duration) bool { t:=time.NewTimer(d); defer t.Stop(); select { case <-ctx.Done(): return false; case <-t.C: return true } }
