package metrics

import "sync/atomic"

type Counters struct {
	Ingress   atomic.Uint64
	Accepted  atomic.Uint64
	Committed atomic.Uint64
	Failed    atomic.Uint64
	Dropped   atomic.Uint64
}

func (c *Counters) Snapshot() map[string]uint64 {
	return map[string]uint64{
		"ingress": c.Ingress.Load(),
		"accepted": c.Accepted.Load(),
		"committed": c.Committed.Load(),
		"failed": c.Failed.Load(),
		"dropped": c.Dropped.Load(),
	}
}
