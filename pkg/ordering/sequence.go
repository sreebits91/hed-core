package ordering

import "sync/atomic"

type Sequence uint64

type Generator struct { next atomic.Uint64 }

func (g *Generator) Next() Sequence { return Sequence(g.next.Add(1)) }
