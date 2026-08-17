package hlf

import (
	"context"
	"errors"
	"sync/atomic"

	"hed-core/pkg/engine"
)

// Gateway submits an already validated batch to a Fabric gateway/client.
// Implementations must be idempotent for a given transaction identity.
type Gateway interface { SubmitBatch(context.Context, []*engine.TxPayload) error }

type GatewaySink struct { gateway Gateway; submitted uint64; failed uint64 }
func NewGatewaySink(g Gateway) (*GatewaySink,error) { if g==nil{return nil,errors.New("hlf: nil gateway")}; return &GatewaySink{gateway:g},nil }
func (s *GatewaySink) Commit(ctx context.Context,batch []*engine.TxPayload) error {
	if err:=s.gateway.SubmitBatch(ctx,batch); err!=nil {atomic.AddUint64(&s.failed,uint64(len(batch)));return err}
	atomic.AddUint64(&s.submitted,uint64(len(batch))); return nil
}
func (s *GatewaySink) Submitted() uint64{return atomic.LoadUint64(&s.submitted)}
func (s *GatewaySink) Failed() uint64{return atomic.LoadUint64(&s.failed)}
