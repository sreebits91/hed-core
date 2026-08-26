package partition

import (
	"hash/fnv"

	"hed-core/pkg/engine"
	"hed-core/pkg/queue"
)

type Router struct {
	queues []queue.TransactionQueue
}

func New(queues []queue.TransactionQueue) *Router { return &Router{queues: queues} }

func (r *Router) Route(tx *engine.TxPayload) bool {
	if tx == nil || len(r.queues) == 0 {
		return false
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(tx.TxUUID))
	return r.queues[int(h.Sum32()%uint32(len(r.queues)))].Push(tx)
}

func (r *Router) PartitionCount() int { return len(r.queues) }
