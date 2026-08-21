package engine

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"hed-core/pkg/plugin"
)

type EventType string

const (
	EventRoute  EventType = "ROUTE"
	EventAck    EventType = "ACK"
	EventOrder  EventType = "ORDER"
	EventCommit EventType = "COMMIT"
	EventSys    EventType = "SYS"
	EventErr    EventType = "ERR"
)

type Event struct {
	Timestamp string    `json:"timestamp"`
	Type      EventType `json:"type"`
	Shard     string    `json:"shard"`
	TxUUID    string    `json:"tx_uuid"`
	Message   string    `json:"message"`
}

type TxPayload struct {
	TxUUID    string `json:"tx_uuid"`
	AccountID string `json:"account_id"`
	Amount    int64  `json:"amount"`
}

type Pipeline struct {
	db          plugin.StateEngine
	shards      atomic.Int64
	subscribers map[chan Event]bool
	subMux      sync.RWMutex
	committed   uint64
	failed      uint64
	engineName  atomic.Value
}

func NewPipeline(db plugin.StateEngine, initialShards int) *Pipeline {
	if initialShards <= 0 {
		initialShards = 1
	}
	engName := "KeyDB"
	if db != nil {
		engName = db.Name()
	}
	p := &Pipeline{
		db:          db,
		subscribers: make(map[chan Event]bool),
	}
	p.shards.Store(int64(initialShards))
	p.engineName.Store(engName)
	return p
}

func (p *Pipeline) SetStorageEngine(db plugin.StateEngine) {
	p.db = db
	if db != nil {
		p.engineName.Store(db.Name())
		p.EmitEvent(EventSys, "", "", fmt.Sprintf("Switched active storage engine to [%s]", db.Name()), 0)
	}
}

func (p *Pipeline) SetShards(count int) {
	if count <= 0 {
		return
	}
	p.shards.Store(int64(count))
	p.EmitEvent(EventSys, "", "", fmt.Sprintf("Updated sharded gateway topology to %d channels", count), 0)
}

func (p *Pipeline) SubmitTransaction(tx *TxPayload) (string, int64, error) {
	start := time.Now()
	if tx == nil {
		atomic.AddUint64(&p.failed, 1)
		return "", 0, fmt.Errorf("nil transaction payload")
	}

	shardCount := p.shards.Load()
	shardIdx := 0
	if len(tx.TxUUID) > 0 {
		shardIdx = int(tx.TxUUID[0]) % int(shardCount)
	}
	shardName := fmt.Sprintf("shard-%d", shardIdx)

	if p.db != nil {
		key := "acc:" + tx.AccountID
		val := []byte(fmt.Sprintf("%d", tx.Amount))
		if err := p.db.PutState(shardName, key, val); err != nil {
			atomic.AddUint64(&p.failed, 1)
			p.EmitEvent(EventErr, shardName, tx.TxUUID, fmt.Sprintf("Execution failed on engine [%s]: %v", p.EngineName(), err), 0)
			return shardName, time.Since(start).Microseconds(), err
		}
	}

	ackLatUs := time.Since(start).Microseconds()
	committedCount := atomic.AddUint64(&p.committed, 1)
	if committedCount%1000 == 0 {
		p.EmitEvent(EventCommit, shardName, tx.TxUUID, fmt.Sprintf("Persisted state [%s] balance=%d via %s", tx.AccountID, tx.Amount, p.EngineName()), ackLatUs)
	}
	return shardName, ackLatUs, nil
}

func (p *Pipeline) TotalCommitted() uint64 {
	return atomic.LoadUint64(&p.committed)
}

func (p *Pipeline) TotalFailed() uint64 {
	return atomic.LoadUint64(&p.failed)
}

func (p *Pipeline) EngineName() string {
	if v := p.engineName.Load(); v != nil {
		return v.(string)
	}
	return "unknown"
}

func (p *Pipeline) SubscribeEvents() chan Event {
	ch := make(chan Event, 500)
	p.subMux.Lock()
	p.subscribers[ch] = true
	p.subMux.Unlock()
	return ch
}

func (p *Pipeline) UnsubscribeEvents(ch chan Event) {
	p.subMux.Lock()
	if _, ok := p.subscribers[ch]; ok {
		delete(p.subscribers, ch)
		close(ch)
	}
	p.subMux.Unlock()
}

func (p *Pipeline) EmitEvent(typ EventType, shard, txUUID, msg string, latUs int64) {
	evt := Event{
		Timestamp: time.Now().Format("15:04:05.000"),
		Type:      typ,
		Shard:     shard,
		TxUUID:    txUUID,
		Message:   msg,
	}

	p.subMux.RLock()
	defer p.subMux.RUnlock()
	for ch := range p.subscribers {
		select {
		case ch <- evt:
		default:
		}
	}
}

func GenerateUUID() string {
	return fmt.Sprintf("%x", time.Now().UnixNano())
}
