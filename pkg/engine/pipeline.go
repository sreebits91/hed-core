package engine

import (
	"crypto/rand"
	"encoding/hex"
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
	mu          sync.RWMutex
	db          plugin.StateEngine
	shards      int
	subscribers map[chan Event]bool
	committed   uint64
	failed      uint64
	engineName  string
}

func NewPipeline(db plugin.StateEngine, initialShards int) *Pipeline {
	if initialShards <= 0 {
		initialShards = 1
	}
	engName := "KeyDB"
	if db != nil {
		engName = db.Name()
	}
	return &Pipeline{db: db, shards: initialShards, subscribers: make(map[chan Event]bool), engineName: engName}
}

func (p *Pipeline) SetStorageEngine(db plugin.StateEngine) {
	p.mu.Lock()
	p.db = db
	if db != nil {
		p.engineName = db.Name()
	}
	p.mu.Unlock()
	if db != nil {
		p.EmitEvent(EventSys, "", "", fmt.Sprintf("Switched active storage engine to [%s]", db.Name()), 0)
	}
}

func (p *Pipeline) SetShards(count int) {
	if count <= 0 {
		return
	}
	p.mu.Lock()
	p.shards = count
	p.mu.Unlock()
	p.EmitEvent(EventSys, "", "", fmt.Sprintf("Updated sharded gateway topology to %d channels", count), 0)
}

func (p *Pipeline) SubmitTransaction(tx *TxPayload) (string, int64, error) {
	start := time.Now()
	if tx == nil {
		atomic.AddUint64(&p.failed, 1)
		return "", 0, fmt.Errorf("transaction payload is nil")
	}

	p.mu.RLock()
	shardCount := p.shards
	db := p.db
	p.mu.RUnlock()
	if shardCount <= 0 {
		shardCount = 1
	}

	shardIdx := 0
	if len(tx.TxUUID) > 0 {
		shardIdx = int(tx.TxUUID[0]) % shardCount
	}
	shardName := fmt.Sprintf("shard-%d", shardIdx)

	if db != nil {
		key := fmt.Sprintf("acc:%s", tx.AccountID)
		if err := db.PutState(shardName, key, []byte(fmt.Sprintf("%d", tx.Amount))); err != nil {
			atomic.AddUint64(&p.failed, 1)
			p.EmitEvent(EventErr, shardName, tx.TxUUID, fmt.Sprintf("Execution failed on engine [%s]: %v", p.EngineName(), err), 0)
			return shardName, time.Since(start).Microseconds(), err
		}
	}

	latency := time.Since(start).Microseconds()
	count := atomic.AddUint64(&p.committed, 1)
	if count%1000 == 0 {
		p.EmitEvent(EventCommit, shardName, tx.TxUUID, fmt.Sprintf("Persisted state [%s] balance=%d via %s", tx.AccountID, tx.Amount, p.EngineName()), latency)
	}
	return shardName, latency, nil
}

func (p *Pipeline) TotalCommitted() uint64 { return atomic.LoadUint64(&p.committed) }
func (p *Pipeline) TotalFailed() uint64    { return atomic.LoadUint64(&p.failed) }

func (p *Pipeline) EngineName() string {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.engineName
}

func (p *Pipeline) SubscribeEvents() chan Event {
	ch := make(chan Event, 500)
	p.mu.Lock()
	p.subscribers[ch] = true
	p.mu.Unlock()
	return ch
}

func (p *Pipeline) UnsubscribeEvents(ch chan Event) {
	p.mu.Lock()
	if _, ok := p.subscribers[ch]; ok {
		delete(p.subscribers, ch)
		close(ch)
	}
	p.mu.Unlock()
}

func (p *Pipeline) EmitEvent(typ EventType, shard, txUUID, msg string, latUs int64) {
	evt := Event{Timestamp: time.Now().Format("15:04:05.000"), Type: typ, Shard: shard, TxUUID: txUUID, Message: msg}
	p.mu.RLock()
	defer p.mu.RUnlock()
	for ch := range p.subscribers {
		select {
		case ch <- evt:
		default:
		}
	}
}

// GenerateUUID creates a 128-bit random identifier. Falling back to the clock
// keeps the hot path operational even if the system entropy source is unavailable.
func GenerateUUID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	return fmt.Sprintf("%016x", time.Now().UnixNano())
}
