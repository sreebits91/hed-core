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
	EventRoute EventType = "ROUTE"
	EventAck EventType = "ACK"
	EventOrder EventType = "ORDER"
	EventCommit EventType = "COMMIT"
	EventSys EventType = "SYS"
	EventErr EventType = "ERR"
)

type Event struct {
	Timestamp string `json:"timestamp"`
	Type EventType `json:"type"`
	Shard string `json:"shard"`
	TxUUID string `json:"tx_uuid"`
	Message string `json:"message"`
}

type TxPayload struct {
	TxUUID string `json:"tx_uuid"`
	AccountID string `json:"account_id"`
	Amount int64 `json:"amount"`
}

type Pipeline struct {
	db plugin.StateEngine
	mu sync.RWMutex
	shards int
	subscribers map[chan Event]bool
	subMux sync.RWMutex
	committed uint64
	failed uint64
	engineName string
}

func NewPipeline(db plugin.StateEngine, initialShards int) *Pipeline {
	if initialShards < 1 {
		initialShards = 1
	}
	engName := "none"
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
	} else {
		p.engineName = "none"
	}
	name := p.engineName
	p.mu.Unlock()
	p.EmitEvent(EventSys, "", "", fmt.Sprintf("Switched active storage engine to [%s]", name), 0)
}

func (p *Pipeline) SetShards(count int) {
	if count < 1 {
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
		return "", 0, fmt.Errorf("transaction is nil")
	}

	p.mu.RLock()
	shards := p.shards
	db := p.db
	engineName := p.engineName
	p.mu.RUnlock()
	if shards < 1 {
		return "", 0, fmt.Errorf("invalid shard count: %d", shards)
	}

	shardIdx := 0
	if len(tx.TxUUID) > 0 {
		shardIdx = int(tx.TxUUID[0]) % shards
	}
	shardName := fmt.Sprintf("shard-%d", shardIdx)

	if db != nil {
		key := fmt.Sprintf("acc:%s", tx.AccountID)
		if err := db.PutState(shardName, key, []byte(fmt.Sprintf("%d", tx.Amount))); err != nil {
			atomic.AddUint64(&p.failed, 1)
			p.EmitEvent(EventErr, shardName, tx.TxUUID, fmt.Sprintf("Execution failed on engine [%s]: %v", engineName, err), 0)
			return shardName, time.Since(start).Microseconds(), err
		}
	}

	latUs := time.Since(start).Microseconds()
	committedCount := atomic.AddUint64(&p.committed, 1)
	if committedCount%1000 == 0 {
		p.EmitEvent(EventCommit, shardName, tx.TxUUID, fmt.Sprintf("Persisted state [%s] balance=%d via %s", tx.AccountID, tx.Amount, engineName), latUs)
	}
	return shardName, latUs, nil
}

func (p *Pipeline) TotalCommitted() uint64 { return atomic.LoadUint64(&p.committed) }
func (p *Pipeline) TotalFailed() uint64 { return atomic.LoadUint64(&p.failed) }

func (p *Pipeline) EngineName() string {
	p.mu.RLock()
	name := p.engineName
	p.mu.RUnlock()
	return name
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
	evt := Event{Timestamp: time.Now().Format("15:04:05.000"), Type: typ, Shard: shard, TxUUID: txUUID, Message: msg}
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
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		// crypto/rand failure is exceptionally rare; keep the API non-error-returning
		// while making the fallback explicit and unique enough for local benchmarking.
		return fmt.Sprintf("%016x", time.Now().UnixNano())
	}
	buf[6] = (buf[6] & 0x0f) | 0x40
	buf[8] = (buf[8] & 0x3f) | 0x80
	return hex.EncodeToString(buf)
}
