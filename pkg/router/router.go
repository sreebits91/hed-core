package router

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"hed-core/pkg/plugin"
)

type ProofOfLock struct {
	ProofID     string `json:"proof_id"`
	FromAccount string `json:"from_account"`
	FromShard   string `json:"from_shard"`
	ToShard     string `json:"to_shard"`
	Amount      int64  `json:"amount"`
	Timestamp   int64  `json:"timestamp"`
}

type GatewayRouter struct {
	totalShards int
	db          plugin.StateEngine
	proofStore  sync.Map // ProofID -> *ProofOfLock
}

func NewGatewayRouter(shards int, db plugin.StateEngine) *GatewayRouter {
	return &GatewayRouter{
		totalShards: shards,
		db:          db,
	}
}

// RouteToShard routes account payloads according to: ShardID = Hash(AccountID) % N
func (r *GatewayRouter) RouteToShard(accountID string) string {
	h := fnv.New32a()
	h.Write([]byte(accountID))
	shardID := h.Sum32() % uint32(r.totalShards)
	return fmt.Sprintf("shard_channel_%02d", shardID)
}

// 2PC Phase 1: Lock Funds on Source Channel & Generate Cryptographic Proof-of-Lock
func (r *GatewayRouter) Phase1_LockFunds(fromAccount string, amount int64, toShard string) (*ProofOfLock, error) {
	fromShard := r.RouteToShard(fromAccount)

	// Deduct / Lock funds on Source Channel
	lockKey := fmt.Sprintf("lock_%s_%d", fromAccount, time.Now().UnixNano())
	if err := r.db.PutState(fromShard, lockKey, []byte(fmt.Sprintf("-%d", amount))); err != nil {
		return nil, fmt.Errorf("phase 1 lock failed: %w", err)
	}

	// Generate Cryptographic Proof-of-Lock Hash
	data := fmt.Sprintf("%s:%s:%s:%d:%d", fromAccount, fromShard, toShard, amount, time.Now().UnixNano())
	hash := sha256.Sum256([]byte(data))
	proofID := hex.EncodeToString(hash[:16])

	proof := &ProofOfLock{
		ProofID:     proofID,
		FromAccount: fromAccount,
		FromShard:   fromShard,
		ToShard:     toShard,
		Amount:      amount,
		Timestamp:   time.Now().UnixNano(),
	}

	r.proofStore.Store(proofID, proof)
	return proof, nil
}

// 2PC Phase 2: Claim Funds on Destination Channel by Validating Proof-of-Lock
func (r *GatewayRouter) Phase2_ClaimFunds(proofID string, toAccount string) error {
	val, exists := r.proofStore.Load(proofID)
	if !exists {
		return fmt.Errorf("invalid or non-existent Proof-of-Lock: %s", proofID)
	}
	proof := val.(*ProofOfLock)

	// Validate Destination Shard Match
	expectedDestShard := r.RouteToShard(toAccount)
	if proof.ToShard != expectedDestShard {
		return fmt.Errorf("proof destination mismatch: expected %s, got %s", expectedDestShard, proof.ToShard)
	}

	// Unlock / Mint funds on Destination Channel
	mintKey := fmt.Sprintf("mint_%s_%s", toAccount, proofID)
	if err := r.db.PutState(proof.ToShard, mintKey, []byte(fmt.Sprintf("+%d", proof.Amount))); err != nil {
		return fmt.Errorf("phase 2 mint failed: %w", err)
	}

	// Consume Proof to prevent double spending
	r.proofStore.Delete(proofID)
	return nil
}
