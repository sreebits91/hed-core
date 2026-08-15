package types

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"
)

// PaymentTransaction represents a real-world financial transaction payload
type PaymentTransaction struct {
	TxID       string    `json:"tx_id"`
	SenderID   string    `json:"sender_id"`
	ReceiverID string    `json:"receiver_id"`
	Amount     int64     `json:"amount"` // Stored in cents (e.g. $10.50 = 1050)
	Currency   string    `json:"currency"`
	ChannelID  string    `json:"channel_id"`
	Timestamp  time.Time `json:"timestamp"`
	Signature  string    `json:"signature"`
}

// ContractReceipt represents the outcome of Smart Contract rules validation
type ContractReceipt struct {
	TxID          string    `json:"tx_id"`
	FeeApplied    int64     `json:"fee_applied"`
	NetAmount     int64     `json:"net_amount"`
	Approved      bool      `json:"approved"`
	FailureReason string    `json:"failure_reason,omitempty"`
	ExecutedAt    time.Time `json:"executed_at"`
}

// LedgerEntry represents an immutable hash-linked entry in the audit trail
type LedgerEntry struct {
	Index     uint64    `json:"index"`
	TxID      string    `json:"tx_id"`
	PrevHash  string    `json:"prev_hash"`
	Hash      string    `json:"hash"`
	Timestamp time.Time `json:"timestamp"`
	Data      string    `json:"data"`
}

// ComputeHash generates a SHA-256 hash ensuring block immutability
func (l *LedgerEntry) ComputeHash() string {
	record := fmt.Sprintf("%d%s%s%s%s", l.Index, l.TxID, l.PrevHash, l.Data, l.Timestamp.String())
	h := sha256.New()
	h.Write([]byte(record))
	return hex.EncodeToString(h.Sum(nil))
}
