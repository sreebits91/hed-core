package ledger

import (
	"fmt"
	"sync"
	"time"

	"hed-core/pkg/types"
)

type AuditLedger struct {
	sync.Mutex
	chain    []*types.LedgerEntry
	lastHash string
}

func NewAuditLedger() *AuditLedger {
	genesis := &types.LedgerEntry{
		Index:     0,
		TxID:      "GENESIS_BLOCK",
		PrevHash:  "0000000000000000000000000000000000000000000000000000000000000000",
		Timestamp: time.Now(),
		Data:      "Genesis Block - HED Core Ledger",
	}
	genesis.Hash = genesis.ComputeHash()

	return &AuditLedger{
		chain:    []*types.LedgerEntry{genesis},
		lastHash: genesis.Hash,
	}
}

// AppendTransaction links a transaction into the immutable cryptographic ledger
func (al *AuditLedger) AppendTransaction(tx *types.PaymentTransaction, receipt *types.ContractReceipt) *types.LedgerEntry {
	al.Lock()
	defer al.Unlock()

	dataStr := fmt.Sprintf("SENDER:%s|RECV:%s|NET_AMT:%d|FEE:%d", tx.SenderID, tx.ReceiverID, receipt.NetAmount, receipt.FeeApplied)

	entry := &types.LedgerEntry{
		Index:     uint64(len(al.chain)),
		TxID:      tx.TxID,
		PrevHash:  al.lastHash,
		Timestamp: time.Now(),
		Data:      dataStr,
	}
	entry.Hash = entry.ComputeHash()

	al.chain = append(al.chain, entry)
	al.lastHash = entry.Hash

	return entry
}

func (al *AuditLedger) GetChainHeight() int {
	al.Lock()
	defer al.Unlock()
	return len(al.chain)
}