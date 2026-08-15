package contract

import (
	"fmt"
	"math"
	"strings"
	"time"

	"hed-core/pkg/types"
)

type SmartContractEngine struct {
	MaxTxLimit  int64
	FeeBasisPts int64 // 10 bps = 0.1%
}

func NewSmartContractEngine() *SmartContractEngine {
	return &SmartContractEngine{MaxTxLimit: 1000000, FeeBasisPts: 10}
}

// ExecuteRules validates business rules and calculates smart contract fees.
// Validation is deterministic; ExecutedAt is assigned only for an approved receipt.
func (sc *SmartContractEngine) ExecuteRules(tx *types.PaymentTransaction) (*types.ContractReceipt, error) {
	if tx == nil {
		return nil, fmt.Errorf("transaction is nil")
	}

	fail := func(reason string) (*types.ContractReceipt, error) {
		return &types.ContractReceipt{TxID: tx.TxID, Approved: false, FailureReason: reason}, fmt.Errorf("%s", reason)
	}

	if strings.TrimSpace(tx.TxID) == "" {
		return fail("transaction ID is required")
	}
	if strings.TrimSpace(tx.SenderID) == "" {
		return fail("sender ID is required")
	}
	if strings.TrimSpace(tx.ReceiverID) == "" {
		return fail("receiver ID is required")
	}
	if tx.SenderID == tx.ReceiverID {
		return fail("sender and receiver must differ")
	}
	if strings.TrimSpace(tx.Currency) == "" {
		return fail("currency is required")
	}
	if strings.TrimSpace(tx.ChannelID) == "" {
		return fail("channel ID is required")
	}
	if tx.Timestamp.IsZero() {
		return fail("transaction timestamp is required")
	}
	if tx.Timestamp.After(time.Now().Add(5 * time.Minute)) {
		return fail("transaction timestamp is too far in the future")
	}
	if tx.Amount <= 0 {
		return fail("transaction amount must be positive")
	}
	if sc.MaxTxLimit <= 0 || tx.Amount > sc.MaxTxLimit {
		return fail(fmt.Sprintf("amount exceeds max contract limit of %d", sc.MaxTxLimit))
	}
	if sc.FeeBasisPts < 0 || sc.FeeBasisPts > 10000 {
		return fail("invalid contract fee basis points")
	}
	if sc.FeeBasisPts != 0 && tx.Amount > math.MaxInt64/sc.FeeBasisPts {
		return fail("fee calculation would overflow")
	}

	fee := (tx.Amount * sc.FeeBasisPts) / 10000
	if fee < 1 {
		fee = 1
	}
	if fee >= tx.Amount {
		return fail("fee must be smaller than transaction amount")
	}

	return &types.ContractReceipt{
		TxID: tx.TxID, FeeApplied: fee, NetAmount: tx.Amount - fee,
		Approved: true, ExecutedAt: time.Now(),
	}, nil
}
