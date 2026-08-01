package contract

import (
	"fmt"
	"time"

	"hed-core/pkg/types"
)

type SmartContractEngine struct {
	MaxTxLimit int64
	FeeBasisPts int64 // 10 bps = 0.1%
}

func NewSmartContractEngine() *SmartContractEngine {
	return &SmartContractEngine{
		MaxTxLimit:  1000000, // $10,000 max limit per transfer
		FeeBasisPts: 10,      // 0.10% fee
	}
}

// ExecuteRules validates business rules and calculates smart contract fees
func (sc *SmartContractEngine) ExecuteRules(tx *types.PaymentTransaction) (*types.ContractReceipt, error) {
	// 1. Basic Schema Validation
	if tx.Amount <= 0 {
		return &types.ContractReceipt{
			TxID:          tx.TxID,
			Approved:      false,
			FailureReason: "Transaction amount must be positive",
		}, fmt.Errorf("invalid transaction amount")
	}

	// 2. Maximum Limit Rule
	if tx.Amount > sc.MaxTxLimit {
		return &types.ContractReceipt{
			TxID:          tx.TxID,
			Approved:      false,
			FailureReason: fmt.Sprintf("Amount exceeds max contract limit of %d", sc.MaxTxLimit),
		}, fmt.Errorf("contract limit exceeded")
	}

	// 3. Fee Calculation (Smart Contract Business Logic)
	fee := (tx.Amount * sc.FeeBasisPts) / 10000
	if fee < 1 {
		fee = 1 // Minimum 1 cent fee
	}
	netAmount := tx.Amount - fee

	return &types.ContractReceipt{
		TxID:          tx.TxID,
		FeeApplied:    fee,
		NetAmount:     netAmount,
		Approved:      true,
		ExecutedAt:    time.Now(),
	}, nil
}