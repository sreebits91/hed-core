package chaincode

import (
	"fmt"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

type SmartContract struct {
	contractapi.Contract
}

// Commit stores a HED transaction exactly once. The HED transaction ID is the
// idempotency key, so a retry after an uncertain commit state cannot create a
// second business transaction.
func (s *SmartContract) Commit(ctx contractapi.TransactionContextInterface, hedTxID string, payload string) error {
	if hedTxID == "" {
		return fmt.Errorf("HED transaction ID is required")
	}
	key, err := ctx.GetStub().CreateCompositeKey("HEDTx", []string{hedTxID})
	if err != nil {
		return fmt.Errorf("create HED transaction key: %w", err)
	}
	existing, err := ctx.GetStub().GetState(key)
	if err != nil {
		return fmt.Errorf("read HED transaction %s: %w", hedTxID, err)
	}
	if existing != nil {
		return nil
	}
	if err := ctx.GetStub().PutState(key, []byte(payload)); err != nil {
		return fmt.Errorf("write HED transaction %s: %w", hedTxID, err)
	}
	return nil
}

// GetByHEDID is used by reconciliation to distinguish committed transactions
// from transactions that can safely be retried.
func (s *SmartContract) GetByHEDID(ctx contractapi.TransactionContextInterface, hedTxID string) ([]byte, error) {
	if hedTxID == "" {
		return nil, fmt.Errorf("HED transaction ID is required")
	}
	key, err := ctx.GetStub().CreateCompositeKey("HEDTx", []string{hedTxID})
	if err != nil {
		return nil, fmt.Errorf("create HED transaction key: %w", err)
	}
	return ctx.GetStub().GetState(key)
}
