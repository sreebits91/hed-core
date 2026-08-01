package chaincode

import (
	"fmt"

	"github.com/hyperledger/fabric-contract-api-go/contractapi"
)

type SmartContract struct {
	contractapi.Contract
}

// WriteStateDelta performs blind writes using composite keys (Shard + TxID)
func (s *SmartContract) WriteStateDelta(ctx contractapi.TransactionContextInterface, shardID string, payload []byte) error {
	txID := ctx.GetStub().GetTxID()

	// Creates unique composite key -> Zero read-set contention across workers
	compositeKey, err := ctx.GetStub().CreateCompositeKey("StateDelta", []string{shardID, txID})
	if err != nil {
		return fmt.Errorf("failed to create composite key: %w", err)
	}

	return ctx.GetStub().PutState(compositeKey, payload)
}
