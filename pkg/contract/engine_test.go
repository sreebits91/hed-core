package contract

import (
    "strings"
    "testing"
    "time"

    "hed-core/pkg/types"
)

func validTransaction() *types.PaymentTransaction {
    return &types.PaymentTransaction{
        TxID: "tx-1", SenderID: "alice", ReceiverID: "bob", Amount: 10000,
        Currency: "USD", ChannelID: "channel1", Timestamp: time.Now(),
    }
}

func TestExecuteRulesRejectsNil(t *testing.T) {
    if _, err := NewSmartContractEngine().ExecuteRules(nil); err == nil { t.Fatal("expected nil transaction error") }
}

func TestExecuteRulesRejectsInvalidIdentity(t *testing.T) {
    cases := []struct{name string; mutate func(*types.PaymentTransaction)}{
        {"missing tx id", func(tx *types.PaymentTransaction) { tx.TxID = "" }},
        {"missing sender", func(tx *types.PaymentTransaction) { tx.SenderID = "" }},
        {"same sender receiver", func(tx *types.PaymentTransaction) { tx.ReceiverID = tx.SenderID }},
        {"missing currency", func(tx *types.PaymentTransaction) { tx.Currency = "" }},
        {"missing channel", func(tx *types.PaymentTransaction) { tx.ChannelID = "" }},
        {"zero timestamp", func(tx *types.PaymentTransaction) { tx.Timestamp = time.Time{} }},
    }
    for _, tc := range cases {
        t.Run(tc.name, func(t *testing.T) {
            tx := validTransaction(); tc.mutate(tx)
            if _, err := NewSmartContractEngine().ExecuteRules(tx); err == nil { t.Fatal("expected validation error") }
        })
    }
}

func TestExecuteRulesCalculatesFee(t *testing.T) {
    receipt, err := NewSmartContractEngine().ExecuteRules(validTransaction())
    if err != nil { t.Fatal(err) }
    if !receipt.Approved || receipt.FeeApplied != 10 || receipt.NetAmount != 9990 { t.Fatalf("unexpected receipt: %+v", receipt) }
}

func TestExecuteRulesRejectsFutureTimestamp(t *testing.T) {
    tx := validTransaction(); tx.Timestamp = time.Now().Add(6 * time.Minute)
    if _, err := NewSmartContractEngine().ExecuteRules(tx); err == nil || !strings.Contains(err.Error(), "future") { t.Fatalf("expected future timestamp error, got %v", err) }
}
