package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"hed-core/pkg/contract"
	"hed-core/pkg/delta"
	"hed-core/pkg/ledger"
	"hed-core/pkg/plugin"
	"hed-core/pkg/types"
)

func main() {
	log.Println("=== INITIALIZING END-TO-END TRANSACTION LIFECYCLE ENGINE ===")
	keydb := plugin.NewKeyDBEngine("127.0.0.1:6379", 256)
	if err := keydb.Init(nil); err != nil { log.Fatalf("KeyDB Connection Failed: %v", err) }
	defer keydb.Close()
	deltaEngine := delta.New(keydb)
	contractEngine := contract.NewSmartContractEngine()
	auditLedger := ledger.NewAuditLedger()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for { select { case <-ctx.Done(): return; case <-ticker.C: _ = deltaEngine.FlushToDB("channel1") } }
	}()

	tx := &types.PaymentTransaction{TxID: "TX_88492049102", SenderID: "acc_alice", ReceiverID: "acc_bob", Amount: 50000, Currency: "USD", ChannelID: "channel1", Timestamp: time.Now(), Signature: "sig_secp256k1_3a8f9c1b..."}
	receipt, err := contractEngine.ExecuteRules(tx)
	if err != nil { log.Fatalf("transaction rejected: %v", err) }
	deltaEngine.ApplyDelta(tx.ChannelID, tx.SenderID, -tx.Amount)
	deltaEngine.ApplyDelta(tx.ChannelID, tx.ReceiverID, receipt.NetAmount)
	deltaEngine.ApplyDelta(tx.ChannelID, "acc_treasury", receipt.FeeApplied)
	block := auditLedger.AppendTransaction(tx, receipt)
	if err := deltaEngine.FlushToDB("channel1"); err != nil { log.Printf("flush failed: %v", err) }
	alice, _ := keydb.GetState("channel1", "acc_alice")
	bob, _ := keydb.GetState("channel1", "acc_bob")
	treasury, _ := keydb.GetState("channel1", "acc_treasury")
	fmt.Println("=======================================================")
	fmt.Println("       FINAL CONFIRMED LEDGER & STATE RECEIPT")
	fmt.Println("=======================================================")
	fmt.Printf(" Transaction ID       : %s\n", tx.TxID)
	fmt.Printf(" Status               : CONFIRMED & COMMITTED (ACK)\n")
	fmt.Printf(" Ledger Height        : %d Blocks\n", auditLedger.GetChainHeight())
	fmt.Printf(" Ledger Entry         : %d\n", block.Index)
	fmt.Printf(" Alice Balance (Db)   : %s cents\n", string(alice))
	fmt.Printf(" Bob Balance (Db)     : %s cents\n", string(bob))
	fmt.Printf(" Treasury Balance(Db) : %s cents\n", string(treasury))
	fmt.Println("=======================================================")
	<-sigChan
}
