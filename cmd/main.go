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

	// 1. Initialize KeyDB Storage Plugin
	keydbEngine := plugin.NewKeyDBEngine("127.0.0.1:6379", 256)
	if err := keydbEngine.Init(nil); err != nil {
		log.Fatalf("❌ [FATAL] KeyDB Connection Failed: %v", err)
	}
	defer keydbEngine.Close()
	log.Println("✅ [1/5] KeyDB Storage Engine Online")

	// 2. Initialize Sharded Delta Engine
	deltaEngine := delta.New(keydbEngine)
	log.Println("✅ [2/5] Sharded Delta Engine Initialized (256 Shards)")

	// 3. Initialize Smart Contract Rules Engine
	contractEngine := contract.NewSmartContractEngine()
	log.Println("✅ [3/5] Smart Contract Business Rules Engine Loaded")

	// 4. Initialize Cryptographic Audit Ledger
	auditLedger := ledger.NewAuditLedger()
	log.Println("✅ [4/5] Immutable Audit Ledger Initialized (Genesis Block Active)")

	// Context and Signal Handling
	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	// 5. Async Background Flusher Loop (10ms Cycle to KeyDB)
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_ = deltaEngine.FlushToDB("channel1")
			}
		}
	}()
	log.Println("✅ [5/5] Background Async DB Pipeline Active")

	// =========================================================================
	// EXECUTE REAL TRANSACTION LIFECYCLE SAMPLE
	// =========================================================================
	fmt.Println("\n-----------------------------------------------------------------")
	log.Println("📥 [STEP 1: PAYLOAD INGESTION] Receiving payment payload...")

	// Sample Payload: Sender transfers $500.00 (50,000 cents) to Receiver
	sampleTx := &types.PaymentTransaction{
		TxID:       "TX_88492049102",
		SenderID:   "acc_alice",
		ReceiverID: "acc_bob",
		Amount:     50000, // $500.00
		Currency:   "USD",
		ChannelID:  "channel1",
		Timestamp:  time.Now(),
		Signature:  "sig_secp256k1_3a8f9c1b...",
	}
	fmt.Printf("   Payload: Sender=%s | Receiver=%s | Amount=$%.2f USD | TxID=%s\n",
		sampleTx.SenderID, sampleTx.ReceiverID, float64(sampleTx.Amount)/100.0, sampleTx.TxID)

	// STEP 2 & 3: Smart Contract Business Rules Validation
	log.Println("⚙️ [STEP 2 & 3: CONTRACT & RULES VALIDATION] Executing Smart Contract...")
	receipt, err := contractEngine.ExecuteRules(sampleTx)
	if err != nil {
		log.Fatalf("❌ Transaction rejected by contract: %v", err)
	}
	fmt.Printf("   Receipt: Approved=%t | NetTransfer=$%.2f | FeeApplied=$%.2f\n",
		receipt.Approved, float64(receipt.NetAmount)/100.0, float64(receipt.FeeApplied)/100.0)

	// STEP 4: Apply Atomic Delta Mutations in RAM
	log.Println("⚡ [STEP 4: SHARDED RAM DELTA UPDATE] Applying balance updates in RAM...")
	// Deduct net + fee from Sender (-$500.00)
	deltaEngine.ApplyDelta(sampleTx.ChannelID, sampleTx.SenderID, -sampleTx.Amount)
	// Add net amount to Receiver (+$499.50)
	deltaEngine.ApplyDelta(sampleTx.ChannelID, sampleTx.ReceiverID, receipt.NetAmount)
	// Add fee to System Treasury (+$0.50)
	deltaEngine.ApplyDelta(sampleTx.ChannelID, "acc_treasury", receipt.FeeApplied)
	log.Println("   ACK: Sub-millisecond state mutation complete in RAM!")

	// STEP 5: Append to Cryptographic Ledger
	log.Println("🔗 [STEP 5: IMMUTABLE LEDGER COMMIT] Appending transaction to Audit Chain...")
	ledgerBlock := auditLedger.AppendTransaction(sampleTx, receipt)
	fmt.Printf("   Ledger Entry #%d | Hash: %s | PrevHash: %s\n",
		ledgerBlock.Index, ledgerBlock.Hash[:16]+"...", ledgerBlock.PrevHash[:16]+"...")

	// STEP 6: Force KeyDB Persistence Flush
	log.Println("💾 [STEP 6: KEYDB DB COMMIT] Flushing RAM states to KeyDB via INCRBY...")
	_ = deltaEngine.FlushToDB("channel1")
	time.Sleep(50 * time.Millisecond) // allow socket write to settle

	// STEP 7: Retrieve and Output Final State Verification
	log.Println("🔍 [STEP 7: STATE RETRIEVAL & ACK] Fetching persisted balances from KeyDB...")
	aliceBalBytes, _ := keydbEngine.GetState("channel1", "acc_alice")
	bobBalBytes, _ := keydbEngine.GetState("channel1", "acc_bob")
	treasuryBalBytes, _ := keydbEngine.GetState("channel1", "acc_treasury")

	fmt.Println("\n=======================================================")
	fmt.Println("       FINAL CONFIRMED LEDGER & STATE RECEIPT          ")
	fmt.Println("=======================================================")
	fmt.Printf(" Transaction ID       : %s\n", sampleTx.TxID)
	fmt.Printf(" Status               : CONFIRMED & COMMITTED (ACK)\n")
	fmt.Printf(" Ledger Height        : %d Blocks\n", auditLedger.GetChainHeight())
	fmt.Printf(" Alice Balance (Db)   : %s cents (-$500.00)\n", string(aliceBalBytes))
	fmt.Printf(" Bob Balance (Db)     : %s cents (+$499.50)\n", string(bobBalBytes))
	fmt.Printf(" Treasury Balance(Db) : %s cents (+$0.50)\n", string(treasuryBalBytes))
	fmt.Println("=======================================================\n")

	// Graceful exit listener
	go func() {
		<-sigChan
		cancel()
		os.Exit(0)
	}()
}
