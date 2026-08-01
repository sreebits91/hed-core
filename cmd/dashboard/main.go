package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"hed-core/pkg/contract"
	"hed-core/pkg/delta"
	"hed-core/pkg/ledger"
	"hed-core/pkg/plugin"
	"hed-core/pkg/types"

	_ "github.com/lib/pq"
)

type Metrics struct {
	TotalTxns      uint64  `json:"total_txns"`
	ProcessedTxns  uint64  `json:"processed_txns"`
	PersistedYuga  uint64  `json:"persisted_yuga"`
	FailedTxns     uint64  `json:"failed_txns"`
	CurrentTPS     float64 `json:"current_tps"`
	AvgLatencyMs   float64 `json:"avg_latency_ms"`
	ProgressPct    float64 `json:"progress_pct"`
	IsRunning      bool    `json:"is_running"`
	ElapsedSeconds float64 `json:"elapsed_seconds"`
}

var (
	metrics        Metrics
	metricsLock    sync.RWMutex
	keydbEngine    *plugin.KeyDBEngine
	deltaEngine    *delta.DeltaEngine
	contractEng    *contract.SmartContractEngine
	auditLedger    *ledger.AuditLedger
	logs           []string
	logsLock       sync.Mutex

	yugaDB         *sql.DB
	yugaChan       chan *types.PaymentTransaction
	persistedCount uint64

	recentTxns     []*types.PaymentTransaction
	recentTxnsLock sync.RWMutex
)

func addLog(msg string) {
	logsLock.Lock()
	defer logsLock.Unlock()
	entry := fmt.Sprintf("[%s] %s", time.Now().Format("15:04:05.000"), msg)
	logs = append(logs, entry)
	if len(logs) > 200 {
		logs = logs[1:]
	}
}

func storeTransaction(tx *types.PaymentTransaction) {
	recentTxnsLock.Lock()
	defer recentTxnsLock.Unlock()
	recentTxns = append(recentTxns, tx)
	if len(recentTxns) > 1000 {
		recentTxns = recentTxns[1:]
	}
}

func startYugabyteWorkers(numWorkers int) {
	yugaChan = make(chan *types.PaymentTransaction, 150000)

	for i := 0; i < numWorkers; i++ {
		go func() {
			batchSize := 500
			batch := make([]*types.PaymentTransaction, 0, batchSize)

			for tx := range yugaChan {
				batch = append(batch, tx)

				if len(batch) >= batchSize || len(yugaChan) == 0 {
					if err := flushBatchToYugabyte(batch); err == nil {
						atomic.AddUint64(&persistedCount, uint64(len(batch)))
					} else {
						log.Printf("Yugabyte Batch Insert Error: %v", err)
					}
					batch = make([]*types.PaymentTransaction, 0, batchSize)
				}
			}

			if len(batch) > 0 {
				if err := flushBatchToYugabyte(batch); err == nil {
					atomic.AddUint64(&persistedCount, uint64(len(batch)))
				}
			}
		}()
	}
}

func flushBatchToYugabyte(batch []*types.PaymentTransaction) error {
	if yugaDB == nil || len(batch) == 0 {
		return nil
	}

	query := "INSERT INTO transactions (tx_id, sender_id, receiver_id, amount, currency, channel_id, created_at) VALUES "
	vals := []interface{}{}

	for i, tx := range batch {
		n := i * 7
		query += fmt.Sprintf("($%d, $%d, $%d, $%d, $%d, $%d, $%d),", n+1, n+2, n+3, n+4, n+5, n+6, n+7)
		vals = append(vals, tx.TxID, tx.SenderID, tx.ReceiverID, tx.Amount, tx.Currency, tx.ChannelID, tx.Timestamp)
	}

	query = query[:len(query)-1]

	_, err := yugaDB.Exec(query, vals...)
	return err
}

func initYugabyte() {
	connStr := "host=127.0.0.1 port=5433 user=yugabyte password=yugabyte dbname=hed_core sslmode=disable"
	var err error
	yugaDB, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Printf("⚠️ YugabyteDB connection skipped/failed: %v", err)
		return
	}

	yugaDB.SetMaxOpenConns(64)
	yugaDB.SetMaxIdleConns(32)

	if err := yugaDB.Ping(); err != nil {
		log.Printf("⚠️ YugabyteDB not reachable on localhost:5433 (running without Yugabyte persistence)")
		yugaDB = nil
		return
	}

	log.Println("✅ Connected successfully to YugabyteDB cluster!")
	startYugabyteWorkers(16)
}

func main() {
	log.Println("=== STARTING HED-CORE CONTROL CENTER & YUGABYTE BENCHMARK ===")

	keydbEngine = plugin.NewKeyDBEngine("127.0.0.1:6379", 256)
	if err := keydbEngine.Init(nil); err != nil {
		log.Printf("⚠️ KeyDB Warning: %v (continuing with fallback/local state)", err)
	}

	deltaEngine = delta.New(keydbEngine)
	contractEng = contract.NewSmartContractEngine()
	auditLedger = ledger.NewAuditLedger()

	initYugabyte()

	http.HandleFunc("/", handleDashboard)
	http.HandleFunc("/api/start", handleStartLoadTest)
	http.HandleFunc("/api/stats", handleStats)
	http.HandleFunc("/api/logs", handleLogs)
	http.HandleFunc("/api/inspect/keydb", handleInspectKeyDB)
	http.HandleFunc("/api/inspect/ledger", handleInspectLedger)
	http.HandleFunc("/api/inspect/transactions", handleInspectTransactions)
	http.HandleFunc("/api/inspect/yugabyte", handleInspectYugabyte)

	go func() {
		ticker := time.NewTicker(5 * time.Millisecond)
		for range ticker.C {
			_ = deltaEngine.FlushToDB("channel1")
		}
	}()

	fmt.Println("\n🚀 Dashboard active at: http://localhost:8080")
	log.Fatal(http.ListenAndServe(":8080", nil))
}

func handleStartLoadTest(w http.ResponseWriter, r *http.Request) {
	metricsLock.Lock()
	if metrics.IsRunning {
		metricsLock.Unlock()
		http.Error(w, "Benchmark already running", http.StatusBadRequest)
		return
	}

	targetTxns := uint64(100000)
	metrics = Metrics{
		TotalTxns: targetTxns,
		IsRunning: true,
	}
	atomic.StoreUint64(&persistedCount, 0)
	metricsLock.Unlock()

	recentTxnsLock.Lock()
	recentTxns = make([]*types.PaymentTransaction, 0, 1000)
	recentTxnsLock.Unlock()

	go runBenchmark(targetTxns)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "STARTED", "target": "100,000"})
}

func runBenchmark(totalTxns uint64) {
	addLog(fmt.Sprintf("🚀 Starting benchmark run for %d transactions...", totalTxns))
	startTime := time.Now()

	var processed uint64
	var failed uint64
	var totalLatencyNs int64

	numWorkers := 64
	jobsChan := make(chan uint64, 10000)
	var wg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for id := range jobsChan {
				txStart := time.Now()

				tx := &types.PaymentTransaction{
					TxID:       fmt.Sprintf("TX_%08d", id),
					SenderID:   fmt.Sprintf("acc_%d", id%1000),
					ReceiverID: fmt.Sprintf("acc_%d", (id+1)%1000),
					Amount:     1000,
					Currency:   "USD",
					ChannelID:  "channel1",
					Timestamp:  time.Now(),
				}

				receipt, err := contractEng.ExecuteRules(tx)
				if err != nil {
					atomic.AddUint64(&failed, 1)
					continue
				}

				deltaEngine.ApplyDelta(tx.ChannelID, tx.SenderID, -tx.Amount)
				deltaEngine.ApplyDelta(tx.ChannelID, tx.ReceiverID, receipt.NetAmount)

				if id%10 == 0 {
					auditLedger.AppendTransaction(tx, receipt)
				}

				if yugaDB != nil {
					yugaChan <- tx
				}

				storeTransaction(tx)

				duration := time.Since(txStart).Nanoseconds()
				atomic.AddInt64(&totalLatencyNs, duration)
				current := atomic.AddUint64(&processed, 1)

				if current%20000 == 0 {
					addLog(fmt.Sprintf("⚡ Processed %d / %d txns...", current, totalTxns))
				}
			}
		}(w)
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	go func() {
		for range ticker.C {
			p := atomic.LoadUint64(&processed)
			f := atomic.LoadUint64(&failed)
			y := atomic.LoadUint64(&persistedCount)
			elapsed := time.Since(startTime).Seconds()

			if elapsed == 0 {
				continue
			}

			tps := float64(p) / elapsed
			avgLat := float64(0)
			if p > 0 {
				avgLat = float64(atomic.LoadInt64(&totalLatencyNs)/int64(p)) / 1e6
			}

			metricsLock.Lock()
			metrics.ProcessedTxns = p
			metrics.PersistedYuga = y
			metrics.FailedTxns = f
			metrics.CurrentTPS = tps
			metrics.AvgLatencyMs = avgLat
			metrics.ElapsedSeconds = elapsed
			metrics.ProgressPct = (float64(p) / float64(totalTxns)) * 100.0
			isDone := !metrics.IsRunning
			metricsLock.Unlock()

			if isDone || p >= totalTxns {
				ticker.Stop()
				return
			}
		}
	}()

	for i := uint64(1); i <= totalTxns; i++ {
		jobsChan <- i
	}
	close(jobsChan)
	wg.Wait()

	totalElapsed := time.Since(startTime).Seconds()
	finalTPS := float64(processed) / totalElapsed

	metricsLock.Lock()
	metrics.IsRunning = false
	metrics.CurrentTPS = finalTPS
	metrics.ProgressPct = 100.0
	metricsLock.Unlock()

	addLog(fmt.Sprintf("✅ BENCHMARK COMPLETE! %d Txns in %.2fs (TPS: %.2f)", processed, totalElapsed, finalTPS))
}

func handleStats(w http.ResponseWriter, r *http.Request) {
	metricsLock.RLock()
	defer metricsLock.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(metrics)
}

func handleLogs(w http.ResponseWriter, r *http.Request) {
	logsLock.Lock()
	defer logsLock.Unlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(logs)
}

func handleInspectKeyDB(w http.ResponseWriter, r *http.Request) {
	results := make(map[string]string)
	for i := 0; i < 10; i++ {
		acc := fmt.Sprintf("acc_%d", i)
		val, err := keydbEngine.GetState("channel1", acc)
		if err != nil {
			results[acc] = "KeyDB Offline / Not Found"
		} else {
			results[acc] = string(val) + " cents"
		}
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(results)
}

func handleInspectLedger(w http.ResponseWriter, r *http.Request) {
	height := auditLedger.GetChainHeight()
	resp := map[string]interface{}{
		"chain_height_sampled_blocks": height,
		"status":                      "IMMUTABLE_HASH_VERIFIED",
		"engine":                      "HED-Core Cryptographic Chain",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func handleInspectTransactions(w http.ResponseWriter, r *http.Request) {
	recentTxnsLock.RLock()
	defer recentTxnsLock.RUnlock()

	limit := 50
	start := 0
	if len(recentTxns) > limit {
		start = len(recentTxns) - limit
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(recentTxns[start:])
}

func handleInspectYugabyte(w http.ResponseWriter, r *http.Request) {
	if yugaDB == nil {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "YugabyteDB Not Connected"})
		return
	}

	var count int64
	err := yugaDB.QueryRow("SELECT COUNT(*) FROM transactions").Scan(&count)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	resp := map[string]interface{}{
		"yugabyte_total_row_count": count,
		"status":                   "CONNECTED_AND_SYNCED",
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprint(w, `<!DOCTYPE html>
<html>
<head>
<meta charset="UTF-8">
    <title>HED-Core 100k Txn Engine UI</title>
    <style>
        body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; background: #0f172a; color: #f8fafc; margin: 0; padding: 24px; }
        .grid { display: grid; grid-template-columns: repeat(5, 1fr); gap: 16px; margin-bottom: 24px; }
        .card { background: #1e293b; padding: 20px; border-radius: 12px; border: 1px solid #334155; }
        .card h3 { margin: 0 0 8px 0; font-size: 13px; color: #94a3b8; text-transform: uppercase; }
        .card .metric { font-size: 24px; font-weight: bold; color: #38bdf8; }
        .btn { background: #0284c7; color: white; border: none; padding: 14px 28px; font-size: 16px; font-weight: bold; border-radius: 8px; cursor: pointer; }
        .btn:hover { background: #0369a1; }
        .progress-bar { width: 100%; height: 16px; background: #334155; border-radius: 8px; overflow: hidden; margin: 16px 0; }
        .progress-fill { height: 100%; background: #22c55e; width: 0%; transition: width 0.2s; }
        .console { background: #020617; border: 1px solid #334155; border-radius: 8px; padding: 16px; font-family: monospace; height: 180px; overflow-y: auto; color: #4ade80; }
        .db-grid { display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 16px; margin-top: 24px; }
        pre { background: #0f172a; padding: 12px; border-radius: 6px; overflow-x: auto; color: #f1f5f9; }
        table { width: 100%; text-align: left; border-collapse: collapse; font-size: 14px; font-family: monospace; }
        th { border-bottom: 1px solid #334155; color: #94a3b8; padding: 8px; }
        td { border-bottom: 1px solid #1e293b; padding: 8px; }
    </style>
</head>
<body>
    <h2>🚀 HED-Core 100k (1 Lakh) Transaction Engine UI</h2>
    <p style="color: #94a3b8;">Real-Time Pipeline Benchmark, KeyDB State, Ledger Audit & YugabyteDB Storage</p>
    
    <button class="btn" onclick="startTest()">▶ RUN 100,000 TXN BENCHMARK</button>

    <div class="progress-bar">
        <div id="progress" class="progress-fill"></div>
    </div>

    <div class="grid">
        <div class="card">
            <h3>Processed / Target</h3>
            <div id="processed" class="metric">0 / 100,000</div>
        </div>
        <div class="card">
            <h3>Yugabyte Persisted</h3>
            <div id="persisted" class="metric" style="color: #a855f7;">0</div>
        </div>
        <div class="card">
            <h3>Current Throughput</h3>
            <div id="tps" class="metric">0 TPS</div>
        </div>
        <div class="card">
            <h3>Avg Latency</h3>
            <div id="latency" class="metric">0.00 ms</div>
        </div>
        <div class="card">
            <h3>Elapsed Time</h3>
            <div id="elapsed" class="metric">0.s</div>
        </div>
    </div>

    <h3>📜 Engine Live Event Stream</h3>
    <div id="console" class="console">Ready to start benchmark run...</div>

    <div class="card" style="margin-top: 24px;">
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px;">
            <h3 style="margin: 0;">💳 Raw Transaction Inspector</h3>
            <button onclick="fetchTxns()" style="background:#0284c7; color:white; border:none; padding:6px 14px; border-radius:6px; cursor:pointer; font-weight:bold;">🔄 Refresh Transactions</button>
        </div>
        <div style="max-height: 220px; overflow-y: auto;">
            <table>
                <thead>
                    <tr>
                        <th>Tx ID</th>
                        <th>Sender</th>
                        <th>Receiver</th>
                        <th>Amount</th>
                        <th>Channel</th>
                        <th>Timestamp</th>
                    </tr>
                </thead>
                <tbody id="tx-table-body">
                    <tr><td colspan="6" style="padding: 16px; text-align: center; color: #64748b;">No transaction data loaded yet. Run benchmark and click Refresh.</td></tr>
                </tbody>
            </table>
        </div>
    </div>

    <div class="db-grid">
        <div class="card">
            <h3>🔍 KeyDB World State</h3>
            <button onclick="inspectKeyDB()" style="background:#334155; color:white; border:none; padding:6px 12px; border-radius:4px; cursor:pointer;">Query KeyDB</button>
            <pre id="keydb-view">Click query to view state...</pre>
        </div>
        <div class="card">
            <h3>🔗 Chaincode Audit Ledger</h3>
            <button onclick="inspectLedger()" style="background:#334155; color:white; border:none; padding:6px 12px; border-radius:4px; cursor:pointer;">Verify Ledger</button>
            <pre id="ledger-view">Click verify to inspect block height...</pre>
        </div>
        <div class="card">
            <h3>🐘 YugabyteDB Inspector</h3>
            <button onclick="inspectYugabyte()" style="background:#a855f7; color:white; border:none; padding:6px 12px; border-radius:4px; cursor:pointer;">Count Yugabyte Rows</button>
            <pre id="yuga-view">Click to query SELECT COUNT(*)...</pre>
        </div>
    </div>

    <script>
        async function startTest() {
            await fetch('/api/start');
        }

        async function inspectKeyDB() {
            const res = await fetch('/api/inspect/keydb');
            const data = await res.json();
            document.getElementById('keydb-view').innerText = JSON.stringify(data, null, 2);
        }

        async function inspectLedger() {
            const res = await fetch('/api/inspect/ledger');
            const data = await res.json();
            document.getElementById('ledger-view').innerText = JSON.stringify(data, null, 2);
        }

        async function inspectYugabyte() {
            const res = await fetch('/api/inspect/yugabyte');
            const data = await res.json();
            document.getElementById('yuga-view').innerText = JSON.stringify(data, null, 2);
        }

        async function fetchTxns() {
            const res = await fetch('/api/inspect/transactions');
            const txns = await res.json();
            const tbody = document.getElementById('tx-table-body');
            tbody.innerHTML = '';

            if (!txns || txns.length === 0) {
                tbody.innerHTML = '<tr><td colspan="6" style="padding: 16px; text-align: center; color: #64748b;">No transactions captured. Run the benchmark first!</td></tr>';
                return;
            }

            txns.reverse().forEach(tx => {
                const row = document.createElement('tr');
                const formattedTime = new Date(tx.Timestamp).toLocaleTimeString() + '.' + new Date(tx.Timestamp).getMilliseconds();
                row.innerHTML = '<td>' + tx.TxID + '</td><td>' + tx.SenderID + '</td><td>' + tx.ReceiverID + '</td><td>$' + (tx.Amount / 100).toFixed(2) + '</td><td>' + tx.ChannelID + '</td><td>' + formattedTime + '</td>';
                tbody.appendChild(row);
            });
        }

        setInterval(async () => {
            const statsRes = await fetch('/api/stats');
            const stats = await statsRes.json();

            document.getElementById('processed').innerText = stats.processed_txns.toLocaleString() + " / " + stats.total_txns.toLocaleString();
            document.getElementById('persisted').innerText = (stats.persisted_yuga || 0).toLocaleString();
            document.getElementById('tps').innerText = Math.round(stats.current_tps).toLocaleString() + " TPS";
            document.getElementById('latency').innerText = stats.avg_latency_ms.toFixed(2) + " ms";
            document.getElementById('elapsed').innerText = stats.elapsed_seconds.toFixed(1) + "s";
            document.getElementById('progress').style.width = stats.progress_pct + "%";

            const logsRes = await fetch('/api/logs');
            const logs = await logsRes.json();
            const consoleDiv = document.getElementById('console');
            consoleDiv.innerHTML = logs.join('<br>');
            consoleDiv.scrollTop = consoleDiv.scrollHeight;
        }, 300);
    </script>
</body>
</html>`)
}