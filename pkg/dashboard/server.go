package dashboard

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/jung-kurt/gofpdf"

	"hed-core/pkg/engine"
	"hed-core/pkg/hlf"
	"hed-core/pkg/plugin"
)

type Server struct {
	pipeline    *engine.Pipeline
	registry    *plugin.Registry
	hlfServer   *HLFServer
	committer   *hlf.HLFCommitter
	isTesting   int32
	cancelTest  context.CancelFunc
	netErrCount uint64
	startTime   time.Time
}

func NewServer(reg *plugin.Registry, db plugin.StateEngine, hlfSrv *HLFServer, committer *hlf.HLFCommitter) *Server {
	pipe := engine.NewPipeline(db, 32)

	return &Server{
		pipeline:  pipe,
		registry:  reg,
		hlfServer: hlfSrv,
		committer: committer,
		startTime: time.Now(),
	}
}

func (s *Server) Start(addr string) error {
	http.HandleFunc("/api/v1/tx", s.handleTxSubmission)
	http.HandleFunc("/api/metrics", s.handleMetrics)
	http.HandleFunc("/api/config", s.handleConfig)
	http.HandleFunc("/api/benchmark", s.handleBenchmark)
	http.HandleFunc("/api/logs", s.handleLogsSSE)
	http.HandleFunc("/api/report/pdf", s.handlePDFReport)
	http.HandleFunc("/", s.handleDashboard)

	return http.ListenAndServe(addr, nil)
}

func (s *Server) handleTxSubmission(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req engine.TxPayload
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	shardName, ackLatencyUs, err := s.pipeline.SubmitTransaction(&req)
	if err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]interface{}{
			"status":  "FAILED",
			"tx_uuid": req.TxUUID,
			"error":   err.Error(),
		})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "ACKNOWLEDGED",
		"tx_uuid":     req.TxUUID,
		"shard":       shardName,
		"ack_time_us": ackLatencyUs,
	})
}

func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	var total uint64
	if s.committer != nil {
		total += s.committer.TotalCommitted()
	}
	total += s.pipeline.TotalCommitted()

	totalFailed := s.pipeline.TotalFailed() + atomic.LoadUint64(&s.netErrCount)

	json.NewEncoder(w).Encode(map[string]interface{}{
		"committed_txs": total,
		"failed_txs":    totalFailed,
		"engine":        s.pipeline.EngineName(),
		"is_testing":    atomic.LoadInt32(&s.isTesting) == 1,
	})
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var cfg struct {
		Engine   string `json:"engine"`
		Channels int    `json:"channels"`
	}

	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if cfg.Channels > 0 {
		s.pipeline.SetShards(cfg.Channels)
	}

	if cfg.Engine != "" {
		dbEngine, err := s.registry.Get(cfg.Engine)
		if err == nil {
			s.pipeline.SetStorageEngine(dbEngine)
		}
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "updated"})
}

func (s *Server) handleBenchmark(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Action   string `json:"action"`
		Workers  int    `json:"workers"`
		Channels int    `json:"channels"`
		DelayUs  int    `json:"delay_us"`
		Limit    int64  `json:"limit"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	if req.Action == "stop" {
		if s.cancelTest != nil {
			s.cancelTest()
			s.cancelTest = nil
		}
		atomic.StoreInt32(&s.isTesting, 0)
		s.pipeline.EmitEvent(engine.EventSys, "", "", "Benchmark generator stopped manually.", 0)
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "stopped"})
		return
	}

	if atomic.LoadInt32(&s.isTesting) == 1 {
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "already_running"})
		return
	}

	workers := req.Workers
	if workers <= 0 {
		workers = 128
	}
	delay := req.DelayUs
	limit := req.Limit

	ctx, cancel := context.WithCancel(context.Background())
	s.cancelTest = cancel
	atomic.StoreInt32(&s.isTesting, 1)

	limitText := "Continuous"
	if limit > 0 {
		limitText = fmt.Sprintf("%d transactions", limit)
	}
	s.pipeline.EmitEvent(engine.EventSys, "", "", fmt.Sprintf("🚀 Launching Direct In-Memory Engine Benchmark [%d Workers | Target Limit: %s]", workers, limitText), 0)

	var sentCounter int64

	// Direct High-Throughput Worker Goroutines (Bypasses HTTP overhead to hit 100k+ TPS)
	for i := 0; i < workers; i++ {
		go func(workerID int) {
			for {
				select {
				case <-ctx.Done():
					return
				default:
					if limit > 0 {
						current := atomic.AddInt64(&sentCounter, 1)
						if current > limit {
							if atomic.CompareAndSwapInt32(&s.isTesting, 1, 0) {
								s.pipeline.EmitEvent(engine.EventSys, "", "", fmt.Sprintf("🎯 Target limit reached (%d txs). Benchmark complete.", limit), 0)
								if s.cancelTest != nil {
									s.cancelTest()
								}
							}
							return
						}
					}

					payload := engine.TxPayload{
						TxUUID:    engine.GenerateUUID(),
						AccountID: fmt.Sprintf("acc_%d", workerID%1000),
						Amount:    100,
					}

					_, _, err := s.pipeline.SubmitTransaction(&payload)
					if err != nil {
						atomic.AddUint64(&s.netErrCount, 1)
					}

					if delay > 0 {
						time.Sleep(time.Duration(delay) * time.Microsecond)
					}
				}
			}
		}(i)
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "started"})
}

func (s *Server) handleLogsSSE(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	subChan := s.pipeline.SubscribeEvents()
	defer s.pipeline.UnsubscribeEvents(subChan)

	flusher, ok := w.(http.Flusher)
	if !ok {
		return
	}

	for {
		select {
		case <-r.Context().Done():
			return
		case evt := <-subChan:
			msg := map[string]interface{}{
				"timestamp": evt.Timestamp,
				"level":     string(evt.Type),
				"tx_uuid":   evt.TxUUID,
				"shard":     evt.Shard,
				"message":   evt.Message,
			}
			data, _ := json.Marshal(msg)
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

func (s *Server) handlePDFReport(w http.ResponseWriter, r *http.Request) {
	var totalCommitted uint64
	if s.committer != nil {
		totalCommitted += s.committer.TotalCommitted()
	}
	totalCommitted += s.pipeline.TotalCommitted()

	totalFailed := s.pipeline.TotalFailed() + atomic.LoadUint64(&s.netErrCount)
	durationSec := time.Since(s.startTime).Seconds()
	avgTPS := 0.0
	if durationSec > 0 {
		avgTPS = float64(totalCommitted) / durationSec
	}

	pdf := gofpdf.New("P", "mm", "A4", "")
	pdf.AddPage()

	pdf.SetFont("Arial", "B", 20)
	pdf.Cell(190, 10, "HED-Core Performance & TPS Report")
	pdf.Ln(12)

	pdf.SetFont("Arial", "I", 10)
	pdf.Cell(190, 6, fmt.Sprintf("Generated on: %s", time.Now().Format("2006-01-02 15:04:05 MST")))
	pdf.Ln(10)

	pdf.SetFont("Arial", "B", 14)
	pdf.Cell(190, 8, "1. Active Configuration")
	pdf.Ln(8)

	pdf.SetFont("Arial", "", 11)
	pdf.Cell(60, 6, "Storage Engine:")
	pdf.Cell(130, 6, s.pipeline.EngineName())
	pdf.Ln(6)
	pdf.Cell(60, 6, "Uptime Duration:")
	pdf.Cell(130, 6, fmt.Sprintf("%.2f seconds", durationSec))
	pdf.Ln(10)

	pdf.SetFont("Arial", "B", 14)
	pdf.Cell(190, 8, "2. Execution Summary")
	pdf.Ln(8)

	pdf.SetFont("Arial", "B", 11)
	pdf.SetFillColor(230, 230, 230)
	pdf.CellFormat(95, 7, "Metric", "1", 0, "L", true, 0, "")
	pdf.CellFormat(95, 7, "Value", "1", 1, "L", true, 0, "")

	pdf.SetFont("Arial", "", 11)
	metrics := [][]string{
		{"Committed Transactions", fmt.Sprintf("%d", totalCommitted)},
		{"Failed / Error Transactions", fmt.Sprintf("%d", totalFailed)},
		{"Average Throughput (TPS)", fmt.Sprintf("%.2f ops/sec", avgTPS)},
	}

	for _, m := range metrics {
		pdf.CellFormat(95, 7, m[0], "1", 0, "L", false, 0, "")
		pdf.CellFormat(95, 7, m[1], "1", 1, "L", false, 0, "")
	}

	pdf.Ln(12)

	pdf.SetFont("Arial", "B", 12)
	if totalFailed == 0 {
		pdf.SetTextColor(0, 128, 0)
		pdf.Cell(190, 8, "STATUS: Operational - Zero Errors Recorded.")
	} else {
		pdf.SetTextColor(200, 0, 0)
		pdf.Cell(190, 8, fmt.Sprintf("STATUS: Completed with %d errors.", totalFailed))
	}

	w.Header().Set("Content-Type", "application/pdf")
	w.Header().Set("Content-Disposition", "attachment; filename=\"hed_core_tps_report.pdf\"")

	if err := pdf.Output(w); err != nil {
		http.Error(w, fmt.Sprintf("Failed to generate PDF: %v", err), http.StatusInternalServerError)
	}
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	html := `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HED Core Dashboard</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <style>
        body { background-color: #0b0f19; color: #f8fafc; font-family: 'Segoe UI', system-ui, -apple-system, sans-serif; }
        .card { background-color: #161e2e; border: 1px solid #2d3748; color: #f8fafc; border-radius: 12px; }
        .badge-active { background-color: #10b981; color: #064e3b; font-weight: 600; }
        .badge-inactive { background-color: #ef4444; color: #7f1d1d; font-weight: 600; }
        .metric-value { font-size: 2.2rem; font-weight: 700; color: #38bdf8; }
        .metric-error { font-size: 2.2rem; font-weight: 700; color: #f87171; }
        .btn-success { background-color: #10b981; border: none; }
        .btn-danger { background-color: #ef4444; border: none; }
        .btn-report { background-color: #3b82f6; border: none; color: #ffffff; font-weight: 600; }
        .btn-report:hover { background-color: #2563eb; color: #ffffff; }
        .terminal-box { background-color: #050811; border: 1px solid #1e293b; color: #10b981; font-family: monospace; font-size: 0.85rem; height: 360px; overflow-y: auto; padding: 12px; border-radius: 8px; }
        .log-ack { color: #f59e0b; font-weight: bold; }
        .log-commit { color: #34d399; font-weight: bold; }
        .log-sys { color: #94a3b8; }
        .log-err { color: #f87171; font-weight: bold; }
    </style>
</head>
<body class="p-4">
    <div class="container-fluid max-w-7xl">
        <div class="d-flex justify-content-between align-items-center mb-4">
            <div>
                <h1 class="h2 mb-1">⚡ HyperEngine-Drunix (HED) Core</h1>
                <p class="text-secondary mb-0">Production Sharded Distributed State Engine</p>
            </div>
            <div class="d-flex align-items-center gap-2">
                <a href="/api/report/pdf" class="btn btn-report px-3 py-2 rounded-pill d-flex align-items-center gap-1" target="_blank">
                    📄 Download PDF Report
                </a>
                <span class="badge badge-active px-3 py-2 rounded-pill">Engine: <span id="activeEngine">KeyDB</span></span>
                <span id="testBadge" class="badge badge-inactive px-3 py-2 rounded-pill">Benchmark: STOPPED</span>
            </div>
        </div>

        <div class="row g-4 mb-4">
            <div class="col-md-3">
                <div class="card p-3">
                    <div class="text-secondary mb-1">Committed Transactions</div>
                    <div class="metric-value" id="txCount">0</div>
                </div>
            </div>
            <div class="col-md-3">
                <div class="card p-3">
                    <div class="text-secondary mb-1">Failed / Errors</div>
                    <div class="metric-error" id="errCount">0</div>
                </div>
            </div>
            <div class="col-md-3">
                <div class="card p-3">
                    <div class="text-secondary mb-1">Throughput</div>
                    <div class="metric-value" id="tpsRate">0 <small class="fs-6 text-secondary">TPS</small></div>
                </div>
            </div>
            <div class="col-md-3">
                <div class="card p-3">
                    <div class="text-secondary mb-1">Shards</div>
                    <div class="metric-value"><span id="channelVal">32</span> <small class="fs-6 text-secondary">Channels</small></div>
                </div>
            </div>
        </div>

        <div class="row g-4 mb-4">
            <div class="col-md-5">
                <div class="card p-4 h-100">
                    <h3 class="h5 mb-3">🛠️ Topology & High-TPS Benchmark</h3>
                    
                    <div class="mb-3">
                        <label class="form-label text-secondary d-flex justify-content-between">
                            <span>Max Test Limit</span>
                            <span class="fw-bold text-warning" id="limitDisplay">100,000 txs</span>
                        </label>
                        <input type="range" class="form-range" id="limitSlider" min="0" max="1000000" step="50000" value="100000" oninput="updateLimitLabel(this.value)">
                    </div>

                    <div class="mb-3">
                        <label class="form-label text-secondary d-flex justify-content-between">
                            <span>Parallel Workers</span>
                            <span class="fw-bold text-info" id="workerVal">256</span>
                        </label>
                        <input type="range" class="form-range" id="workerSlider" min="16" max="512" step="16" value="256" oninput="document.getElementById('workerVal').innerText = this.value">
                    </div>

                    <div class="mb-3">
                        <label class="form-label text-secondary d-flex justify-content-between">
                            <span>Sharded Channels</span>
                            <span class="fw-bold text-info" id="channelSliderVal">64</span>
                        </label>
                        <input type="range" class="form-range" id="channelSlider" min="8" max="128" step="8" value="64" oninput="document.getElementById('channelSliderVal').innerText = this.value; document.getElementById('channelVal').innerText = this.value">
                    </div>

                    <div class="mb-3">
                        <label class="form-label text-secondary d-flex justify-content-between">
                            <span>Delay (µs)</span>
                            <span class="fw-bold text-info" id="delayVal">0 (Max Speed)</span>
                        </label>
                        <input type="range" class="form-range" id="delaySlider" min="0" max="500" value="0" step="10" oninput="document.getElementById('delayVal').innerText = this.value == 0 ? '0 (Max Speed)' : this.value">
                    </div>

                    <div class="mb-4">
                        <label class="form-label text-secondary">Active Storage Engine Plugin</label>
                        <select id="engineSelect" class="form-select bg-dark text-light border-secondary">
                            <option value="KeyDB">KeyDB In-Memory RAM Store (Tier-1)</option>
                            <option value="YugabyteDB (Distributed SQL)">YugabyteDB Distributed SQL (Tier-2 Async)</option>
                        </select>
                    </div>

                    <div class="d-flex gap-2">
                        <button onclick="toggleBenchmark('start')" class="btn btn-success flex-grow-1 py-2">Start 100k Benchmark</button>
                        <button onclick="toggleBenchmark('stop')" class="btn btn-danger flex-grow-1 py-2">Halt</button>
                    </div>
                </div>
            </div>

            <div class="col-md-7">
                <div class="card p-4 h-100">
                    <div class="d-flex justify-content-between align-items-center mb-2">
                        <h3 class="h5 mb-0">📺 Core Event Stream Terminal</h3>
                        <button onclick="clearLogs()" class="btn btn-sm btn-outline-secondary">Clear</button>
                    </div>
                    <div class="terminal-box" id="terminal">
                        <div class="log-sys">[SYSTEM] Core Streamer Connected...</div>
                    </div>
                </div>
            </div>
        </div>
    </div>

    <script>
        let lastCount = 0;
        let lastTime = Date.now();

        function updateLimitLabel(val) {
            val = parseInt(val);
            document.getElementById('limitDisplay').innerText = val === 0 ? "Continuous" : val.toLocaleString() + " txs";
        }

        async function fetchMetrics() {
            try {
                const res = await fetch('/api/metrics');
                const data = await res.json();
                
                const now = Date.now();
                const timeDiff = (now - lastTime) / 1000;
                const countDiff = data.committed_txs - lastCount;
                
                let tps = (timeDiff > 0 && lastCount > 0) ? Math.round(countDiff / timeDiff) : 0;

                document.getElementById('txCount').innerText = data.committed_txs.toLocaleString();
                document.getElementById('errCount').innerText = data.failed_txs.toLocaleString();
                document.getElementById('tpsRate').innerText = tps.toLocaleString();
                document.getElementById('activeEngine').innerText = data.engine;

                const badge = document.getElementById('testBadge');
                badge.innerText = data.is_testing ? 'Benchmark: RUNNING' : 'Benchmark: STOPPED';
                badge.className = data.is_testing ? 'badge badge-active px-3 py-2 rounded-pill' : 'badge badge-inactive px-3 py-2 rounded-pill';

                lastCount = data.committed_txs;
                lastTime = now;
            } catch (err) { console.error(err); }
        }

        async function toggleBenchmark(action) {
            const workers = parseInt(document.getElementById('workerSlider').value);
            const channels = parseInt(document.getElementById('channelSlider').value);
            const delay = parseInt(document.getElementById('delaySlider').value);
            const limit = parseInt(document.getElementById('limitSlider').value);
            const engine = document.getElementById('engineSelect').value;

            await fetch('/api/config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ engine: engine, channels: channels })
            });

            await fetch('/api/benchmark', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ action: action, workers: workers, channels: channels, delay_us: delay, limit: limit })
            });

            fetchMetrics();
        }

        function clearLogs() { document.getElementById('terminal').innerHTML = ''; }

        const evtSource = new EventSource('/api/logs');
        evtSource.onmessage = function(event) {
            const msg = JSON.parse(event.data);
            const term = document.getElementById('terminal');
            
            let classMap = { 'ACK': 'log-ack', 'COMMIT': 'log-commit', 'SYS': 'log-sys', 'ERR': 'log-err' };
            let cls = classMap[msg.level] || 'log-sys';
            let uuidPart = msg.tx_uuid ? ' [' + msg.tx_uuid + ']' : '';
            let shardPart = msg.shard ? ' [' + msg.shard + ']' : '';
            
            let logLine = document.createElement('div');
            logLine.className = cls;
            logLine.innerText = '[' + msg.timestamp + '] [' + msg.level + ']' + shardPart + uuidPart + ' ' + msg.message;
            
            term.appendChild(logLine);
            if (term.childNodes.length > 200) term.removeChild(term.firstChild);
            term.scrollTop = term.scrollHeight;
        };

        setInterval(fetchMetrics, 500);
        fetchMetrics();
    </script>
</body>
</html>`

	fmt.Fprint(w, html)
}