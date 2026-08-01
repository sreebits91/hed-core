package dashboard

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"hed-core/pkg/engine"
	"hed-core/pkg/hlf"
	"hed-core/pkg/plugin"
)

type Server struct {
	pipeline   *engine.Pipeline
	registry   *plugin.Registry
	hlfServer  *HLFServer
	committer  *hlf.HLFCommitter
	isTesting  int32
	cancelTest context.CancelFunc
}

func NewServer(reg *plugin.Registry, db plugin.StateEngine, hlfSrv *HLFServer, committer *hlf.HLFCommitter) *Server {
	pipe := engine.NewPipeline(db, 32)

	return &Server{
		pipeline:  pipe,
		registry:  reg,
		hlfServer: hlfSrv,
		committer: committer,
	}
}

func (s *Server) Start(addr string) error {
	http.HandleFunc("/api/v1/tx", s.handleTxSubmission)
	http.HandleFunc("/api/metrics", s.handleMetrics)
	http.HandleFunc("/api/config", s.handleConfig)
	http.HandleFunc("/api/benchmark", s.handleBenchmark)
	http.HandleFunc("/api/logs", s.handleLogsSSE)
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

	shardName, ackLatencyUs := s.pipeline.SubmitTransaction(&req)

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

	json.NewEncoder(w).Encode(map[string]interface{}{
		"committed_txs": total,
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
		Limit    int64  `json:"limit"` // 0 = continuous stream, >0 = stop after exact count
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
		workers = 32
	}

	delay := req.DelayUs
	if delay <= 0 {
		delay = 200
	}

	limit := req.Limit

	ctx, cancel := context.WithCancel(context.Background())
	s.cancelTest = cancel
	atomic.StoreInt32(&s.isTesting, 1)

	limitText := "Continuous"
	if limit > 0 {
		limitText = fmt.Sprintf("%d transactions", limit)
	}
	s.pipeline.EmitEvent(engine.EventSys, "", "", fmt.Sprintf("Starting HTTP POST Benchmark [%d Workers | Target Limit: %s]", workers, limitText), 0)

	tr := &http.Transport{
		MaxIdleConns:        2000,
		MaxIdleConnsPerHost: 2000,
		IdleConnTimeout:     90 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   2 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	client := &http.Client{Transport: tr, Timeout: 3 * time.Second}

	var sentCounter int64

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
							// Target reached - trigger automatic halt
							if atomic.CompareAndSwapInt32(&s.isTesting, 1, 0) {
								s.pipeline.EmitEvent(engine.EventSys, "", "", fmt.Sprintf("🎯 Target benchmark limit reached (%d txs). Halting generator...", limit), 0)
								if s.cancelTest != nil {
									s.cancelTest()
								}
							}
							return
						}
					}

					txUUID := engine.GenerateUUID()
					accountID := fmt.Sprintf("acc_%d", workerID%500)
					payload := engine.TxPayload{
						TxUUID:    txUUID,
						AccountID: accountID,
						Amount:    100,
					}
					bodyBytes, _ := json.Marshal(payload)

					resp, err := client.Post("http://127.0.0.1:8080/api/v1/tx", "application/json", bytes.NewBuffer(bodyBytes))
					if err == nil {
						resp.Body.Close()
					}
					time.Sleep(time.Duration(delay) * time.Microsecond)
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
        .btn-success { background-color: #10b981; border: none; }
        .btn-danger { background-color: #ef4444; border: none; }
        .terminal-box { background-color: #050811; border: 1px solid #1e293b; color: #10b981; font-family: monospace; font-size: 0.85rem; height: 360px; overflow-y: auto; padding: 12px; border-radius: 8px; }
        .log-route { color: #38bdf8; }
        .log-ack { color: #f59e0b; font-weight: bold; }
        .log-order { color: #c084fc; }
        .log-commit { color: #34d399; font-weight: bold; }
        .log-sys { color: #94a3b8; }
        .peer-status { font-size: 0.8rem; padding: 3px 8px; border-radius: 4px; }
    </style>
</head>
<body class="p-4">
    <div class="container-fluid max-w-7xl">
        <div class="d-flex justify-content-between align-items-center mb-4">
            <div>
                <h1 class="h2 mb-1">⚡ HyperEngine-Drunix (HED) Core</h1>
                <p class="text-secondary mb-0">Production Sharded Distributed State Engine</p>
            </div>
            <div>
                <span class="badge badge-active px-3 py-2 rounded-pill me-2">Engine: <span id="activeEngine">KeyDB</span></span>
                <span id="testBadge" class="badge badge-inactive px-3 py-2 rounded-pill">Benchmark: STOPPED</span>
            </div>
        </div>

        <div class="row g-3 mb-4">
            <div class="col-md-4">
                <div class="card p-3 d-flex flex-row align-items-center justify-content-between">
                    <div>
                        <div class="fw-bold">hed-peer-0.org1.hed.net</div>
                        <small class="text-secondary">Stateless Ingest & KeyDB Tier-1 Execution</small>
                    </div>
                    <span class="peer-status bg-success text-dark fw-bold">ONLINE</span>
                </div>
            </div>
            <div class="col-md-4">
                <div class="card p-3 d-flex flex-row align-items-center justify-content-between">
                    <div>
                        <div class="fw-bold">hed-peer-1.org1.hed.net</div>
                        <small class="text-secondary">Tier-2 YugabyteDB Async SQL Flusher</small>
                    </div>
                    <span class="peer-status bg-success text-dark fw-bold">ONLINE</span>
                </div>
            </div>
            <div class="col-md-4">
                <div class="card p-3 d-flex flex-row align-items-center justify-content-between">
                    <div>
                        <div class="fw-bold">hed-orderer-0.hed.net</div>
                        <small class="text-secondary">HED Consensus & Block Batcher Service</small>
                    </div>
                    <span class="peer-status bg-success text-dark fw-bold">ONLINE</span>
                </div>
            </div>
        </div>

        <div class="row g-4 mb-4">
            <div class="col-md-4">
                <div class="card p-3">
                    <div class="text-secondary mb-1">Total Committed Transactions</div>
                    <div class="metric-value" id="txCount">0</div>
                </div>
            </div>
            <div class="col-md-4">
                <div class="card p-3">
                    <div class="text-secondary mb-1">Live Ingestion Throughput</div>
                    <div class="metric-value" id="tpsRate">0 <small class="fs-6 text-secondary">TPS</small></div>
                </div>
            </div>
            <div class="col-md-4">
                <div class="card p-3">
                    <div class="text-secondary mb-1">Sharded Gateway Channels</div>
                    <div class="metric-value"><span id="channelVal">32</span> <small class="fs-6 text-secondary">Shards</small></div>
                </div>
            </div>
        </div>

        <div class="row g-4 mb-4">
            <div class="col-md-5">
                <div class="card p-4 h-100">
                    <h3 class="h5 mb-3">🛠️ Engine Topology & Benchmark Control</h3>
                    
                    <div class="mb-3">
                        <label class="form-label text-secondary d-flex justify-content-between">
                            <span>Max Test Limit (Transactions)</span>
                            <span class="fw-bold text-warning" id="limitDisplay">10,000 txs</span>
                        </label>
                        <input type="range" class="form-range" id="limitSlider" min="0" max="100000" step="5000" value="10000" oninput="updateLimitLabel(this.value)">
                    </div>

                    <div class="mb-3">
                        <label class="form-label text-secondary d-flex justify-content-between">
                            <span>Parallel Ingest Workers</span>
                            <span class="fw-bold text-info" id="workerVal">32</span>
                        </label>
                        <input type="range" class="form-range" id="workerSlider" min="1" max="128" value="32" oninput="document.getElementById('workerVal').innerText = this.value">
                    </div>

                    <div class="mb-3">
                        <label class="form-label text-secondary d-flex justify-content-between">
                            <span>Sharded Channels</span>
                            <span class="fw-bold text-info" id="channelSliderVal">32</span>
                        </label>
                        <input type="range" class="form-range" id="channelSlider" min="4" max="128" value="32" oninput="document.getElementById('channelSliderVal').innerText = this.value; document.getElementById('channelVal').innerText = this.value">
                    </div>

                    <div class="mb-3">
                        <label class="form-label text-secondary d-flex justify-content-between">
                            <span>Inter-Request Delay (µs)</span>
                            <span class="fw-bold text-info" id="delayVal">200</span>
                        </label>
                        <input type="range" class="form-range" id="delaySlider" min="50" max="2000" value="200" step="50" oninput="document.getElementById('delayVal').innerText = this.value">
                    </div>

                    <div class="mb-4">
                        <label class="form-label text-secondary">Active Storage Engine Plugin</label>
                        <select id="engineSelect" class="form-select bg-dark text-light border-secondary">
                            <option value="KeyDB">KeyDB In-Memory RAM Store (Tier-1)</option>
                            <option value="YugabyteDB (Distributed SQL)">YugabyteDB Distributed SQL (Tier-2 Async)</option>
                        </select>
                    </div>

                    <div class="d-flex gap-2">
                        <button onclick="toggleBenchmark('start')" class="btn btn-success flex-grow-1 py-2">Start Batch Test</button>
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
                        <div class="log-sys">[SYSTEM] Initialized Core Event Bus Streamer. Connected...</div>
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
            if (val === 0) {
                document.getElementById('limitDisplay').innerText = "Continuous (Unlimited)";
            } else {
                document.getElementById('limitDisplay').innerText = val.toLocaleString() + " txs";
            }
        }

        async function fetchMetrics() {
            try {
                const res = await fetch('/api/metrics');
                const data = await res.json();
                
                const now = Date.now();
                const timeDiff = (now - lastTime) / 1000;
                const countDiff = data.committed_txs - lastCount;
                
                let tps = 0;
                if (timeDiff > 0 && lastCount > 0) {
                    tps = Math.round(countDiff / timeDiff);
                }

                document.getElementById('txCount').innerText = data.committed_txs.toLocaleString();
                document.getElementById('tpsRate').innerText = tps.toLocaleString();
                document.getElementById('activeEngine').innerText = data.engine;

                const badge = document.getElementById('testBadge');
                if (data.is_testing) {
                    badge.innerText = 'Benchmark: RUNNING';
                    badge.className = 'badge badge-active px-3 py-2 rounded-pill';
                } else {
                    badge.innerText = 'Benchmark: STOPPED';
                    badge.className = 'badge badge-inactive px-3 py-2 rounded-pill';
                }

                lastCount = data.committed_txs;
                lastTime = now;
            } catch (err) {
                console.error("Failed to fetch metrics", err);
            }
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

        function clearLogs() {
            document.getElementById('terminal').innerHTML = '';
        }

        const evtSource = new EventSource('/api/logs');
        evtSource.onmessage = function(event) {
            const msg = JSON.parse(event.data);
            const term = document.getElementById('terminal');
            
            let classMap = {
                'ACK': 'log-ack',
                'ORDER': 'log-order',
                'COMMIT': 'log-commit',
                'SYS': 'log-sys'
            };
            let cls = classMap[msg.level] || 'log-sys';
            let uuidPart = msg.tx_uuid ? ' [' + msg.tx_uuid + ']' : '';
            let shardPart = msg.shard ? ' [' + msg.shard + ']' : '';
            
            let logLine = document.createElement('div');
            logLine.className = cls;
            logLine.innerText = '[' + msg.timestamp + '] [' + msg.level + ']' + shardPart + uuidPart + ' ' + msg.message;
            
            term.appendChild(logLine);

            if (term.childNodes.length > 200) {
                term.removeChild(term.firstChild);
            }
            term.scrollTop = term.scrollHeight;
        };

        setInterval(fetchMetrics, 1000);
        fetchMetrics();
    </script>
</body>
</html>`

	fmt.Fprint(w, html)
}
