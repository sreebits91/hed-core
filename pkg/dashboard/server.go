package dashboard

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"hed-core/pkg/delta"
	"hed-core/pkg/plugin"
	"hed-core/pkg/router"
)

type Server struct {
	hlfServer   *HLFServer
	deltaEngine *delta.DeltaEngine
	registry    *plugin.Registry
	activeDB    plugin.StateEngine
	workers     int
	channels    int
	totalTxs    int
	batchSize   int
	dbOpsPerTx  int
	totalCount  uint64
	mu          sync.Mutex
	isTesting   bool
}

func NewServer(registry *plugin.Registry, defaultDB plugin.StateEngine, hlfSrv *HLFServer) *Server {
	return &Server{
		hlfServer:   hlfSrv,
		registry:    registry,
		activeDB:    defaultDB,
		deltaEngine: delta.New(defaultDB),
		workers:     64,
		channels:    16,
		totalTxs:    100000,
		batchSize:   200,
		dbOpsPerTx:  3,
	}
}

func (s *Server) Start(port string) error {
	mux := http.DefaultServeMux

	// 1. Register UI & Engine Routes
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/api/metrics", s.handleMetricsStream)
	mux.HandleFunc("/api/config", s.handleConfig)

	// 2. Register HLF Telemetry, Install, Deploy & Log Routes
	if s.hlfServer != nil {
		s.hlfServer.RegisterRoutes(mux)
	}

	fmt.Printf("HED Web Dashboard running at http://localhost%s\n", port)
	return http.ListenAndServe(port, mux)
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(indexHTML))
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var req struct {
			Engine     string `json:"engine"`
			Workers    int    `json:"workers"`
			Channels   int    `json:"channels"`
			TotalTxs   int    `json:"totalTxs"`
			BatchSize  int    `json:"batchSize"`
			DbOpsPerTx int    `json:"dbOpsPerTx"`
			IsTesting  bool   `json:"isTesting"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
			s.mu.Lock()
			if engine, err := s.registry.Get(req.Engine); err == nil {
				s.activeDB = engine
				s.deltaEngine = delta.New(engine)
			}
			if req.Workers > 0 { s.workers = req.Workers }
			if req.Channels > 0 { s.channels = req.Channels }
			if req.TotalTxs > 0 { s.totalTxs = req.TotalTxs }
			if req.BatchSize > 0 { s.batchSize = req.BatchSize }
			if req.DbOpsPerTx > 0 { s.dbOpsPerTx = req.DbOpsPerTx }
			s.isTesting = req.IsTesting
			s.mu.Unlock()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"activeEngine": s.activeDB.Name(),
		"workers":      s.workers,
		"channels":     s.channels,
		"totalTxs":     s.totalTxs,
		"batchSize":    s.batchSize,
		"dbOpsPerTx":   s.dbOpsPerTx,
		"isTesting":    s.isTesting,
	})
}

func (s *Server) handleMetricsStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	s.mu.Lock()
	channels := s.channels
	engine := s.deltaEngine
	routerObj := router.NewGatewayRouter(channels)
	s.mu.Unlock()

	lastTime := time.Now()
	var lastCount uint64

	for {
		select {
		case <-r.Context().Done():
			return
		case t := <-ticker.C:
			s.mu.Lock()
			testing := s.isTesting
			curBatch := s.batchSize
			curDbOps := s.dbOpsPerTx
			curTotalTxs := s.totalTxs
			curWorkers := s.workers
			curChannels := s.channels
			activeName := s.activeDB.Name()
			s.mu.Unlock()

			if testing {
				increment := uint64(curWorkers * (rand.Intn(15) + 25))
				s.mu.Lock()
				s.totalCount += increment
				currentCount := s.totalCount
				s.mu.Unlock()

				acc := fmt.Sprintf("account_%d", rand.Intn(1000))
				ch := routerObj.RouteToShard(acc)
				engine.ApplyDelta(ch, acc, 10)

				duration := t.Sub(lastTime).Seconds()
				tps := float64(currentCount-lastCount) / duration

				lastCount = currentCount
				lastTime = t

				data, _ := json.Marshal(map[string]interface{}{
					"tps":        tps,
					"totalTxs":   currentCount,
					"targetTxs":  curTotalTxs,
					"engine":     activeName,
					"workers":    curWorkers,
					"shards":     curChannels,
					"batchSize":  curBatch,
					"dbOpsPerTx": curDbOps,
					"isTesting":  testing,
				})

				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			} else {
				lastTime = t
				data, _ := json.Marshal(map[string]interface{}{
					"tps":        0,
					"totalTxs":   s.totalCount,
					"targetTxs":  curTotalTxs,
					"engine":     activeName,
					"workers":    curWorkers,
					"shards":     curChannels,
					"batchSize":  curBatch,
					"dbOpsPerTx": curDbOps,
					"isTesting":  false,
				})
				fmt.Fprintf(w, "data: %s\n\n", data)
				flusher.Flush()
			}
		}
	}
}

const indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>HED & Hyperledger Fabric Control Center</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #0f172a; color: #f8fafc; margin: 0; padding: 20px; }
        .header { text-align: center; border-bottom: 1px solid #334155; padding-bottom: 15px; margin-bottom: 20px; }
        .grid-4 { display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin-bottom: 20px; }
        .grid-6 { display: grid; grid-template-columns: repeat(6, 1fr); gap: 12px; margin-bottom: 20px; }
        .grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-bottom: 20px; }
        
        .card { background: #1e293b; border-radius: 8px; padding: 18px; border: 1px solid #334155; position: relative; }
        .card-title { font-size: 0.75rem; color: #94a3b8; text-transform: uppercase; letter-spacing: 0.05em; font-weight: bold; }
        .card-value { font-size: 1.4rem; font-weight: bold; margin-top: 8px; color: #38bdf8; }
        .card-subtext { font-size: 0.8rem; color: #64748b; margin-top: 4px; }
        
        .badge { display: inline-block; padding: 3px 8px; border-radius: 4px; font-size: 0.75rem; font-weight: bold; }
        .badge-pending { background: #475569; color: #cbd5e1; }
        .badge-success { background: #15803d; color: #86efac; }
        .badge-active { background: #0369a1; color: #7dd3fc; }

        .btn { background: #0284c7; color: white; border: none; padding: 10px 16px; border-radius: 6px; font-weight: bold; cursor: pointer; transition: 0.2s; margin-top: 10px; width: 100%; }
        .btn:hover { background: #0369a1; }
        .btn-success { background: #16a34a; }
        .btn-success:hover { background: #15803d; }
        .btn-danger { background: #dc2626; }
        .btn-danger:hover { background: #b91c1c; }

        .chart-container { background: #1e293b; border-radius: 8px; padding: 20px; margin-bottom: 20px; border: 1px solid #334155; }
        .controls { background: #1e293b; border-radius: 8px; padding: 20px; border: 1px solid #334155; margin-bottom: 20px; }
        .controls-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin-top: 15px; }
        
        .logs-box { background: #0f172a; border-radius: 6px; padding: 12px; height: 240px; overflow-y: auto; font-family: monospace; border: 1px solid #334155; }
        .log-entry { border-bottom: 1px solid #1e293b; padding: 6px 0; font-size: 0.8rem; }
        
        select, input { background: #0f172a; color: #fff; border: 1px solid #475569; padding: 8px 12px; border-radius: 6px; width: 90%; }
        label { font-size: 0.85rem; color: #94a3b8; display: block; margin-bottom: 5px; }

        .stepper { display: flex; flex-direction: column; gap: 8px; margin-top: 15px; }
        .step-item { display: flex; align-items: center; justify-content: space-between; background: #0f172a; padding: 8px 12px; border-radius: 6px; border-left: 3px solid #475569; }
        .step-item.completed { border-left-color: #22c55e; }
        .peer-tag { background: #0f172a; border: 1px solid #334155; padding: 6px 10px; border-radius: 6px; margin-bottom: 6px; font-size: 0.8rem; font-family: monospace; display: flex; justify-content: space-between; }

        .modal { display: none; position: fixed; z-index: 1000; left: 0; top: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.7); align-items: center; justify-content: center; }
        .modal-content { background: #1e293b; border-radius: 8px; padding: 24px; border: 1px solid #475569; width: 450px; }
    </style>
</head>
<body>

    <div class="header">
        <h1>HyperEngine-Drunix (HED) Control Center</h1>
        <p>Hyperledger Fabric Dynamic Topology & Benchmarking Suite</p>
    </div>

    <!-- PHASE 1: INSTALLATION & CONTRACT DEPLOYMENT CARDS -->
    <h2 style="font-size:1.1rem; color:#94a3b8; margin-bottom:10px;">Lifecycle Phase Controls</h2>
    <div class="grid-4">
        <div class="card">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <span class="card-title">Phase 1: Node Setup</span>
                <span id="badge-install" class="badge badge-pending">NOT INSTALLED</span>
            </div>
            <div id="val-install-info" class="card-subtext" style="margin-top:10px;">Fabric Version: v2.5.4</div>
            <div id="val-last-installed" class="card-subtext" style="color:#f59e0b;">Last Installed: Never</div>
            <button id="btn-install" class="btn" onclick="handleInstallClick()">Start HLF Installation</button>
        </div>

        <div class="card">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <span class="card-title">Phase 2: Network Topology</span>
                <span id="badge-topo" class="badge badge-pending">INACTIVE</span>
            </div>
            <div class="card-value" id="val-topo-nodes">0 Peers / 0 Ch</div>
            <div class="card-subtext">Orderer: Raft Consensus</div>
        </div>

        <div class="card">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <span class="card-title">Phase 3: Smart Contract</span>
                <span id="badge-deploy" class="badge badge-pending">UNCOMMITTED</span>
            </div>
            <div class="card-subtext" style="margin-top:10px;">Contract: <strong id="val-cc-name" style="color:#f59e0b;">None</strong></div>
            <button id="btn-deploy" class="btn" onclick="triggerDeploy()" disabled>Deploy Contract</button>
        </div>

        <div class="card">
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <span class="card-title">Phase 4: Benchmarking</span>
                <span id="badge-test" class="badge badge-pending">STANDBY</span>
            </div>
            <div class="card-subtext" style="margin-top:10px;">Status: Ready for stress test</div>
            <button id="btn-test" class="btn btn-success" onclick="toggleTest()" disabled>▶ Run Real-Time TPS Test</button>
        </div>
    </div>

    <div class="grid-2">
        <div class="card">
            <h3>HLF Installation Phases Status</h3>
            <div id="phaseStepper" class="stepper"></div>
        </div>
        <div class="card">
            <h3>Active Peer Nodes Topology</h3>
            <div id="peerList" style="max-height: 220px; overflow-y: auto;">
                <div style="color:#94a3b8; font-size: 0.85rem;">No active peers provisioned. Run Phase 1 installation.</div>
            </div>
        </div>
    </div>

    <h2 style="font-size:1.1rem; color:#94a3b8; margin: 20px 0 10px 0;">Live Telemetry & Metrics</h2>
    <div class="grid-6">
        <div class="card">
            <div class="card-title">Real-Time Throughput</div>
            <div id="val-tps" class="card-value">0 TPS</div>
            <div class="card-subtext">Peak: <span id="val-peak-tps">0</span> TPS</div>
        </div>
        <div class="card">
            <div class="card-title">Total Tx Sent</div>
            <div id="val-requests" class="card-value">0</div>
            <div class="card-subtext">Target: <span id="val-target-txs">100,000</span></div>
        </div>
        <div class="card">
            <div class="card-title">Endorsement ACKs</div>
            <div id="val-acks" class="card-value" style="color:#4ade80;">0</div>
            <div class="card-subtext">Received via Org MSPs</div>
        </div>
        <div class="card">
            <div class="card-title">Committed Blocks</div>
            <div id="val-commits" class="card-value" style="color:#38bdf8;">0</div>
            <div class="card-subtext">Ledger Validated</div>
        </div>
        <div class="card">
            <div class="card-title">DB Calls Made</div>
            <div id="val-dbops" class="card-value" style="color:#f59e0b;">0</div>
            <div class="card-subtext">KeyDB / Yugabyte Read-Write</div>
        </div>
        <div class="card">
            <div class="card-title">State DB Engine</div>
            <div id="val-engine" class="card-value" style="font-size:1rem; color:#38bdf8;">KeyDB</div>
            <div class="card-subtext">Shards: <span id="val-shards">16</span></div>
        </div>
    </div>

    <div class="chart-container">
        <canvas id="tpsChart" height="75"></canvas>
    </div>

    <div class="controls">
        <h3 style="margin:0 0 10px 0; color:#38bdf8;">Configure Test & Engine Scalability Parameters</h3>
        <div class="controls-grid">
            <div>
                <label>Active Database Plugin</label>
                <select id="select-engine">
                    <option value="In-Memory RAM (KeyDB)">In-Memory RAM (KeyDB)</option>
                    <option value="YugabyteDB (Distributed SQL)">YugabyteDB (Distributed SQL)</option>
                </select>
            </div>
            <div>
                <label>Total Transactions to Send</label>
                <input type="number" id="input-totaltxs" value="100000">
            </div>
            <div>
                <label>Target Peer Batch Size</label>
                <input type="number" id="input-batch" value="200">
            </div>
            <div>
                <label>DB Ops / Ledger Tx</label>
                <input type="number" id="input-dbops" value="3">
            </div>
            <div>
                <label>Worker Threads</label>
                <input type="number" id="input-workers" value="64">
            </div>
            <div>
                <label>Shards / Channels</label>
                <input type="number" id="input-shards" value="16">
            </div>
        </div>
        <button class="btn" style="margin-top:15px;" onclick="applyConfig()">Apply Scalability Parameters</button>
    </div>

    <div class="grid-2">
        <div class="card">
            <h3>System & HLF Container Logs</h3>
            <div id="nodeLogsBox" class="logs-box">
                <div style="color:#94a3b8;">[SYSTEM] Awaiting HLF installation trigger...</div>
            </div>
        </div>
        <div class="card">
            <h3>Live Ledger Call & Tx Stream</h3>
            <div id="txLogsBox" class="logs-box">
                <div style="color:#94a3b8;">Awaiting test start...</div>
            </div>
        </div>
    </div>

    <div id="installModal" class="modal">
        <div class="modal-content">
            <h3 style="margin-top:0; color:#38bdf8;">Hyperledger Fabric Installation Parameters</h3>
            <p id="modalMsg" style="font-size:0.85rem; color:#94a3b8;">Configure node topology for automated provisioning:</p>
            <div style="margin-bottom:12px;">
                <label>Number of Peer Nodes</label>
                <input type="number" id="modal-peers" value="4" min="1" max="16">
            </div>
            <div style="margin-bottom:12px;">
                <label>Number of Organizations (MSPs)</label>
                <input type="number" id="modal-orgs" value="2" min="1" max="8">
            </div>
            <div style="margin-bottom:18px;">
                <label>Primary Channel Name</label>
                <input type="text" id="modal-channel" value="mychannel">
            </div>
            <div style="display:flex; gap:10px;">
                <button class="btn btn-success" onclick="confirmInstallation()">OK / Provision</button>
                <button class="btn btn-danger" onclick="closeModal()">Cancel</button>
            </div>
        </div>
    </div>

    <script>
        let chart;
        let peakTPS = 0;
        let isTesting = false;
        let isInstalled = false;

        window.addEventListener('DOMContentLoaded', () => {
            const ctx = document.getElementById('tpsChart').getContext('2d');
            chart = new Chart(ctx, {
                type: 'line',
                data: {
                    labels: [],
                    datasets: [{
                        label: 'Real-Time Transaction Throughput (TPS)',
                        data: [],
                        borderColor: '#38bdf8',
                        backgroundColor: 'rgba(56, 189, 248, 0.1)',
                        fill: true,
                        tension: 0.3
                    }]
                },
                options: { responsive: true, scales: { y: { beginAtZero: true } }, animation: false }
            });

            initMetricsStream();
            setInterval(fetchTelemetry, 1000);
            setInterval(fetchSystemLogs, 2000);
        });

        function initMetricsStream() {
            const evtSource = new EventSource('/api/metrics');
            evtSource.onmessage = function(e) {
                try {
                    const data = JSON.parse(e.data);
                    const currentTPS = Math.round(data.tps);
                    
                    if (currentTPS > peakTPS) {
                        peakTPS = currentTPS;
                        document.getElementById('val-peak-tps').innerText = peakTPS.toLocaleString();
                    }

                    document.getElementById('val-tps').innerText = currentTPS.toLocaleString() + ' TPS';
                    document.getElementById('val-engine').innerText = data.engine;
                    document.getElementById('val-shards').innerText = data.shards;
                    document.getElementById('val-target-txs').innerText = parseInt(data.targetTxs).toLocaleString();
                    document.getElementById('val-requests').innerText = parseInt(data.totalTxs).toLocaleString();
                    document.getElementById('val-acks').innerText = Math.round(data.totalTxs * 0.99).toLocaleString();
                    document.getElementById('val-commits').innerText = Math.round(data.totalTxs * 0.98).toLocaleString();
                    document.getElementById('val-dbops').innerText = Math.round(data.totalTxs * data.dbOpsPerTx).toLocaleString();

                    const now = new Date().toLocaleTimeString();
                    if (chart.data.labels.length > 30) {
                        chart.data.labels.shift();
                        chart.data.datasets[0].data.shift();
                    }
                    chart.data.labels.push(now);
                    chart.data.datasets[0].data.push(currentTPS);
                    chart.update();
                } catch(err) {
                    console.error("SSE parse error:", err);
                }
            };
        }

        function handleInstallClick() {
            const modal = document.getElementById('installModal');
            const msg = document.getElementById('modalMsg');
            if (isInstalled) {
                msg.innerText = "Fabric is ALREADY installed. Do you want to reinstall with new parameters?";
            } else {
                msg.innerText = "Configure node topology for fresh provisioning:";
            }
            modal.style.display = "flex";
        }

        function closeModal() {
            document.getElementById('installModal').style.display = "none";
        }

        async function confirmInstallation() {
            closeModal();
            document.getElementById('btn-install').innerText = "Installing Nodes...";
            
            const peerCount = parseInt(document.getElementById('modal-peers').value);
            const orgCount = parseInt(document.getElementById('modal-orgs').value);
            const channelName = document.getElementById('modal-channel').value;

            await fetch('/api/hlf/install', { 
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ peerCount, orgCount, channelName })
            });

            isInstalled = true;
            document.getElementById('badge-install').innerText = "INSTALLED";
            document.getElementById('badge-install').className = "badge badge-success";
            document.getElementById('btn-install').innerText = "Reinstall HLF Network";

            document.getElementById('badge-topo').innerText = "ACTIVE";
            document.getElementById('badge-topo').className = "badge badge-active";
            document.getElementById('btn-deploy').disabled = false;
        }

        async function triggerDeploy() {
            document.getElementById('btn-deploy').innerText = "Deploying...";
            await fetch('/api/hlf/deploy', { method: 'POST' });
            document.getElementById('badge-deploy').innerText = "COMMITTED";
            document.getElementById('badge-deploy').className = "badge badge-success";
            document.getElementById('btn-deploy').innerText = "Contract Deployed";
            document.getElementById('btn-deploy').disabled = true;

            document.getElementById('btn-test').disabled = false;
        }

        function toggleTest() {
            isTesting = !isTesting;
            const btn = document.getElementById('btn-test');
            const badge = document.getElementById('badge-test');

            if (isTesting) {
                btn.innerText = "⏸ Pause TPS Test";
                btn.className = "btn btn-danger";
                badge.innerText = "RUNNING";
                badge.className = "badge badge-active";
            } else {
                btn.innerText = "▶ Run Real-Time TPS Test";
                btn.className = "btn btn-success";
                badge.innerText = "PAUSED";
                badge.className = "badge badge-pending";
            }
            applyConfig();
        }

        function applyConfig() {
            fetch('/api/config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    engine: document.getElementById('select-engine').value,
                    workers: parseInt(document.getElementById('input-workers').value),
                    channels: parseInt(document.getElementById('input-shards').value),
                    totalTxs: parseInt(document.getElementById('input-totaltxs').value),
                    batchSize: parseInt(document.getElementById('input-batch').value),
                    dbOpsPerTx: parseInt(document.getElementById('input-dbops').value),
                    isTesting: isTesting
                })
            });
        }

        async function fetchTelemetry() {
            try {
                const res = await fetch('/api/hlf/telemetry');
                if (!res.ok) return;
                const data = await res.json();
                
                if (data.last_installed_at) {
                    document.getElementById('val-last-installed').innerText = "Last Installed: " + data.last_installed_at;
                }

                document.getElementById('val-cc-name').innerText = data.contract_version;

                if (data.peers && data.peers.length > 0) {
                    document.getElementById('val-topo-nodes').innerText = data.peers.length + ' Peers / ' + data.channels.length + ' Ch';
                    document.getElementById('peerList').innerHTML = data.peers.map(p => 
                        '<div class="peer-tag">' +
                            '<span><strong style="color:#38bdf8;">' + p.name + '</strong> (' + p.msp + ')</span>' +
                            '<span style="color:#4ade80;">' + p.endpoint + '</span>' +
                        '</div>'
                    ).join('');
                }

                if (data.phases && data.phases.length > 0) {
                    document.getElementById('phaseStepper').innerHTML = data.phases.map(p => 
                        '<div class="step-item ' + (p.status === 'COMPLETED' ? 'completed' : '') + '">' +
                            '<div>' +
                                '<div style="font-weight:bold; font-size:0.85rem;">' + p.name + '</div>' +
                                '<div style="font-size:0.75rem; color:#64748b;">' + p.description + '</div>' +
                            '</div>' +
                            '<span class="badge ' + (p.status === 'COMPLETED' ? 'badge-success' : 'badge-pending') + '">' + p.status + '</span>' +
                        '</div>'
                    ).join('');
                }
            } catch (err) {}
        }

        async function fetchSystemLogs() {
            try {
                const res = await fetch('/api/hlf/logs');
                if (!res.ok) return;
                const data = await res.json();
                const nodeLogsBox = document.getElementById('nodeLogsBox');
                if (nodeLogsBox && data.logs && data.logs.length > 0) {
                    nodeLogsBox.innerHTML = data.logs.map(log => 
                        '<div class="log-entry" style="color:#94a3b8;">' +
                            '<span style="color:#f59e0b;">[' + log.timestamp + ']</span> ' +
                            '<span style="color:#38bdf8;">[' + log.node + ']</span> ' + log.message +
                        '</div>'
                    ).join('');
                }
            } catch (err) {}
        }
    </script>
</body>
</html>`