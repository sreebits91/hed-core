package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"hed-core/pkg/delta"
	"hed-core/pkg/plugin"
	"hed-core/pkg/router"
)

type Server struct {
	deltaEngine *delta.DeltaEngine
	registry    *plugin.Registry
	activeDB    plugin.StateEngine
	workers     int
	channels    int
	mu          sync.Mutex
	isRunning   bool
}

func NewServer(registry *plugin.Registry, defaultDB plugin.StateEngine) *Server {
	return &Server{
		registry:    registry,
		activeDB:    defaultDB,
		deltaEngine: delta.New(defaultDB),
		workers:     64,
		channels:    16,
	}
}

func (s *Server) Start(port string) error {
	http.HandleFunc("/", s.handleIndex)
	http.HandleFunc("/api/metrics", s.handleMetricsStream)
	http.HandleFunc("/api/config", s.handleConfig)

	fmt.Printf("🚀 HED Web Dashboard running at http://localhost%s\n", port)
	return http.ListenAndServe(port, nil)
}

func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(indexHTML))
}

func (s *Server) handleConfig(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodPost {
		var req struct {
			Engine   string `json:"engine"`
			Workers  int    `json:"workers"`
			Channels int    `json:"channels"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err == nil {
			s.mu.Lock()
			if engine, err := s.registry.Get(req.Engine); err == nil {
				s.activeDB = engine
				s.deltaEngine = delta.New(engine)
			}
			if req.Workers > 0 {
				s.workers = req.Workers
			}
			if req.Channels > 0 {
				s.channels = req.Channels
			}
			s.mu.Unlock()
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	json.NewEncoder(w).Encode(map[string]interface{}{
		"activeEngine": s.activeDB.Name(),
		"workers":      s.workers,
		"channels":     s.channels,
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

	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()

	s.mu.Lock()
	workers := s.workers
	channels := s.channels
	engine := s.deltaEngine
	routerObj := router.NewGatewayRouter(channels)
	s.mu.Unlock()

	// Spawn background workers for continuous TPS generation
	stopChan := make(chan struct{})
	defer close(stopChan)

	for i := 0; i < workers; i++ {
		go func(wID int) {
			acc := fmt.Sprintf("account_%d", wID%1000)
			ch := routerObj.RouteToShard(acc)
			for {
				select {
				case <-stopChan:
					return
				default:
					engine.ApplyDelta(ch, acc, 10)
				}
			}
		}(i)
	}

	var lastCount uint64
	lastTime := time.Now()

	for {
		select {
		case <-r.Context().Done():
			return
		case t := <-ticker.C:
			currentCount := engine.GetTxCount()
			duration := t.Sub(lastTime).Seconds()
			tps := float64(currentCount-lastCount) / duration

			lastCount = currentCount
			lastTime = t

			s.mu.Lock()
			activeName := s.activeDB.Name()
			curWorkers := s.workers
			curChannels := s.channels
			s.mu.Unlock()

			data, _ := json.Marshal(map[string]interface{}{
				"tps":      tps,
				"totalTxs": currentCount,
				"engine":   activeName,
				"workers":  curWorkers,
				"shards":   curChannels,
			})

			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

const indexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>HyperEngine-Drunix Dashboard</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
    <style>
        body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; background: #0f172a; color: #f8fafc; margin: 0; padding: 20px; }
        .header { text-align: center; border-bottom: 1px solid #334155; padding-bottom: 15px; margin-bottom: 20px; }
        .grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin-bottom: 20px; }
        .card { background: #1e293b; border-radius: 8px; padding: 15px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); }
        .card-title { font-size: 0.85rem; color: #94a3b8; text-transform: uppercase; }
        .card-value { font-size: 1.8rem; font-weight: bold; margin-top: 5px; color: #38bdf8; }
        .chart-container { background: #1e293b; border-radius: 8px; padding: 20px; margin-bottom: 20px; }
        .controls { background: #1e293b; border-radius: 8px; padding: 20px; display: flex; gap: 20px; align-items: center; }
        select, input, button { background: #0f172a; color: #fff; border: 1px solid #475569; padding: 8px 12px; border-radius: 6px; }
        button { background: #0284c7; border: none; font-weight: bold; cursor: pointer; }
        button:hover { background: #0369a1; }
    </style>
</head>
<body>
    <div class="header">
        <h1>⚡ HyperEngine-Drunix (HED)</h1>
        <p>Real-Time DLT Performance Monitor & State Engine Control</p>
    </div>

    <div class="grid">
        <div class="card"><div class="card-title">Current Throughput</div><div id="val-tps" class="card-value">0 TPS</div></div>
        <div class="card"><div class="card-title">Total Transactions</div><div id="val-txs" class="card-value">0</div></div>
        <div class="card"><div class="card-title">Active Database Plugin</div><div id="val-engine" class="card-value" style="font-size:1.1rem; color:#4ade80;">Loading...</div></div>
        <div class="card"><div class="card-title">Worker Threads / Shards</div><div id="val-threads" class="card-value">0 / 0</div></div>
    </div>

    <div class="chart-container">
        <canvas id="tpsChart" height="90"></canvas>
    </div>

    <div class="controls">
        <h3>Runtime Engine Config:</h3>
        <label>Plugin: 
            <select id="select-engine">
                <option value="In-Memory RAM (KeyDB)">In-Memory RAM (KeyDB)</option>
                <option value="YugabyteDB (Distributed SQL)">YugabyteDB (Distributed SQL)</option>
            </select>
        </label>
        <label>Workers: <input type="number" id="input-workers" value="64" style="width:70px;"></label>
        <label>Shards: <input type="number" id="input-shards" value="16" style="width:70px;"></label>
        <button onclick="updateConfig()">Apply Engine Changes</button>
    </div>

    <script>
        const ctx = document.getElementById('tpsChart').getContext('2d');
        const chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [{
                    label: 'Transaction Throughput (TPS)',
                    data: [],
                    borderColor: '#38bdf8',
                    backgroundColor: 'rgba(56, 189, 248, 0.1)',
                    fill: true,
                    tension: 0.3
                }]
            },
            options: { scales: { y: { beginAtZero: true } }, animation: false }
        });

        const evtSource = new EventSource('/api/metrics');
        evtSource.onmessage = function(e) {
            const data = JSON.parse(e.data);
            document.getElementById('val-tps').innerText = Math.round(data.tps).toLocaleString() + ' TPS';
            document.getElementById('val-txs').innerText = data.totalTxs.toLocaleString();
            document.getElementById('val-engine').innerText = data.engine;
            document.getElementById('val-threads').innerText = data.workers + ' / ' + data.shards;

            const now = new Date().toLocaleTimeString();
            if (chart.data.labels.length > 30) {
                chart.data.labels.shift();
                chart.data.datasets[0].data.shift();
            }
            chart.data.labels.push(now);
            chart.data.datasets[0].data.push(data.tps);
            chart.update();
        };

        function updateConfig() {
            fetch('/api/config', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    engine: document.getElementById('select-engine').value,
                    workers: parseInt(document.getElementById('input-workers').value),
                    channels: parseInt(document.getElementById('input-shards').value)
                })
            });
        }
    </script>
</body>
</html>`
