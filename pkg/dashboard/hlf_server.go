package dashboard

import (
	"fmt"
	"net/http"

	"hed-core/pkg/hlf"
)

type HLFServer struct {
	deployer *hlf.Deployer
}

func NewHLFServer(version string) *HLFServer {
	return &HLFServer{
		deployer: hlf.NewDeployer(version),
	}
}

func (s *HLFServer) Start(port string) error {
	http.HandleFunc("/", s.handleIndex)
	http.HandleFunc("/api/deploy-stream", s.handleDeployStream)
	http.HandleFunc("/api/start-deploy", s.handleStartDeploy)

	fmt.Printf("🚀 HLF Installation Dashboard running at http://localhost%s\n", port)
	return http.ListenAndServe(port, nil)
}

func (s *HLFServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(hlfIndexHTML))
}

func (s *HLFServer) handleStartDeploy(w http.ResponseWriter, r *http.Request) {
	go s.deployer.RunDeployment()
	w.WriteHeader(http.StatusOK)
}

func (s *HLFServer) handleDeployStream(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	ch := make(chan string, 100)
	s.deployer.RegisterListener(ch)
	defer s.deployer.UnregisterListener(ch)

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			fmt.Fprint(w, msg)
			flusher.Flush()
		}
	}
}

const hlfIndexHTML = `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>Hyperledger Fabric Deployment Pipeline</title>
    <style>
        body { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; background: #0f172a; color: #f8fafc; margin: 0; padding: 25px; }
        .header { display: flex; justify-content: space-between; align-items: center; border-bottom: 1px solid #334155; padding-bottom: 20px; margin-bottom: 25px; }
        .badge { background: #0284c7; color: #fff; padding: 6px 14px; border-radius: 20px; font-weight: bold; font-size: 0.9rem; }
        .grid { display: grid; grid-template-columns: repeat(5, 1fr); gap: 15px; margin-bottom: 25px; }
        .stage-card { background: #1e293b; border-radius: 10px; padding: 18px; border-left: 6px solid #475569; transition: all 0.3s ease; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.2); }
        .stage-card.in_progress { animation: pulse 1.5s infinite; border-color: #38bdf8 !important; }
        .stage-card.completed { border-color: #22c55e !important; background: #14532d22; }
        .stage-card.failed { border-color: #ef4444 !important; background: #7f1d1d22; }
        .stage-title { font-weight: bold; font-size: 0.95rem; margin-bottom: 8px; }
        .stage-desc { font-size: 0.78rem; color: #94a3b8; line-height: 1.3; }
        .stage-timer { font-size: 0.85rem; font-weight: bold; margin-top: 12px; color: #38bdf8; }
        .console { background: #020617; border-radius: 10px; padding: 20px; height: 350px; overflow-y: auto; font-family: 'Courier New', monospace; font-size: 0.85rem; color: #38bdf8; border: 1px solid #1e293b; }
        .btn { background: #10b981; color: #fff; border: none; padding: 12px 24px; border-radius: 8px; font-weight: bold; cursor: pointer; font-size: 1rem; }
        .btn:hover { background: #059669; }
        @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.6; } 100% { opacity: 1; } }
    </style>
</head>
<body>
    <div class="header">
        <div>
            <h1 style="margin:0; font-size:1.8rem;">⚡ Hyperledger Fabric Deployment Orchestrator</h1>
            <p style="margin:5px 0 0 0; color:#94a3b8;">Automated Stage-wise Network Provisioning & Verification</p>
        </div>
        <div>
            <span class="badge">HLF Version: v2.5.4</span>
            <button class="btn" onclick="startDeployment()" style="margin-left:15px;">Start HLF Installation</button>
        </div>
    </div>

    <div class="grid" id="stage-grid">
        <!-- Stage cards populated dynamically -->
    </div>

    <h3>Real-Time Terminal Execution Logs:</h3>
    <div class="console" id="console-logs">Waiting to launch deployment pipeline...</div>

    <script>
        const evtSource = new EventSource('/api/deploy-stream');
        const logsDiv = document.getElementById('console-logs');

        evtSource.onmessage = function(e) {
            const data = JSON.parse(e.data);
            renderStages(data.stages);
            if (data.log) {
                logsDiv.innerHTML += '<div>> ' + escapeHtml(data.log) + '</div>';
                logsDiv.scrollTop = logsDiv.scrollHeight;
            }
        };

        function renderStages(stages) {
            const grid = document.getElementById('stage-grid');
            grid.innerHTML = '';
            stages.forEach(function(stage) {
                const card = document.createElement('div');
                card.className = 'stage-card ' + stage.status;
                card.style.borderLeftColor = stage.color;

                var durationText = stage.duration ? (' | ' + stage.duration) : '';
                
                card.innerHTML = 
                    '<div class="stage-title" style="color:' + stage.color + '">' + stage.name + '</div>' +
                    '<div class="stage-desc">' + stage.description + '</div>' +
                    '<div class="stage-timer">Status: ' + stage.status.toUpperCase() + durationText + '</div>';
                
                grid.appendChild(card);
            });
        }

        function startDeployment() {
            logsDiv.innerHTML = '<div>> Initiating deployment triggers...</div>';
            fetch('/api/start-deploy', { method: 'POST' });
        }

        function escapeHtml(text) {
            return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
        }
    </script>
</body>
</html>`
