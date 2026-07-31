package dashboard

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"hed-core/pkg/delta"
	"hed-core/pkg/hlf"
	"hed-core/plugins/memory"
)

type HLFServer struct {
	deployer    *hlf.Deployer
	deltaEngine *delta.DeltaEngine
	mu          sync.Mutex
	isDeployed  bool
}

func NewHLFServer(version string) *HLFServer {
	db := memory.New()
	return &HLFServer{
		deployer:    hlf.NewDeployer(version),
		deltaEngine: delta.New(db),
	}
}

func (s *HLFServer) Start(port string) error {
	http.HandleFunc("/", s.handleIndex)
	http.HandleFunc("/api/deploy-stream", s.handleDeployStream)
	http.HandleFunc("/api/start-deploy", s.handleStartDeploy)
	http.HandleFunc("/api/tx/execute", s.handleTxExecute)

	fmt.Printf("🚀 HLF Dashboard & Transaction Studio running at http://localhost%s\n", port)
	return http.ListenAndServe(port, nil)
}

func (s *HLFServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	w.Write([]byte(hlfIndexHTML))
}

func (s *HLFServer) handleStartDeploy(w http.ResponseWriter, r *http.Request) {
	go func() {
		s.deployer.RunDeployment()
		s.mu.Lock()
		s.isDeployed = true
		s.mu.Unlock()
	}()
	w.WriteHeader(http.StatusOK)
}

func (s *HLFServer) handleTxExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		ChannelID string `json:"channelId"`
		AccountID string `json:"accountId"`
		Amount    int64  `json:"amount"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	start := time.Now()
	s.deltaEngine.ApplyDelta(req.ChannelID, req.AccountID, req.Amount)
	latency := time.Since(start).Microseconds()

	totalTxs := s.deltaEngine.GetTxCount()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":      "SUCCESS",
		"txId":        fmt.Sprintf("tx_hlf_%d", time.Now().UnixNano()),
		"latencyUs":   latency,
		"totalTxs":    totalTxs,
		"channel":     req.ChannelID,
		"account":     req.AccountID,
		"deltaAmount": req.Amount,
		"appliedAt":   time.Now().Format(time.RFC3339),
	})
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
    <title>Hyperledger Fabric + HED Engine Studio</title>
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

        .tx-studio { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-top: 25px; }
        .panel { background: #1e293b; border-radius: 10px; padding: 20px; border: 1px solid #334155; }
        .form-group { margin-bottom: 15px; }
        label { display: block; font-size: 0.85rem; color: #94a3b8; margin-bottom: 6px; }
        input, select { width: 100%; box-sizing: border-box; background: #0f172a; color: #fff; border: 1px solid #475569; padding: 10px; border-radius: 6px; }
        
        .btn-tx { background: #38bdf8; color: #0f172a; border: none; padding: 12px; border-radius: 6px; font-weight: bold; cursor: pointer; width: 100%; font-size: 1rem; transition: all 0.2s ease; }
        .btn-tx:hover { background: #0284c7; color: #fff; }
        .btn-tx:disabled { background: #475569 !important; color: #94a3b8 !important; cursor: not-allowed; opacity: 0.6; }

        .tx-log { background: #020617; border-radius: 6px; padding: 15px; height: 220px; overflow-y: auto; font-family: monospace; font-size: 0.82rem; color: #4ade80; }
        .console { background: #020617; border-radius: 10px; padding: 20px; height: 200px; overflow-y: auto; font-family: 'Courier New', monospace; font-size: 0.85rem; color: #38bdf8; border: 1px solid #1e293b; }
        
        .btn { background: #10b981; color: #fff; border: none; padding: 12px 24px; border-radius: 8px; font-weight: bold; cursor: pointer; font-size: 1rem; transition: all 0.2s ease; }
        .btn:hover { background: #059669; }
        .btn:disabled { background: #475569 !important; color: #94a3b8 !important; cursor: not-allowed; opacity: 0.7; }
        .btn.failed-btn { background: #ef4444 !important; color: #fff !important; }
        .btn.failed-btn:hover { background: #dc2626 !important; }

        /* Pop-up Overlay Styling */
        .modal-overlay { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.75); z-index: 1000; justify-content: center; align-items: center; }
        .modal-card { background: #1e293b; border-radius: 12px; padding: 25px; width: 420px; text-align: center; border: 1px solid #475569; box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.5); }
        .modal-card img { width: 140px; height: 140px; border-radius: 50%; object-fit: cover; margin-bottom: 15px; border: 3px solid #ef4444; }
        .modal-title { font-size: 1.3rem; font-weight: bold; margin-bottom: 10px; }
        .modal-title.success { color: #4ade80; }
        .modal-title.error { color: #f87171; }
        .modal-msg { color: #cbd5e1; font-size: 0.9rem; margin-bottom: 20px; line-height: 1.4; }
        .modal-btn { background: #38bdf8; color: #0f172a; border: none; padding: 10px 20px; border-radius: 6px; font-weight: bold; cursor: pointer; }
        .modal-btn:hover { background: #0284c7; color: #fff; }

        @keyframes pulse { 0% { opacity: 1; } 50% { opacity: 0.6; } 100% { opacity: 1; } }
    </style>
</head>
<body>
    <div class="header">
        <div>
            <h1 style="margin:0; font-size:1.8rem;">⚡ Hyperledger Fabric + HED Engine Studio</h1>
            <p style="margin:5px 0 0 0; color:#94a3b8;">Automated Provisioning & Sub-Millisecond Transaction Execution</p>
        </div>
        <div>
            <span class="badge">HLF Version: v2.5.4</span>
            <button class="btn" id="btn-deploy" onclick="startDeployment()" style="margin-left:15px;">Start HLF Installation</button>
        </div>
    </div>

    <div class="grid" id="stage-grid">
        <!-- Stage cards -->
    </div>

    <div class="tx-studio">
        <div class="panel">
            <h3 style="margin-top:0; color:#38bdf8;">💳 Execute Ledger Transaction</h3>
            <div class="form-group">
                <label>Target Channel</label>
                <input type="text" id="txChannel" value="mychannel" readonly />
            </div>
            <div class="form-group">
                <label>Account / Key ID</label>
                <input type="text" id="txAccount" value="account_001" />
            </div>
            <div class="form-group">
                <label>Delta Balance Shift (int64)</label>
                <input type="number" id="txAmount" value="500" />
            </div>
            <button class="btn-tx" id="btn-submit-tx" onclick="submitTransaction()" disabled>Submit High-Speed Transaction (Complete Setup First)</button>
        </div>

        <div class="panel">
            <h3 style="margin-top:0; color:#4ade80;">📜 Execution Receipts & Block Commit Stream</h3>
            <div class="tx-log" id="tx-log-stream">Installation in progress. Complete deployment to unlock high-speed execution!</div>
        </div>
    </div>

    <h3 style="margin-top:30px;">Real-Time Terminal Execution Logs:</h3>
    <div class="console" id="console-logs">Waiting to launch deployment pipeline...</div>

    <!-- POPUP MODAL -->
    <div class="modal-overlay" id="popupModal">
        <div class="modal-card">
            <img id="modalImg" src="https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExM3hveThmc2prbWRrb3Bhdjl4dHZpMnBmeXZyd2J5eHZ2bXNpd3NxeSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/k3P1HSUatw23P8I3bX/giphy.gif" alt="Puppy GIF" />
            <div id="modalTitle" class="modal-title">Notice</div>
            <div id="modalMsg" class="modal-msg">Modal message text</div>
            <button class="modal-btn" onclick="closeModal()">Close</button>
        </div>
    </div>

    <script>
        const evtSource = new EventSource('/api/deploy-stream');
        const logsDiv = document.getElementById('console-logs');
        const txLogDiv = document.getElementById('tx-log-stream');
        const deployBtn = document.getElementById('btn-deploy');
        const submitTxBtn = document.getElementById('btn-submit-tx');

        let popupShown = false;

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
            
            let hasInProgress = false;
            let hasFailed = false;
            let allCompleted = stages.length > 0;

            stages.forEach(function(stage) {
                if (stage.status === 'in_progress') hasInProgress = true;
                if (stage.status === 'failed') hasFailed = true;
                if (stage.status !== 'completed') allCompleted = false;

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

            // Update Start Button & Submit Button States
            if (hasFailed) {
                deployBtn.disabled = false;
                deployBtn.className = 'btn failed-btn';
                deployBtn.innerText = 'Retry Installation (Failed)';
                
                submitTxBtn.disabled = true;
                submitTxBtn.innerText = 'Submit High-Speed Transaction (Setup Failed)';

                if (!popupShown) {
                    showModal(false, 'HLF Deployment Failed!', 'An error occurred during installation. Check execution logs below.');
                    popupShown = true;
                }
            } else if (hasInProgress) {
                deployBtn.disabled = true;
                deployBtn.className = 'btn';
                deployBtn.innerText = 'Installation In Progress...';

                submitTxBtn.disabled = true;
                submitTxBtn.innerText = 'Submit High-Speed Transaction (Installing...)';
            } else if (allCompleted) {
                deployBtn.disabled = true;
                deployBtn.className = 'btn';
                deployBtn.innerText = 'HLF Installed & Ready';

                submitTxBtn.disabled = false;
                submitTxBtn.innerText = 'Submit High-Speed Transaction';

                if (!popupShown) {
                    showModal(true, 'Installation Complete! 🎉', 'Hyperledger Fabric network is active. Transaction Studio is now unlocked!');
                    popupShown = true;
                }
            }
        }

        function submitTransaction() {
            if (submitTxBtn.disabled) return;

            const channelId = document.getElementById('txChannel').value;
            const accountId = document.getElementById('txAccount').value;
            const amount = parseInt(document.getElementById('txAmount').value);

            fetch('/api/tx/execute', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ channelId: channelId, accountId: accountId, amount: amount })
            })
            .then(res => {
                if (!res.ok) throw new Error("Transaction execution failed on server.");
                return res.json();
            })
            .then(data => {
                const logEntry = '<div>[' + data.appliedAt + '] <b>' + data.txId + '</b> | Acc: ' + data.account + ' | Shift: ' + data.deltaAmount + ' | Latency: <b>' + data.latencyUs + 'µs</b> | Total Txs: ' + data.totalTxs + '</div>';
                if (txLogDiv.innerText.includes("Installation in progress") || txLogDiv.innerText.includes("Complete deployment")) {
                    txLogDiv.innerHTML = logEntry;
                } else {
                    txLogDiv.innerHTML = logEntry + txLogDiv.innerHTML;
                }
            })
            .catch(err => {
                showModal(false, 'Transaction Error!', 'Failed to process transaction: ' + err.message);
            });
        }

        function startDeployment() {
            popupShown = false;
            deployBtn.disabled = true;
            deployBtn.innerText = 'Installation In Progress...';
            logsDiv.innerHTML = '<div>> Initiating deployment triggers...</div>';
            fetch('/api/start-deploy', { method: 'POST' });
        }

        function showModal(isSuccess, title, message) {
            const modal = document.getElementById('popupModal');
            const modalImg = document.getElementById('modalImg');
            const modalTitle = document.getElementById('modalTitle');
            const modalMsg = document.getElementById('modalMsg');

            modalTitle.innerText = title;
            modalMsg.innerText = message;

            if (isSuccess) {
                modalTitle.className = 'modal-title success';
                // Success icon
                modalImg.src = 'https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExaG45Mm8xYWp0OXE1cTRxbzJrdDZwMzEwODk0eTVrNWs0Z2VraTF4ciZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/artj92V8o75VPL7AeQ/giphy.gif';
                modalImg.style.borderColor = '#22c55e';
            } else {
                modalTitle.className = 'modal-title error';
                // Smirking dog / puppy GIF for errors
                modalImg.src = 'https://media.giphy.com/media/v1.Y2lkPTc5MGI3NjExM3hveThmc2prbWRrb3Bhdjl4dHZpMnBmeXZyd2J5eHZ2bXNpd3NxeSZlcD12MV9pbnRlcm5hbF9naWZfYnlfaWQmY3Q9Zw/k3P1HSUatw23P8I3bX/giphy.gif';
                modalImg.style.borderColor = '#ef4444';
            }

            modal.style.display = 'flex';
        }

        function closeModal() {
            document.getElementById('popupModal').style.display = 'none';
        }

        function escapeHtml(text) {
            return text.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
        }
    </script>
</body>
</html>`