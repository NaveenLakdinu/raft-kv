package server

import (
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"sync"

	"raft-kv/internal/kv"
	"raft-kv/internal/raft"
)

// NodeView represents the snapshot of a node's state for the UI
type NodeView struct {
	ID        int               `json:"id"`
	Role      string            `json:"role"`
	Term      int               `json:"term"`
	LogLength int               `json:"log_length"`
	IsDead    bool              `json:"is_dead"`
	Store     map[string]string `json:"store"`
}
// ClusterView represents the snapshot of the entire cluster
type ClusterView struct {
	Nodes []NodeView `json:"nodes"`
}

// NodeHandle holds references to a node instance
type NodeHandle struct {
	ID       int
	RaftNode *raft.Raft
	KVStore  *kv.KVStore
	KillFunc func()
	IsDead   func() bool
}

// HTTPServer manages the web dashboard and REST API
type HTTPServer struct {
	mu    sync.Mutex
	nodes []*NodeHandle
}

// NewHTTPServer creates an HTTP dashboard server
func NewHTTPServer(nodes []*NodeHandle) *HTTPServer {
	return &HTTPServer{
		nodes: nodes,
	}
}

// RegisterRoutes sets up HTTP endpoints
func (s *HTTPServer) RegisterRoutes() {
	http.HandleFunc("/", s.handleIndex)
	http.HandleFunc("/api/cluster", s.handleClusterState)
	http.HandleFunc("/api/execute", s.handleExecute)
	http.HandleFunc("/api/kill", s.handleKill)
}

func (s *HTTPServer) handleClusterState(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var cluster ClusterView
	for _, n := range s.nodes {
		term, isLeader := n.RaftNode.GetState()
		role := "Follower"
		if isLeader {
			role = "Leader"
		}
		if n.IsDead() {
			role = "Crashed"
		}

		cluster.Nodes = append(cluster.Nodes, NodeView{
			ID:        n.ID,
			Role:      role,
			Term:      term,
			LogLength: n.RaftNode.GetLogLength(),
			IsDead:    n.IsDead(),
			Store:     n.KVStore.Snapshot(),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cluster)
}

func (s *HTTPServer) handleExecute(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	cmdStr := r.URL.Query().Get("cmd")
	if cmdStr == "" {
		http.Error(w, "cmd parameter required", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	var leader *NodeHandle
	for _, n := range s.nodes {
		if !n.IsDead() {
			_, isLeader := n.RaftNode.GetState()
			if isLeader {
				leader = n
				break
			}
		}
	}
	s.mu.Unlock()

	if leader == nil {
		http.Error(w, "No active leader available to process command", http.StatusServiceUnavailable)
		return
	}

	index, term, ok := leader.RaftNode.Start(cmdStr)
	if !ok {
		http.Error(w, "Failed to propose command", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":  "submitted",
		"leader":  leader.ID,
		"index":   index,
		"term":    term,
		"command": cmdStr,
	})
}

func (s *HTTPServer) handleKill(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	idStr := r.URL.Query().Get("id")
	var targetId int
	_, err := fmt.Sscanf(idStr, "%d", &targetId)
	if err != nil {
		http.Error(w, "Invalid node id", http.StatusBadRequest)
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, n := range s.nodes {
		if n.ID == targetId {
			n.KillFunc()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"status": "killed",
				"id":     targetId,
			})
			return
		}
	}

	http.Error(w, "Node not found", http.StatusNotFound)
}

func (s *HTTPServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	tmpl := `
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Raft Consensus Distributed KV Dashboard</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    body { background: #0f172a; color: #f8fafc; font-family: ui-sans-serif, system-ui; }
    .card-glass { background: rgba(30, 41, 59, 0.7); backdrop-filter: blur(12px); border: 1px solid rgba(255, 255, 255, 0.1); }
  </style>
</head>
<body class="p-8">
  <div class="max-w-6xl mx-auto">
    <header class="flex justify-between items-center mb-8 border-b border-slate-700 pb-5">
      <div>
        <h1 class="text-3xl font-bold bg-gradient-to-r from-teal-400 to-cyan-500 bg-clip-text text-transparent">
          Raft Consensus Cluster Visualizer
        </h1>
        <p class="text-sm text-slate-400 mt-1">Distributed Fault-Tolerant Key-Value Store in Go</p>
      </div>
      <div class="flex items-center space-x-2">
        <span class="inline-block w-3 h-3 bg-emerald-500 rounded-full animate-ping"></span>
        <span class="text-xs font-semibold text-emerald-400 uppercase tracking-wider">Cluster Live</span>
      </div>
    </header>

    <!-- Controls Section -->
    <div class="card-glass rounded-xl p-6 mb-8 shadow-xl">
      <h2 class="text-lg font-semibold mb-4 text-cyan-300">Client Operations Console</h2>
      <div class="flex gap-4">
        <input id="cmdInput" type="text" placeholder="e.g. SET name=Geeth  or  SET role=Engineer" 
               class="flex-1 bg-slate-900 border border-slate-700 rounded-lg px-4 py-2.5 text-sm focus:outline-none focus:border-cyan-500 text-white" />
        <button onclick="submitCommand()" class="bg-gradient-to-r from-teal-500 to-cyan-600 hover:from-teal-600 hover:to-cyan-700 px-6 py-2.5 rounded-lg text-sm font-semibold transition shadow-md">
          Execute Proposal
        </button>
      </div>
      <div id="cmdStatus" class="mt-2 text-xs text-slate-400 h-4"></div>
    </div>

    <!-- Cluster Nodes Grid -->
    <h2 class="text-xl font-bold mb-4 text-slate-200">Cluster Nodes Status</h2>
    <div id="nodesGrid" class="grid grid-cols-1 md:grid-cols-3 gap-6 mb-8">
      <!-- Injected via JavaScript -->
    </div>
  </div>

  <script>
    async function fetchCluster() {
      try {
        const res = await fetch('/api/cluster');
        const data = await res.json();
        renderNodes(data.nodes);
      } catch (e) {
        console.error(e);
      }
    }

    function renderNodes(nodes) {
      const grid = document.getElementById('nodesGrid');
      grid.innerHTML = '';

      nodes.forEach(node => {
        let badgeColor = "bg-blue-500/20 text-blue-400 border-blue-500/30";
        let cardBorder = "border-slate-700";

        if (node.role === "Leader") {
          badgeColor = "bg-emerald-500/20 text-emerald-400 border-emerald-500/40 shadow-emerald-500/20 shadow-md";
          cardBorder = "border-emerald-500/60 ring-1 ring-emerald-500/30";
        } else if (node.role === "Crashed") {
          badgeColor = "bg-rose-500/20 text-rose-400 border-rose-500/30";
          cardBorder = "border-rose-900/60 opacity-60";
        }

        const storeItems = Object.entries(node.store).map(([k, v]) => 
          '<div class="flex justify-between py-1 border-b border-slate-800 text-xs"><span class="text-slate-400">' + k + '</span><span class="font-mono text-cyan-300 font-medium">' + v + '</span></div>'
        ).join('') || '<div class="text-xs text-slate-500 italic py-1">Store is currently empty</div>';

        const killBtn = node.is_dead 
          ? '<span class="text-xs text-rose-400 font-semibold uppercase">Node Offline</span>'
          : '<button onclick="killNode(' + node.id + ')" class="text-xs bg-rose-600/20 hover:bg-rose-600/40 text-rose-300 border border-rose-500/30 px-3 py-1.5 rounded transition font-medium">💥 Kill Node</button>';

        grid.innerHTML += `
          <div class="card-glass rounded-xl p-6 border ${cardBorder} transition duration-300">
            <div class="flex justify-between items-center mb-4">
              <span class="text-lg font-bold text-white">Node ${node.id}</span>
              <span class="text-xs px-2.5 py-1 rounded-full border font-semibold ${badgeColor}">${node.role}</span>
            </div>

            <div class="space-y-2 mb-5 text-sm">
              <div class="flex justify-between text-slate-400 text-xs">
                <span>Current Term:</span>
                <span class="font-mono font-bold text-white">${node.term}</span>
              </div>
              <div class="flex justify-between text-slate-400 text-xs">
                <span>Committed Log Entries:</span>
                <span class="font-mono font-bold text-white">${node.log_length}</span>
              </div>
            </div>

            <div class="bg-slate-900/80 rounded-lg p-3 border border-slate-800 mb-5">
              <div class="text-[11px] uppercase tracking-wider text-slate-400 font-semibold mb-2">In-Memory State Machine</div>
              <div class="max-h-28 overflow-y-auto pr-1">
                ${storeItems}
              </div>
            </div>

            <div class="flex justify-end pt-2 border-t border-slate-800">
              ${killBtn}
            </div>
          </div>
        `;
      });
    }

    async function submitCommand() {
      const input = document.getElementById('cmdInput');
      const status = document.getElementById('cmdStatus');
      const val = input.value.trim();
      if (!val) return;

      status.innerText = "Proposing to leader...";
      try {
        const res = await fetch('/api/execute?cmd=' + encodeURIComponent(val), { method: 'POST' });
        if (res.ok) {
          const data = await res.json();
          status.innerText = "Propose accepted at Index " + data.index + " by Leader Node " + data.leader;
          input.value = '';
          fetchCluster();
        } else {
          status.innerText = "Error: " + await res.text();
        }
      } catch (e) {
        status.innerText = "Network Error";
      }
    }

    async function killNode(id) {
      if (!confirm("Are you sure you want to simulate crash on Node " + id + "?")) return;
      await fetch('/api/kill?id=' + id, { method: 'POST' });
      fetchCluster();
    }

    // Live polling every 500ms
    setInterval(fetchCluster, 500);
    fetchCluster();
  </script>
</body>
</html>
`
	t, _ := template.New("index").Parse(tmpl)
	t.Execute(w, nil)
}