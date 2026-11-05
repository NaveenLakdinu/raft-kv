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