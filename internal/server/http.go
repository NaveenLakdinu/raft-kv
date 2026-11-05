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
