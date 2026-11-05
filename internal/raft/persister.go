package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"raft-kv/internal/rpc"
)

// PersistentState holds the fields that must survive node crashes
type PersistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []rpc.LogEntry
}

// Persister handles atomic disk serialization of Raft persistent state
type Persister struct {
	mu       sync.Mutex
	filePath string
}

// NewPersister initializes the storage directory and returns a Persister instance
func NewPersister(nodeId int) *Persister {
	dir := "data"
	if err := os.MkdirAll(dir, 0755); err != nil {
		fmt.Printf("[Persister %d] Warning: failed to create data directory: %v\n", nodeId, err)
	}

	return &Persister{
		filePath: filepath.Join(dir, fmt.Sprintf("node_%d.raft", nodeId)),
	}
}