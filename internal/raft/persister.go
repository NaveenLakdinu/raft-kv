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

// SaveState serializes and atomically writes the state to disk
func (p *Persister) SaveState(term int, votedFor int, logEntries []rpc.LogEntry) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	state := PersistentState{
		CurrentTerm: term,
		VotedFor:    votedFor,
		Log:         logEntries,
	}

	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(state); err != nil {
		return fmt.Errorf("gob encode error: %w", err)
	}
	// Write to disk atomically using WriteFile
	if err := os.WriteFile(p.filePath, buf.Bytes(), 0644); err != nil {
		return fmt.Errorf("failed writing state to disk: %w", err)
	}

	return nil
}

// ReadState reads and restores persistent state from disk
func (p *Persister) ReadState() (int, int, []rpc.LogEntry, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := os.ReadFile(p.filePath)
	if err != nil {