package raft

import (
	"sync"
	"time"

	"raft-kv/internal/rpc"
)

// NodeRole represents the role of a node in the Raft cluster
type NodeRole string

const (
	Follower  NodeRole = "Follower"
	Candidate NodeRole = "Candidate"
	Leader    NodeRole = "Leader"
)

// Raft represents a single Raft consensus node
type Raft struct {
	mu        sync.Mutex   // Lock to protect shared access to this peer's state
	peers     []string     // RPC addresses of all peers in the cluster
	me        int          // Index of this node into peers[]
	role      NodeRole     // Current role (Follower, Candidate, or Leader)

	// Persistent state on all servers (updated on stable storage before responding to RPCs)
	currentTerm int          // Latest term server has seen (initialized to 0)
	votedFor    int          // CandidateId that received vote in current term (-1 if none)
	log         []rpc.LogEntry // Log entries; each entry contains command for state machine

	// Volatile state on all servers
	commitIndex int // Index of highest log entry known to be committed
	lastApplied int // Index of highest log entry applied to state machine

	// Election and heartbeat control
	heartbeatInterval time.Duration
	electionTimeout   time.Duration
}

// NewRaft creates and initializes a new Raft node instance
func NewRaft(peers []string, me int) *Raft {
	rf := &Raft{
		peers:             peers,
		me:                me,
		role:              Follower,
		currentTerm:       0,
		votedFor:          -1,
		log:               make([]rpc.LogEntry, 0),
		commitIndex:       0,
		lastApplied:       0,
		heartbeatInterval: 50 * time.Millisecond,
		electionTimeout:   150 * time.Millisecond,
	}

	return rf
}

// GetState returns currentTerm and whether this server believes it is the leader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.role == Leader
}