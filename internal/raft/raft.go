package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
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
	mu    sync.Mutex // Lock to protect shared access to this peer's state
	peers []string   // RPC addresses of all peers in the cluster
	me    int        // Index of this node into peers[]
	role  NodeRole   // Current role (Follower, Candidate, or Leader)
	dead  int32      // Set by Kill() using atomic operations

	// Persistent state on all servers
	currentTerm int            // Latest term server has seen
	votedFor    int            // CandidateId that received vote in current term (-1 if none)
	log         []rpc.LogEntry // Log entries

	// Volatile state on all servers
	commitIndex int // Index of highest log entry known to be committed
	lastApplied int // Index of highest log entry applied to state machine

	// Volatile state on leaders (Reinitialized after election)
	nextIndex  []int // For each server, index of next log entry to send
	matchIndex []int // For each server, index of highest log entry known to be replicated

	// Timer management
	lastHeartbeat time.Time // Time when the last valid RPC was received
}

// getRandomElectionTimeout returns a randomized duration between 250ms and 450ms
func getRandomElectionTimeout() time.Duration {
	return time.Duration(250+rand.Intn(200)) * time.Millisecond
}

// Kill sets the dead flag to stop all background goroutines
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

// killed checks whether this node has been killed
func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// NewRaft creates and initializes a new Raft node instance
func NewRaft(peers []string, me int) *Raft {
	rf := &Raft{
		peers:         peers,
		me:            me,
		role:          Follower,
		currentTerm:   0,
		votedFor:      -1,
		log:           make([]rpc.LogEntry, 0),
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		lastHeartbeat: time.Now(),
	}

	// Start the background election timer goroutine
	go rf.runElectionTimer()

	return rf
}

// GetState returns currentTerm and whether this server believes it is the leader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.role == Leader
}

// Start proposes a command to the Raft cluster
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader || rf.killed() {
		return -1, rf.currentTerm, false
	}

	newIndex := len(rf.log) + 1
	newTerm := rf.currentTerm

	entry := rpc.LogEntry{
		Index:   newIndex,
		Term:    newTerm,
		Command: command,
	}

	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = newIndex
	rf.nextIndex[rf.me] = newIndex + 1

	fmt.Printf("[Leader %d] Appended entry at Index %d: %v\n", rf.me, newIndex, command)

	return newIndex, newTerm, true
}

// GetLogLength returns the total number of log entries currently stored
func (rf *Raft) GetLogLength() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log)
}