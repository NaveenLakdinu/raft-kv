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

// RequestVote RPC handler invoked by a Candidate to gather votes
func (rf *Raft) RequestVote(args *rpc.RequestVoteArgs, reply *rpc.RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return fmt.Errorf("node %d is dead", rf.me)
	}

	// Rule 1: If caller's term is smaller, reject vote
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return nil
	}

	// Rule 2: If caller's term is higher, update currentTerm and step down to Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}

	// Rule 3: Grant vote if we haven't voted for anyone yet in this term
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.lastHeartbeat = time.Now() // Reset election timer when granting vote
		reply.VoteGranted = true
		fmt.Printf("[Node %d] Granted vote to Node %d for Term %d\n", rf.me, args.CandidateId, rf.currentTerm)
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
	return nil
}

// AppendEntries RPC handler invoked by Leader for replication and heartbeats
func (rf *Raft) AppendEntries(args *rpc.AppendEntriesArgs, reply *rpc.AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return fmt.Errorf("node %d is dead", rf.me)
	}

	// Rule 1: Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return nil
	}

	// Rule 2: If term is higher, update term and step down to Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}

	// Valid heartbeat received, reset election timeout
	rf.lastHeartbeat = time.Now()
	reply.Term = rf.currentTerm
	reply.Success = true

	return nil
}

// Start proposes a command to the Raft cluster (used by clients/KV store)
// Returns index, currentTerm, and true if this node is the leader
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// If not the leader, reject the proposal
	if rf.role != Leader || rf.killed() {
		return -1, rf.currentTerm, false
	}