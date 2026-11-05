package raft

import (
	"fmt"
	"math/rand"
	"net"
	"net/rpc"
	"sync"
	"sync/atomic"
	"time"

	raftrpc "raft-kv/internal/rpc"
)

type NodeRole string

const (
	Follower  NodeRole = "Follower"
	Candidate NodeRole = "Candidate"
	Leader    NodeRole = "Leader"
)

// peerConn holds a cached, persistent RPC client for a single peer address.
// All access is guarded by mu.
type peerConn struct {
	mu   sync.Mutex
	addr string
	cli  *rpc.Client
}

// call dials (or reuses) the persistent connection and invokes the named method.
// On any network error the connection is closed so the next call re-dials.
func (pc *peerConn) call(method string, args, reply interface{}) error {
	pc.mu.Lock()
	defer pc.mu.Unlock()

	// Lazily establish or re-establish the connection.
	if pc.cli == nil {
		conn, err := net.DialTimeout("tcp", pc.addr, 200*time.Millisecond)
		if err != nil {
			return err
		}
		pc.cli = rpc.NewClient(conn)
	}

	err := pc.cli.Call(method, args, reply)
	if err != nil {
		// Treat any error as a broken pipe — discard and re-dial next time.
		pc.cli.Close()
		pc.cli = nil
	}
	return err
}

type Raft struct {
	mu        sync.Mutex
	peers     []string
	peerConns []*peerConn // persistent RPC connections, indexed by peer index
	me        int
	role      NodeRole
	dead      int32
	persister *Persister

	// Persistent state on all servers (saved to disk before responding)
	currentTerm int
	votedFor    int
	log         []raftrpc.LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int
	applyCh     chan raftrpc.ApplyMsg

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	// lastHeartbeat is updated whenever a valid heartbeat/vote-grant is received.
	// Protected by rf.mu.
	lastHeartbeat time.Time
}

// getRandomElectionTimeout returns a uniform random duration in [500ms, 1000ms).
// Using a local rand source avoids the global lock and gives each node a unique seed.
func (rf *Raft) getRandomElectionTimeout() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me)*997))
	return time.Duration(500+r.Intn(500)) * time.Millisecond
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Close all cached peer connections so goroutines unblock quickly.
	for _, pc := range rf.peerConns {
		pc.mu.Lock()
		if pc.cli != nil {
			pc.cli.Close()
			pc.cli = nil
		}
		pc.mu.Unlock()
	}
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// persist writes state to disk atomically. Caller MUST hold rf.mu.
func (rf *Raft) persist() {
	if rf.persister != nil {
		_ = rf.persister.SaveState(rf.currentTerm, rf.votedFor, rf.log)
	}
}

// readPersist restores saved state from disk if present. Called only during init.
func (rf *Raft) readPersist() {
	if rf.persister == nil {
		return
	}
	term, votedFor, savedLog, found := rf.persister.ReadState()
	if found {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = savedLog
		fmt.Printf("💾 [Node %d Recovery] Restored from disk: Term %d, VotedFor %d, Log Entries %d\n",
			rf.me, rf.currentTerm, rf.votedFor, len(rf.log))
	}
}

// NewRaft creates and initializes a new Raft node instance.
func NewRaft(peers []string, me int, applyCh chan raftrpc.ApplyMsg) *Raft {
	persister := NewPersister(me)

	conns := make([]*peerConn, len(peers))
	for i, addr := range peers {
		conns[i] = &peerConn{addr: addr}
	}

	rf := &Raft{
		peers:         peers,
		peerConns:     conns,
		me:            me,
		role:          Follower,
		currentTerm:   0,
		votedFor:      -1,
		log:           make([]raftrpc.LogEntry, 0),
		commitIndex:   0,
		lastApplied:   0,
		applyCh:       applyCh,
		persister:     persister,
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		lastHeartbeat: time.Now(),
	}

	for i := range peers {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	// Restore state from disk before handling traffic.
	rf.readPersist()

	go rf.runElectionTimer()
	go rf.runApplier()

	return rf
}

func (rf *Raft) runApplier() {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		var msgs []raftrpc.ApplyMsg

		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			entry := rf.log[rf.lastApplied-1]
			msgs = append(msgs, raftrpc.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			})
		}
		rf.mu.Unlock()

		// Send committed entries to the state machine outside the lock
		// to avoid blocking Raft goroutines on a slow consumer.
		for _, msg := range msgs {
			rf.applyCh <- msg
		}
	}
}

func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader || rf.killed() {
		return -1, rf.currentTerm, false
	}

	newIndex := len(rf.log) + 1
	newTerm := rf.currentTerm

	entry := raftrpc.LogEntry{
		Index:   newIndex,
		Term:    newTerm,
		Command: command,
	}

	rf.log = append(rf.log, entry)
	rf.matchIndex[rf.me] = newIndex
	rf.nextIndex[rf.me] = newIndex + 1

	// Persist log to disk immediately before acknowledging the client.
	rf.persist()

	fmt.Printf("[Leader %d] Appended entry at Index %d (Persisted to disk): %v\n", rf.me, newIndex, command)

	return newIndex, newTerm, true
}

func (rf *Raft) GetLogLength() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log)
}

// lastLogIndexAndTerm returns the index and term of the last log entry.
// Caller MUST hold rf.mu.
func (rf *Raft) lastLogIndexAndTerm() (int, int) {
	if len(rf.log) == 0 {
		return 0, 0
	}
	last := rf.log[len(rf.log)-1]
	return last.Index, last.Term
}

// RequestVote handles incoming vote requests from candidates.
func (rf *Raft) RequestVote(args *raftrpc.RequestVoteArgs, reply *raftrpc.RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return fmt.Errorf("node %d is dead", rf.me)
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// §5.1: Reject any message from an older term.
	if args.Term < rf.currentTerm {
		return nil
	}

	// §5.1: If we see a higher term, convert to follower immediately.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.persist()
		reply.Term = rf.currentTerm
	}

	// §5.4.1: Grant vote only if we haven't voted yet (or already voted for
	// this candidate) AND the candidate's log is at least as up-to-date as ours.
	myLastIndex, myLastTerm := rf.lastLogIndexAndTerm()
	logOK := (args.LastLogTerm > myLastTerm) ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIndex)

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && logOK {
		rf.votedFor = args.CandidateId
		rf.lastHeartbeat = time.Now() // Reset timer when granting a vote.
		rf.persist()
		reply.VoteGranted = true
		fmt.Printf("[Node %d] Granted vote to Node %d for Term %d\n", rf.me, args.CandidateId, args.Term)
	}

	return nil
}

// AppendEntries handles heartbeats and log replication from the leader.
func (rf *Raft) AppendEntries(args *raftrpc.AppendEntriesArgs, reply *raftrpc.AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	if rf.killed() {
		return fmt.Errorf("node %d is dead", rf.me)
	}

	// §5.1: Reject stale leaders.
	if args.Term < rf.currentTerm {
		return nil
	}

	// §5.1 / §5.2: Valid message from current-or-newer leader — step down if needed.
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	// §5.2: A candidate that receives an AppendEntries from a valid leader must
	// revert to follower, regardless of whether the term is equal or higher.
	rf.role = Follower

	// *** Critical: reset the election timer on EVERY valid heartbeat. ***
	rf.lastHeartbeat = time.Now()
	reply.Term = rf.currentTerm

	// §5.3: Consistency check — verify PrevLogIndex/PrevLogTerm match.
	if args.PrevLogIndex > 0 {
		if len(rf.log) < args.PrevLogIndex {
			// We're missing entries; signal the leader to back up.
			return nil
		}
		if rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			// Log conflict at PrevLogIndex; signal the leader to back up.
			return nil
		}
	}

	// §5.3: Merge incoming entries, resolving conflicts.
	modified := false
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index <= len(rf.log) {
			if rf.log[index-1].Term != entry.Term {
				// Conflict: truncate from this point and append.
				rf.log = rf.log[:index-1]
				rf.log = append(rf.log, entry)
				modified = true
			}
			// Existing entry matches — no action needed.
		} else {
			rf.log = append(rf.log, entry)
			modified = true
		}
	}

	if modified {
		rf.persist()
	}

	// §5.3: Advance commitIndex.
	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := len(rf.log)
		if args.LeaderCommit < lastNewIndex {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewIndex
		}
	}

	if len(args.Entries) > 0 {
		fmt.Printf("[Follower %d] Successfully replicated %d log entries. Total log size: %d\n",
			rf.me, len(args.Entries), len(rf.log))
	}

	reply.Success = true
	return nil
}