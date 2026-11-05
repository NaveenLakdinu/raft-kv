package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"raft-kv/internal/rpc"
)

type NodeRole string

const (
	Follower  NodeRole = "Follower"
	Candidate NodeRole = "Candidate"
	Leader    NodeRole = "Leader"
)

type Raft struct {
	mu    sync.Mutex
	peers []string
	me    int
	role  NodeRole
	dead  int32

	currentTerm int
	votedFor    int
	log         []rpc.LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastHeartbeat time.Time
}

// Ensure unique random seed per node so timers don't clash
func (rf *Raft) getRandomElectionTimeout() time.Duration {
	r := rand.New(rand.NewSource(time.Now().UnixNano() + int64(rf.me*1000)))
	return time.Duration(300+r.Intn(300)) * time.Millisecond
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

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

	for i := range peers {
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	go rf.runElectionTimer()

	return rf
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

func (rf *Raft) GetLogLength() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.log)
}

func (rf *Raft) RequestVote(args *rpc.RequestVoteArgs, reply *rpc.RequestVoteReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		return fmt.Errorf("node %d is dead", rf.me)
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return nil
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.lastHeartbeat = time.Now()
		reply.VoteGranted = true
		fmt.Printf("[Node %d] Granted vote to Node %d for Term %d\n", rf.me, args.CandidateId, rf.currentTerm)
	} else {
		reply.VoteGranted = false
	}

	reply.Term = rf.currentTerm
	return nil
}

func (rf *Raft) AppendEntries(args *rpc.AppendEntriesArgs, reply *rpc.AppendEntriesReply) error {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	if rf.killed() {
		return fmt.Errorf("node %d is dead", rf.me)
	}

	if args.Term < rf.currentTerm {
		return nil
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}

	rf.lastHeartbeat = time.Now()

	if args.PrevLogIndex > 0 {
		if len(rf.log) < args.PrevLogIndex {
			return nil
		}
		if rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
			return nil
		}
	}

	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index <= len(rf.log) {
			if rf.log[index-1].Term != entry.Term {
				rf.log = rf.log[:index-1]
				rf.log = append(rf.log, entry)
			}
		} else {
			rf.log = append(rf.log, entry)
		}
	}

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
	reply.Term = rf.currentTerm
	return nil
}