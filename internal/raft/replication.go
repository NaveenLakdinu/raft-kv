package raft

import (
	"fmt"
	"time"

	raftrpc "raft-kv/internal/rpc"
)

// runHeartbeatTicker is started exactly once when this node becomes Leader.
// It fires at ~60ms intervals (well below the 500ms election timeout floor)
// and exits as soon as the node loses leadership.
func (rf *Raft) runHeartbeatTicker() {
	// Send the first heartbeat immediately so followers reset their timers
	// before any of them can time out.
	rf.broadcastAppendEntries()

	ticker := time.NewTicker(60 * time.Millisecond)
	defer ticker.Stop()

	for !rf.killed() {
		<-ticker.C

		rf.mu.Lock()
		isLeader := rf.role == Leader
		rf.mu.Unlock()

		if !isLeader {
			return
		}

		rf.broadcastAppendEntries()
	}
}

// broadcastAppendEntries sends AppendEntries (heartbeat or replication) to every
// peer concurrently. All state is captured under rf.mu and then released before
// any network I/O, satisfying the "no RPC under lock" requirement.
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.role != Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}

	// Snapshot the leader's immutable-for-this-round state.
	currentTerm := rf.currentTerm
	leaderId := rf.me
	commitIndex := rf.commitIndex
	logLen := len(rf.log)
	rf.mu.Unlock()

	for peerIndex := range rf.peers {
		if peerIndex == leaderId {
			continue
		}

		go func(peer int) {
			// --- lock window 1: build the args ---
			rf.mu.Lock()
			if rf.role != Leader || rf.killed() {
				rf.mu.Unlock()
				return
			}

			prevLogIndex := rf.nextIndex[peer] - 1
			prevLogTerm := 0
			if prevLogIndex > 0 && prevLogIndex <= logLen {
				prevLogTerm = rf.log[prevLogIndex-1].Term
			}

			// Copy only the slice we need to avoid holding a reference into
			// rf.log across the network call.
			var entriesToSend []raftrpc.LogEntry
			if logLen >= rf.nextIndex[peer] {
				slice := rf.log[rf.nextIndex[peer]-1:]
				entriesToSend = make([]raftrpc.LogEntry, len(slice))
				copy(entriesToSend, slice)
			}

			args := raftrpc.AppendEntriesArgs{
				Term:         currentTerm,
				LeaderId:     leaderId,
				PrevLogIndex: prevLogIndex,
				PrevLogTerm:  prevLogTerm,
				Entries:      entriesToSend,
				LeaderCommit: commitIndex,
			}
			rf.mu.Unlock()
			// --- end lock window 1 ---

			// Network call happens with NO lock held.
			var reply raftrpc.AppendEntriesReply
			err := rf.peerConns[peer].call("Raft.AppendEntries", &args, &reply)
			if err != nil {
				return
			}

			// --- lock window 2: process the reply ---
			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Stale reply: our term or role changed while the RPC was in flight.
			if rf.role != Leader || rf.currentTerm != currentTerm {
				return
			}

			if reply.Term > rf.currentTerm {
				// Discovered a higher term — step down.
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.votedFor = -1
				rf.lastHeartbeat = time.Now()
				rf.persist()
				return
			}

			if reply.Success {
				// Advance matchIndex/nextIndex monotonically (never decrease).
				newMatch := args.PrevLogIndex + len(args.Entries)
				if newMatch > rf.matchIndex[peer] {
					rf.matchIndex[peer] = newMatch
					rf.nextIndex[peer] = newMatch + 1
				}
				rf.checkAndAdvanceCommitIndex()
			} else {
				// Consistency check failed — back up nextIndex by one and retry
				// on the next heartbeat round.
				if rf.nextIndex[peer] > 1 {
					rf.nextIndex[peer]--
				}
			}
			// --- end lock window 2 ---
		}(peerIndex)
	}
}

// checkAndAdvanceCommitIndex scans from the end of the log toward commitIndex
// looking for the highest N such that a majority have matchIndex >= N and
// log[N-1].Term == currentTerm (§5.4.2).
// MUST be called with rf.mu held.
func (rf *Raft) checkAndAdvanceCommitIndex() {
	majority := (len(rf.peers) / 2) + 1

	for n := len(rf.log); n > rf.commitIndex; n-- {
		if rf.log[n-1].Term != rf.currentTerm {
			// Never commit entries from previous terms by counting replicas alone.
			continue
		}

		matchCount := 1 // leader itself
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= n {
				matchCount++
			}
		}

		if matchCount >= majority {
			rf.commitIndex = n
			fmt.Printf("🎯 [Leader %d] Log Entry at Index %d COMMITTED by majority (%d/%d)!\n",
				rf.me, n, matchCount, majority)
			break
		}
	}
}