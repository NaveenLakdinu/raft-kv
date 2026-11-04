package raft

import (
	"fmt"
	"net/rpc"
	"time"

	raftrpc "raft-kv/internal/rpc"
)

// runHeartbeatTicker periodically sends AppendEntries to all peers to maintain leadership & replicate logs
func (rf *Raft) runHeartbeatTicker() {
	for !rf.killed() {
		time.Sleep(50 * time.Millisecond)

		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		rf.broadcastAppendEntries()
	}
}

// broadcastAppendEntries sends entries or heartbeats to each peer concurrently based on nextIndex
func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.role != Leader || rf.killed() {
		rf.mu.Unlock()
		return
	}

	currentTerm := rf.currentTerm
	leaderId := rf.me
	commitIndex := rf.commitIndex
	peersCount := len(rf.peers)
	rf.mu.Unlock()

	for peerIndex, peerAddr := range rf.peers {
		if peerIndex == leaderId {
			continue // Skip self
		}

		go func(peer int, addr string) {
			rf.mu.Lock()
			if rf.role != Leader || rf.killed() {
				rf.mu.Unlock()
				return
			}

			prevLogIndex := rf.nextIndex[peer] - 1
			prevLogTerm := 0
			if prevLogIndex > 0 && prevLogIndex <= len(rf.log) {
				prevLogTerm = rf.log[prevLogIndex-1].Term
			}

			// Get all entries starting from nextIndex[peer]
			var entriesToSend []raftrpc.LogEntry
			if len(rf.log) >= rf.nextIndex[peer] {
				entriesToSend = make([]raftrpc.LogEntry, len(rf.log[rf.nextIndex[peer]-1:]))
				copy(entriesToSend, rf.log[rf.nextIndex[peer]-1:])
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

			var reply raftrpc.AppendEntriesReply
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				return // Peer down or unreachable
			}
			defer client.Close()

			err = client.Call("Raft.AppendEntries", &args, &reply)
			if err != nil {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Check if peer has a higher term
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.votedFor = -1
				return
			}

			if rf.role != Leader || rf.currentTerm != currentTerm {
				return
			}

			if reply.Success {
				// Replicated successfully: update matchIndex and nextIndex
				rf.matchIndex[peer] = args.PrevLogIndex + len(args.Entries)
				rf.nextIndex[peer] = rf.matchIndex[peer] + 1

				// Check if this entry is replicated on majority of nodes to commit
				rf.checkAndAdvanceCommitIndex(peersCount)
			} else {
				// Rejected due to log inconsistency: decrement nextIndex and retry later
				if rf.nextIndex[peer] > 1 {
					rf.nextIndex[peer]--
				}
			}
		}(peerIndex, peerAddr)
	}
}

// checkAndAdvanceCommitIndex commits entries replicated on a majority of nodes
// Note: Raft only commits log entries from the current term by counting replicas (Section 5.4.2)
func (rf *Raft) checkAndAdvanceCommitIndex(peersCount int) {
	majority := (peersCount / 2) + 1

	for n := len(rf.log); n > rf.commitIndex; n-- {
		// Only entries from the current term can be committed by counting replicas
		if rf.log[n-1].Term != rf.currentTerm {
			continue
		}

		matchCount := 1 // Leader itself
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