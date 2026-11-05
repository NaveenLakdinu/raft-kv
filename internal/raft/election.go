package raft

import (
	"fmt"
	"time"

	raftrpc "raft-kv/internal/rpc"
)

// runElectionTimer is the main election-timeout loop.
// It runs for the lifetime of the node and is the ONLY place that starts elections.
//
// Design notes:
//   - We use a tight 10ms poll loop so the timer fires within ~10ms of its deadline.
//   - The timeout is re-randomized after every election fires to avoid split-vote loops.
//   - The lock is held only for the brief state-read window; RPC calls happen outside.
func (rf *Raft) runElectionTimer() {
	timeout := rf.getRandomElectionTimeout()

	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		rf.mu.Lock()

		// Leaders do not need an election timer.
		if rf.role == Leader {
			rf.mu.Unlock()
			continue
		}

		elapsed := time.Since(rf.lastHeartbeat)
		if elapsed >= timeout {
			fmt.Printf("[Node %d] Election timeout (%dms elapsed, limit %dms) — starting election\n",
				rf.me, elapsed.Milliseconds(), timeout.Milliseconds())

			// startElection runs entirely with rf.mu held and does NOT make
			// any network calls itself — it only spawns goroutines.
			rf.startElection()

			// Re-randomize timeout so the next election window is different.
			timeout = rf.getRandomElectionTimeout()
		}

		rf.mu.Unlock()
	}
}

// startElection converts this node into a Candidate and kicks off a vote broadcast.
// MUST be called with rf.mu held. Does NOT release or re-acquire rf.mu.
func (rf *Raft) startElection() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now() // Reset timer to give the election time to complete.
	rf.persist()

	currentTerm := rf.currentTerm
	candidateId := rf.me

	// Capture log metadata under the lock for the up-to-date check on the peer side.
	lastLogIndex, lastLogTerm := rf.lastLogIndexAndTerm()

	fmt.Printf("[Node %d] Started election for Term %d\n", rf.me, currentTerm)

	// Launch the broadcast in a separate goroutine so we can release rf.mu immediately.
	go rf.broadcastRequestVote(currentTerm, candidateId, lastLogIndex, lastLogTerm)
}

// broadcastRequestVote sends RequestVote RPCs concurrently to all peers.
// It runs entirely WITHOUT rf.mu held; it re-acquires rf.mu only to mutate state.
func (rf *Raft) broadcastRequestVote(term, candidateId, lastLogIndex, lastLogTerm int) {
	// votesNeeded is computed once; we self-vote so start at 1.
	majority := (len(rf.peers) / 2) + 1
	votes := 1          // self-vote
	electedOnce := false // guard so we start the heartbeat ticker exactly once

	type result struct {
		peerIdx int
		reply   raftrpc.RequestVoteReply
		err     error
	}

	resultCh := make(chan result, len(rf.peers))

	for i := range rf.peers {
		if i == candidateId {
			continue
		}
		go func(peerIdx int) {
			args := raftrpc.RequestVoteArgs{
				Term:         term,
				CandidateId:  candidateId,
				LastLogIndex: lastLogIndex,
				LastLogTerm:  lastLogTerm,
			}
			var reply raftrpc.RequestVoteReply
			err := rf.peerConns[peerIdx].call("Raft.RequestVote", &args, &reply)
			resultCh <- result{peerIdx: peerIdx, reply: reply, err: err}
		}(i)
	}

	// Collect replies — stop early once we win or learn we are outdated.
	for i := 0; i < len(rf.peers)-1; i++ {
		res := <-resultCh

		if res.err != nil {
			continue
		}

		rf.mu.Lock()

		// If our term has changed since we started, the election is void.
		if rf.currentTerm != term || rf.role != Candidate {
			rf.mu.Unlock()
			return
		}

		if res.reply.Term > rf.currentTerm {
			// Higher term discovered — step down immediately.
			rf.currentTerm = res.reply.Term
			rf.role = Follower
			rf.votedFor = -1
			rf.lastHeartbeat = time.Now()
			rf.persist()
			rf.mu.Unlock()
			return
		}

		if res.reply.VoteGranted {
			votes++
			fmt.Printf("[Node %d] Received vote from peer %d — total votes: %d/%d\n",
				rf.me, res.peerIdx, votes, majority)

			if votes >= majority && !electedOnce {
				electedOnce = true
				rf.role = Leader
				fmt.Printf(">>> [Node %d] ELECTED AS LEADER for Term %d! <<<\n", rf.me, rf.currentTerm)

				// Reinitialize leader volatile state (§5.3).
				for j := range rf.peers {
					rf.nextIndex[j] = len(rf.log) + 1
					rf.matchIndex[j] = 0
				}

				// Start sending heartbeats immediately — still holding rf.mu so
				// the goroutine will block until we release, which is fine.
				go rf.runHeartbeatTicker()
			}
		}

		rf.mu.Unlock()
	}
}