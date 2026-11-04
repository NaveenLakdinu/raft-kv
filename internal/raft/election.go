package raft

import (
	"fmt"
	"net/rpc"
	"sync"
	"time"

	raftrpc "raft-kv/internal/rpc"
)

// runElectionTimer runs in the background and starts an election if no heartbeat is received
func (rf *Raft) runElectionTimer() {
	for {
		timeout := getRandomElectionTimeout()
		time.Sleep(timeout)

		rf.mu.Lock()
		if rf.role == Leader {
			rf.mu.Unlock()
			continue
		}

		if time.Since(rf.lastHeartbeat) >= timeout {
			fmt.Printf("[Node %d] Election timeout elapsed! Transitioning to Candidate...\n", rf.me)
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}

// startElection converts node into Candidate, increments term, and requests votes
func (rf *Raft) startElection() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()

	currentTerm := rf.currentTerm
	candidateId := rf.me
	peersCount := len(rf.peers)

	fmt.Printf("[Node %d] Started election for Term %d\n", rf.me, rf.currentTerm)

	// Release lock while sending RPCs across the network
	rf.mu.Unlock()
	go rf.broadcastRequestVote(currentTerm, candidateId, peersCount)
	rf.mu.Lock()
}

// broadcastRequestVote sends RequestVote RPCs concurrently to all peers
func (rf *Raft) broadcastRequestVote(term int, candidateId int, peersCount int) {
	var voteMutex sync.Mutex
	votesReceived := 1 // Vote for self
	majority := (peersCount / 2) + 1

	for peerIndex, peerAddr := range rf.peers {
		if peerIndex == candidateId {
			continue // Skip self
		}

		go func(addr string) {
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				return // Peer might be down or unreachable
			}
			defer client.Close()

			args := raftrpc.RequestVoteArgs{
				Term:        term,
				CandidateId: candidateId,
			}
			var reply raftrpc.RequestVoteReply

			err = client.Call("Raft.RequestVote", &args, &reply)
			if err != nil {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			// Check if reply has a higher term (we are obsolete)
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.votedFor = -1
				return
			}

			// If vote granted and we are still running this election
			if reply.VoteGranted && rf.role == Candidate && rf.currentTerm == term {
				voteMutex.Lock()
				votesReceived++
				fmt.Printf("[Node %d] Received vote! Total votes: %d/%d\n", rf.me, votesReceived, majority)

				// Won majority votes -> Become Leader!
				if votesReceived >= majority && rf.role != Leader {
					rf.role = Leader
					fmt.Printf(">>> [Node %d] ELECTED AS LEADER for Term %d! <<<\n", rf.me, rf.currentTerm)
				}
				voteMutex.Unlock()
			}
		}(peerAddr)
	}
}