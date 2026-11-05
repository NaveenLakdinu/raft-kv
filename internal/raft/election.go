package raft

import (
	"fmt"
	"net/rpc"
	"sync"
	"time"

	raftrpc "raft-kv/internal/rpc"
)

func (rf *Raft) runElectionTimer() {
	for !rf.killed() {
		timeout := rf.getRandomElectionTimeout()
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

func (rf *Raft) startElection() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()

	currentTerm := rf.currentTerm
	candidateId := rf.me
	peersCount := len(rf.peers)

	fmt.Printf("[Node %d] Started election for Term %d\n", rf.me, rf.currentTerm)

	rf.mu.Unlock()
	go rf.broadcastRequestVote(currentTerm, candidateId, peersCount)
	rf.mu.Lock()
}

func (rf *Raft) broadcastRequestVote(term int, candidateId int, peersCount int) {
	var voteMutex sync.Mutex
	votesReceived := 1
	majority := (peersCount / 2) + 1

	for peerIndex, peerAddr := range rf.peers {
		if peerIndex == candidateId {
			continue
		}

		go func(addr string) {
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				return
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

			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.votedFor = -1
				return
			}

			if reply.VoteGranted && rf.role == Candidate && rf.currentTerm == term {
				voteMutex.Lock()
				votesReceived++
				fmt.Printf("[Node %d] Received vote! Total votes: %d/%d\n", rf.me, votesReceived, majority)

				if votesReceived >= majority && rf.role != Leader {
					rf.role = Leader
					fmt.Printf(">>> [Node %d] ELECTED AS LEADER for Term %d! <<<\n", rf.me, rf.currentTerm)

					for i := range rf.peers {
						rf.nextIndex[i] = len(rf.log) + 1
						rf.matchIndex[i] = 0
					}

					go rf.runHeartbeatTicker()
				}
				voteMutex.Unlock()
			}
		}(peerAddr)
	}
}