package raft

import (
	"fmt"
	"time"
)

// runElectionTimer runs in the background and starts an election if no heartbeat is received
func (rf *Raft) runElectionTimer() {
	for {
		timeout := getRandomElectionTimeout()
		time.Sleep(timeout)

		rf.mu.Lock()
		// If already a Leader, no need to run election timeout
		if rf.role == Leader {
			rf.mu.Unlock()
			continue
		}

		// Check if election timeout has elapsed since last heartbeat
		if time.Since(rf.lastHeartbeat) >= timeout {
			fmt.Printf("[Node %d] Election timeout elapsed! Transitioning to Candidate...\n", rf.me)
			rf.startElection()
		}
		rf.mu.Unlock()
	}
}