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