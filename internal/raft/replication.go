package raft

import (
	"fmt"
	"net/rpc"
	"time"

	raftrpc "raft-kv/internal/rpc"
)

// runHeartbeatTicker sends periodic heartbeats to all peers while node is Leader
func (rf *Raft) runHeartbeatTicker() {
	for {
		time.Sleep(50 * time.Millisecond)

		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return // Exit goroutine if no longer leader
		}

		currentTerm := rf.currentTerm
		leaderId := rf.me
		rf.mu.Unlock()

		rf.broadcastHeartbeat(currentTerm, leaderId)
	}
}