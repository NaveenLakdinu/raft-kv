package raft

import (
	"fmt"
	"net/rpc"
	"time"

	raftrpc "raft-kv/internal/rpc"
)

// runHeartbeatTicker sends periodic heartbeats to all peers while node is Leader
func (rf *Raft) runHeartbeatTicker() {
	for !rf.killed() {
		time.Sleep(50 * time.Millisecond)

		rf.mu.Lock()
		if rf.role != Leader {
			rf.mu.Unlock()
			return
		}

		currentTerm := rf.currentTerm
		leaderId := rf.me
		rf.mu.Unlock()

		rf.broadcastHeartbeat(currentTerm, leaderId)
	}
}

// broadcastHeartbeat sends AppendEntries RPC (heartbeat) to all peers concurrently
func (rf *Raft) broadcastHeartbeat(term int, leaderId int) {
	for peerIndex, peerAddr := range rf.peers {
		if peerIndex == leaderId {
			continue // Skip self
		}

		go func(addr string) {
			client, err := rpc.Dial("tcp", addr)
			if err != nil {
				return // Peer is down or unreachable
			}
			defer client.Close()

			args := raftrpc.AppendEntriesArgs{
				Term:     term,
				LeaderId: leaderId,
				Entries:  nil,
			}
			var reply raftrpc.AppendEntriesReply

			err = client.Call("Raft.AppendEntries", &args, &reply)
			if err != nil {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()

			if reply.Term > rf.currentTerm {
				fmt.Printf("[Node %d] Discovered higher term %d from peer. Stepping down to Follower.\n", rf.me, reply.Term)
				rf.currentTerm = reply.Term
				rf.role = Follower
				rf.votedFor = -1
			}
		}(peerAddr)
	}
}