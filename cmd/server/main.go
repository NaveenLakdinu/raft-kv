package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"

	"raft-kv/internal/raft"
)

// ServerInstance tracks network listener and raft node for graceful termination
type ServerInstance struct {
	nodeId   int
	raftNode *raft.Raft
	listener net.Listener
	mu       sync.Mutex
	isDead   bool
}

// Kill stops the RPC listener and marks raft node as dead
func (s *ServerInstance) Kill() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.isDead {
		s.isDead = true
		s.raftNode.Kill() // Terminate Raft goroutines
		if s.listener != nil {
			s.listener.Close()
		}
		fmt.Printf("💥 [Chaos] Node %d has been KILLED!\n", s.nodeId)
	}
}

// startNode initializes an RPC server and returns a pointer to the ServerInstance
func startNode(nodeId int, peerAddresses []string) *ServerInstance {
	rf := raft.NewRaft(peerAddresses, nodeId)

	rpcServer := rpc.NewServer()
	err := rpcServer.Register(rf)
	if err != nil {
		log.Fatalf("Failed to register node %d: %v", nodeId, err)
	}

	port := peerAddresses[nodeId]
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Node %d listen error on %s: %v", nodeId, port, err)
	}

	instance := &ServerInstance{
		nodeId:   nodeId,
		raftNode: rf,
		listener: listener,
	}

	fmt.Printf("[Cluster] Node %d listening on %s\n", nodeId, port)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go rpcServer.ServeConn(conn)
		}
	}()

	return instance
}

func main() {
	peers := []string{
		"127.0.0.1:8001", // Node 0
		"127.0.0.1:8002", // Node 1
		"127.0.0.1:8003", // Node 2
	}

	fmt.Println("==================================================")
	fmt.Println("🚀 Starting 3-Node Raft Cluster with Chaos Testing")
	fmt.Println("==================================================")

	nodes := make([]*ServerInstance, len(peers))
	for i := 0; i < len(peers); i++ {
		nodes[i] = startNode(i, peers)
	}

	// 1. Wait 1.5 seconds for initial leader to be elected
	time.Sleep(1500 * time.Millisecond)

	// 2. Identify the active leader
	var leaderInstance *ServerInstance
	for _, n := range nodes {
		_, isLeader := n.raftNode.GetState()
		if isLeader {
			leaderInstance = n
			break
		}
	}

	if leaderInstance != nil {
		fmt.Printf("\n⚡ Current Active Leader is Node %d. Simulating unexpected failure...\n", leaderInstance.nodeId)
		leaderInstance.Kill()
	}

	// 3. Wait 3.5 seconds to observe re-election among the remaining alive nodes
	time.Sleep(3500 * time.Millisecond)

	fmt.Println("\n==================================================")
	fmt.Println("🏁 Failover test completed.")
	fmt.Println("==================================================")
}