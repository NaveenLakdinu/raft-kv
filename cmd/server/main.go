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

// ServerInstance tracks network listener and raft node
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
		s.raftNode.Kill()
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
	fmt.Println("🚀 Starting 3-Node Raft Cluster - Log Replication Test")
	fmt.Println("==================================================")

	nodes := make([]*ServerInstance, len(peers))
	for i := 0; i < len(peers); i++ {
		nodes[i] = startNode(i, peers)
	}

	// 1. Wait 1.5 seconds for leader election to settle
	time.Sleep(1500 * time.Millisecond)

	// 2. Locate the active leader
	var leaderInstance *ServerInstance
	for _, n := range nodes {
		_, isLeader := n.raftNode.GetState()
		if isLeader {
			leaderInstance = n
			break
		}
	}

	if leaderInstance == nil {
		fmt.Println("❌ No leader was elected in time.")
		return
	}

	fmt.Printf("\n⭐ Discovered Leader: Node %d\n", leaderInstance.nodeId)
	fmt.Println("--------------------------------------------------")
	fmt.Println("📝 Proposing Client Commands to the Cluster...")

	// 3. Propose Command 1
	cmd1 := "SET name=Geeth"
	index1, term1, ok1 := leaderInstance.raftNode.Start(cmd1)
	if ok1 {
		fmt.Printf("Client -> Submitted: '%s' (Assigned Index: %d, Term: %d)\n", cmd1, index1, term1)
	}

	time.Sleep(200 * time.Millisecond)

	// 4. Propose Command 2
	cmd2 := "SET project=RaftKV"
	index2, term2, ok2 := leaderInstance.raftNode.Start(cmd2)
	if ok2 {
		fmt.Printf("Client -> Submitted: '%s' (Assigned Index: %d, Term: %d)\n", cmd2, index2, term2)
	}

	// 5. Wait 1 second for heartbeats and replication rounds to complete
	time.Sleep(1000 * time.Millisecond)

	fmt.Println("--------------------------------------------------")
	fmt.Println("🔍 Verifying Cluster Log Status:")
	for _, n := range nodes {
		fmt.Printf("Node %d Log Entries Count: %d\n", n.nodeId, n.raftNode.GetLogLength())
	}

	fmt.Println("==================================================")
	fmt.Println("🏁 Log Replication test completed successfully.")
	fmt.Println("==================================================")
}