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

type ServerInstance struct {
	nodeId   int
	raftNode *raft.Raft
	listener net.Listener
	mu       sync.Mutex
	isDead   bool
}

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
		"127.0.0.1:8001",
		"127.0.0.1:8002",
		"127.0.0.1:8003",
	}

	fmt.Println("==================================================")
	fmt.Println("🚀 Starting 3-Node Raft Cluster - Log Replication Test")
	fmt.Println("==================================================")

	nodes := make([]*ServerInstance, len(peers))
	for i := 0; i < len(peers); i++ {
		nodes[i] = startNode(i, peers)
		time.Sleep(50 * time.Millisecond) // Give network listeners brief headstart
	}

	// 1. Wait for leader election to settle
	time.Sleep(2000 * time.Millisecond)

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

	// 5. Wait for replication rounds to complete
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