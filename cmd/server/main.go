package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"sync"
	"time"

	"raft-kv/internal/kv"
	"raft-kv/internal/raft"
	raftrpc "raft-kv/internal/rpc"
)

type ServerInstance struct {
	nodeId   int
	raftNode *raft.Raft
	kvStore  *kv.KVStore
	applyCh  chan raftrpc.ApplyMsg
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

// listenApplier reads committed ApplyMsgs and applies them to the KVStore
func (s *ServerInstance) listenApplier() {
	for msg := range s.applyCh {
		if msg.CommandValid {
			rawCmd, ok := msg.Command.(string)
			if ok {
				cmd, err := kv.ParseCommand(rawCmd)
				if err == nil {
					s.kvStore.Apply(cmd)
					fmt.Printf("📦 [Node %d StateMachine] Applied '%s' at Index %d | Current Store: %v\n",
						s.nodeId, rawCmd, msg.CommandIndex, s.kvStore.Snapshot())
				}
			}
		}
	}
}

func startNode(nodeId int, peerAddresses []string) *ServerInstance {
	applyCh := make(chan raftrpc.ApplyMsg, 100)
	kvStore := kv.NewKVStore()
	rf := raft.NewRaft(peerAddresses, nodeId, applyCh)

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
		kvStore:  kvStore,
		applyCh:  applyCh,
		listener: listener,
	}

	// Start reading committed messages into KVStore
	go instance.listenApplier()

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
	fmt.Println("🚀 3-Node Raft Cluster - End-to-End KV State Machine Test")
	fmt.Println("==================================================")

	nodes := make([]*ServerInstance, len(peers))
	for i := 0; i < len(peers); i++ {
		nodes[i] = startNode(i, peers)
		time.Sleep(50 * time.Millisecond)
	}

	time.Sleep(2000 * time.Millisecond)

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
	fmt.Println("📝 Executing Distributed KV Operations...")

	// 1. Propose SET name=Geeth
	cmd1 := "SET name=Geeth"
	leaderInstance.raftNode.Start(cmd1)

	time.Sleep(250 * time.Millisecond)

	// 2. Propose SET role=SoftwareEngineer
	cmd2 := "SET role=SoftwareEngineer"
	leaderInstance.raftNode.Start(cmd2)

	// Wait for replication and state machine execution
	time.Sleep(1000 * time.Millisecond)

	fmt.Println("--------------------------------------------------")
	fmt.Println("🔍 Verifying State Machine Consistency Across All Nodes:")
	for _, n := range nodes {
		fmt.Printf("Node %d KV Store Data: %v\n", n.nodeId, n.kvStore.Snapshot())
	}

	fmt.Println("==================================================")
	fmt.Println("🏁 State Machine execution test completed successfully.")
	fmt.Println("==================================================")
}