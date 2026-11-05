package main

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"sync"
	"time"

	"raft-kv/internal/kv"
	"raft-kv/internal/raft"
	raftrpc "raft-kv/internal/rpc"
	"raft-kv/internal/server"
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

func (s *ServerInstance) IsDead() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isDead
}

func (s *ServerInstance) listenApplier() {
	for msg := range s.applyCh {
		if msg.CommandValid {
			rawCmd, ok := msg.Command.(string)
			if ok {
				cmd, err := kv.ParseCommand(rawCmd)
				if err == nil {
					s.kvStore.Apply(cmd)
					fmt.Printf("📦 [Node %d StateMachine] Applied '%s' at Index %d\n",
						s.nodeId, rawCmd, msg.CommandIndex)
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
	fmt.Println("🚀 3-Node Raft Cluster with Interactive Web Dashboard")
	fmt.Println("==================================================")

	nodes := make([]*ServerInstance, len(peers))
	var handles []*server.NodeHandle

	for i := 0; i < len(peers); i++ {
		inst := startNode(i, peers)
		nodes[i] = inst
		handles = append(handles, &server.NodeHandle{
			ID:       inst.nodeId,
			RaftNode: inst.raftNode,
			KVStore:  inst.kvStore,
			KillFunc: inst.Kill,
			IsDead:   inst.IsDead,
		})
		time.Sleep(50 * time.Millisecond)
	}

	// Start Web Dashboard on port 8080
	httpDashboard := server.NewHTTPServer(handles)
	httpDashboard.RegisterRoutes()

	go func() {
		dashboardPort := ":8080"
		fmt.Printf("\n🌐 Interactive Dashboard running at: http://localhost%s\n", dashboardPort)
		if err := http.ListenAndServe(dashboardPort, nil); err != nil {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	// Keep server running indefinitely for user interaction
	select {}
}