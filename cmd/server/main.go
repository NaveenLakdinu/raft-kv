package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"

	"raft-kv/internal/raft"
)

// startNode initializes an RPC server and starts a Raft node instance
func startNode(nodeId int, peerAddresses []string) {
	// 1. Create a new Raft node instance
	rf := raft.NewRaft(peerAddresses, nodeId)

	// 2. Register with Go's standard RPC server
	rpcServer := rpc.NewServer()
	err := rpcServer.Register(rf)
	if err != nil {
		log.Fatalf("Failed to register node %d: %v", nodeId, err)
	}

	// 3. Listen on assigned TCP port
	port := peerAddresses[nodeId]
	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Node %d listen error on %s: %v", nodeId, port, err)
	}
	defer listener.Close()

	fmt.Printf("[Cluster] Node %d listening on %s\n", nodeId, port)

	// Accept incoming peer connections
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go rpcServer.ServeConn(conn)
	}
}

func main() {
	// Cluster configuration with 3 local nodes
	peers := []string{
		"127.0.0.1:8001", // Node 0
		"127.0.0.1:8002", // Node 1
		"127.0.0.1:8003", // Node 2
	}

	fmt.Println("==================================================")
	fmt.Println("🚀 Starting 3-Node Raft Cluster on Localhost")
	fmt.Println("==================================================")