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