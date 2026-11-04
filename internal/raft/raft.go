package raft

import (
	"sync"
	"time"

	"raft-kv/internal/rpc"
)

// NodeRole represents the role of a node in the Raft cluster
type NodeRole string

const (
	Follower  NodeRole = "Follower"
	Candidate NodeRole = "Candidate"
	Leader    NodeRole = "Leader"
)