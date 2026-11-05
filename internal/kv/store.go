package kv

import (
	"fmt"
	"strings"
	"sync"
)

// OpType represents the type of key-value operation
type OpType string

const (
	OpPut    OpType = "PUT"
	OpGet    OpType = "GET"
	OpDelete OpType = "DELETE"
)

// Command represents a client operation to be processed by the state machine
type Command struct {
	Op    OpType // PUT, GET, DELETE
	Key   string
	Value string
}