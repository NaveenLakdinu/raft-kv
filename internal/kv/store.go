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

// ParseCommand parses a raw string command like "SET key=val", "GET key", or "DEL key"
func ParseCommand(raw string) (Command, error) {
	parts := strings.SplitN(strings.TrimSpace(raw), " ", 2)
	if len(parts) == 0 || parts[0] == "" {
		return Command{}, fmt.Errorf("empty command")
	}

	opStr := strings.ToUpper(parts[0])

	switch opStr {
	case "SET", "PUT":
		if len(parts) < 2 {
			return Command{}, fmt.Errorf("malformed SET command, expected: SET key=value")
		}
		kvPair := strings.SplitN(parts[1], "=", 2)
		if len(kvPair) < 2 {
			return Command{}, fmt.Errorf("malformed SET pair, expected: key=value")
		}
		return Command{
			Op:    OpPut,
			Key:   strings.TrimSpace(kvPair[0]),
			Value: strings.TrimSpace(kvPair[1]),
		}, nil

	case "GET":
		if len(parts) < 2 {
			return Command{}, fmt.Errorf("malformed GET command, expected: GET key")
		}
		return Command{
			Op:  OpGet,
			Key: strings.TrimSpace(parts[1]),
		}, nil

	case "DEL", "DELETE":
		if len(parts) < 2 {
			return Command{}, fmt.Errorf("malformed DEL command, expected: DEL key")
		}
		return Command{
			Op:  OpDelete,
			Key: strings.TrimSpace(parts[1]),
		}, nil

	default:
		return Command{}, fmt.Errorf("unsupported operation: %s", opStr)
	}
}
