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

// KVStore represents the in-memory state machine
type KVStore struct {
	mu   sync.RWMutex
	data map[string]string
}

// NewKVStore initializes an empty Key-Value state machine
func NewKVStore() *KVStore {
	return &KVStore{
		data: make(map[string]string),
	}
}

// Apply executes a command against the state machine
func (kv *KVStore) Apply(cmd Command) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch cmd.Op {
	case OpPut:
		kv.data[cmd.Key] = cmd.Value
		return cmd.Value, true

	case OpGet:
		val, exists := kv.data[cmd.Key]
		return val, exists

	case OpDelete:
		_, exists := kv.data[cmd.Key]
		if exists {
			delete(kv.data, cmd.Key)
		}
		return "", exists

	default:
		return "", false
	}
}

// Snapshot returns a shallow copy of the state machine's dataset
func (kv *KVStore) Snapshot() map[string]string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()

	clone := make(map[string]string, len(kv.data))
	for k, v := range kv.data {
		clone[k] = v
	}
	return clone
}