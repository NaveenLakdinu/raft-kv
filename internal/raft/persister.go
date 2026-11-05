package raft

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"raft-kv/internal/rpc"
)

// PersistentState holds the fields that must survive node crashes
type PersistentState struct {
	CurrentTerm int
	VotedFor    int
	Log         []rpc.LogEntry
}