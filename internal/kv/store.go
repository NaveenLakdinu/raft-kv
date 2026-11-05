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