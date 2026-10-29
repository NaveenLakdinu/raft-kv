package main

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"time"

	raftrpc "raft-kv/internal/rpc"
)

// RaftNode කියන්නේ දැනට අපේ සරල Node එක
type RaftNode struct {
	ID int
}

// RequestVote function එක තමයි අනිත් node එකෙන් call කරන්නේ
func (r *RaftNode) RequestVote(args *raftrpc.RequestVoteArgs, reply *raftrpc.RequestVoteReply) error {
	fmt.Printf("[Node %d] Vote request received from Node %d for Term %d\n", r.ID, args.CandidateId, args.Term)

	// දැනට අපි සරලව ඡන්දය දෙනවා (VoteGranted = true)
	reply.Term = args.Term
	reply.VoteGranted = true

	fmt.Printf("[Node %d] Voted YES for Node %d!\n", r.ID, args.CandidateId)
	return nil
}

func main() {
	// 1. Node 1 (Server එක) විදියට සකස් කරමු
	node1 := &RaftNode{ID: 1}

	// RPC Server එකක් register කරමු
	server := rpc.NewServer()
	server.Register(node1)

	// Port 1234 එකේ listen කරගෙන ඉමු
	listener, err := net.Listen("tcp", ":1234")
	if err != nil {
		log.Fatalf("Listen error: %v", err)
	}
	defer listener.Close()

	// Server එක background එකේ run වෙන්න දාමු (Goroutine)
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go server.ServeConn(conn)
		}
	}()

	fmt.Println("--- Node 1 (Server) Started on port 1234 ---")

	// Server එක start වෙන්න පොඩි වෙලාවක් දෙමු (100ms)
	time.Sleep(100 * time.Millisecond)

	// 2. දැන් Node 2 (Client එක) විදියට Node 1 ට කතා කරමු
	fmt.Println("--- Node 2 (Client) connecting to Node 1 ---")
	client, err := rpc.Dial("tcp", "localhost:1234")
	if err != nil {
		log.Fatalf("Connection failed: %v", err)
	}
	defer client.Close()

	// Node 2 ගේ vote request එක හදමු
	args := raftrpc.RequestVoteArgs{
		Term:         1,
		CandidateId:  2, // මම Node 2, මට ඡන්දය දෙන්න
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	var reply raftrpc.RequestVoteReply

	// Node 1 ගේ RequestVote function එක run කරමු!
	err = client.Call("RaftNode.RequestVote", &args, &reply)
	if err != nil {
		log.Fatalf("RPC call failed: %v", err)
	}

	// ප්‍රතිඵලය බලමු
	fmt.Printf("[Node 2 Result] VoteGranted: %v, Term: %d\n", reply.VoteGranted, reply.Term)
}