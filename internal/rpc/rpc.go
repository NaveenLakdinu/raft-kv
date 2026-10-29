package rpc

// RequestVoteArgs කියන්නේ ඡන්දය ඉල්ලන කෙනා (Candidate) යවන විස්තර
type RequestVoteArgs struct {
	Term         int // Candidate ගේ වත්මන් term එක (වාරය)
	CandidateId  int // ඡන්දය ඉල්ලන node එකේ ID එක
	LastLogIndex int // Candidate ගේ අන්තිම log entry එකේ index එක
	LastLogTerm  int // Candidate ගේ අන්තිම log entry එකේ term එක
}

// RequestVoteReply කියන්නේ ඡන්දය දෙන කෙනා ආපහු එවන උත්තරය
type RequestVoteReply struct {
	Term        int  // උත්තරය දෙන node එකේ term එක
	VoteGranted bool // ඡන්දය දුන්නද නැද්ද (true / false)
}