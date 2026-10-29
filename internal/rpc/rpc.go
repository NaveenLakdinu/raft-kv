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

// LogEntry කියන්නේ Raft log එකේ තැන්පත් වෙන එක record එකක්
type LogEntry struct {
	Index   int         // Log එකේ පිහිටීම (1, 2, 3...)
	Term    int         // මේ entry එක ආපු වෙලාවේ තිබ්බ Term එක
	Command interface{} // Save කරන්න ආපු සත්‍ය data එක (උදා: "SET x=10")
}

// AppendEntriesArgs කියන්නේ Leader විසින් අනිත් nodes වලට යවන විස්තර
type AppendEntriesArgs struct {
	Term         int        // Leader ගේ වත්මන් term එක
	LeaderId     int        // Leader ගේ ID එක (Followers ලාට දැනගන්න)
	PrevLogIndex int        // අලුත් entries වලට කලින් තියෙන log index එක
	PrevLogTerm  int        // PrevLogIndex එකේ තිබ්බ term එක
	Entries      []LogEntry // Save කරන්න ඕන log entries (Heartbeat එකක් නම් මේක හිස්)
	LeaderCommit int        // Leader commit කරපු අන්තිම index එක
}

// AppendEntriesReply කියන්නේ Follower විසින් Leader ට දෙන උත්තරය
type AppendEntriesReply struct {
	Term    int  // Follower ගේ වත්මන් term එක
	Success bool // Leader ගේ log එක follower ගේ log එක එක්ක match වුණා නම් true
}