package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

const (
	Candidate = "Candidate"
	Leader    = "Leader"
	Follower  = "Follower"
)

type LogEntry struct {
	Term int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders (reinit after election)
	nextIndex  []int
	matchIndex []int

	state         string
	lastHeardTime time.Time
	timeout time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).

	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == Leader

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// DPrintf("==%d==t%02d==%10s== vote for %d from: %v", rf.me, rf.currentTerm, rf.state, rf.votedFor, args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}

	largerTerm := args.Term > rf.currentTerm
	if largerTerm || rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetTimer()

		if largerTerm {
			rf.currentTerm = args.Term
			rf.state = Follower
		}
		// if candidate’s log is at least as up-to-date as receiver’s log {}
		// DPrintf("==%d==t%02d==%10s==[vote ok][vote for %d] from: %v", rf.me, rf.currentTerm, rf.state, rf.votedFor, args)
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat)
	LeaderCommit int        // leader's commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	reply.Success = false
	// DPrintf("==%d==t%02d==%10s== receive heartbeat: from ==%d==t%02d==whether reset: %t", rf.me, rf.currentTerm, rf.state, args.LeaderId, args.Term, args.Term <= rf.currentTerm)
	if args.Term < rf.currentTerm { // outdated leader
		return
	}
	rf.resetTimer()
	if args.Term > rf.currentTerm {
		rf.state = Follower
		return
	}
	if len(rf.log) < args.PrevLogIndex {
		return
	}
	if args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		return
	}
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i
		if len(rf.log) > index { // have entry in index
			if rf.log[index].Term != entry.Term {
				rf.log[index] = entry
			}
		} else { // add this as new entry
			rf.log = append(rf.log, entry)
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log))
	}
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.state = Follower
	rf.lastHeardTime = time.Now()
	rf.timeout = genElectionTime()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		prevState := rf.state
		for {
			rf.mu.Lock()
			switch rf.state {
			case Candidate:
				if prevState == Follower || (prevState == Candidate && !rf.lastHeardTime.Add(rf.timeout).After(time.Now())) {
					go rf.candidateAction()
				}
			case Follower:
				// when timeout
				if !rf.lastHeardTime.Add(rf.timeout).After(time.Now()) {
					rf.state = Candidate
					rf.mu.Unlock()
					continue
				}
			case Leader:
				if prevState == Candidate {
					go rf.leaderAction()
				}
			}
			prevState = rf.state
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}()

	return rf
}

func (rf *Raft) candidateAction() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.votedFor = rf.me
	args := &RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	vote := 1
	rf.mu.Unlock()
	var wg sync.WaitGroup
	wg.Add(len(rf.peers))
	for i := range rf.peers {
		go func(i int, args *RequestVoteArgs) {
			defer wg.Done()
			if i == rf.me {
				return
			}
			reply := &RequestVoteReply{}
			// DPrintf("==%d==t%02d==%10s== will send RequestVote to %d]", rf.me, rf.currentTerm, rf.state, i)
			rf.mu.Lock()
			isCandidate := rf.state == Candidate 
			rf.mu.Unlock()
			if !isCandidate {
				return
			}
			rf.sendRequestVote(i, args, reply)

			// handle reply
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.state = Follower
				return
			}
			if reply.VoteGranted && rf.state == Candidate {
				vote++
			}
		}(i, args)
	}
	wg.Wait()

	// DPrintf("rf.state: %s, vote: %d", rf.state, vote)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Candidate && vote > len(rf.peers)/2 {
		rf.state = Leader
	}
}

func (rf *Raft) leaderAction() {
	go rf.heartbeat()
}

func (rf *Raft) heartbeat() {
	for {
		rf.mu.Lock()
		isLeader := rf.state == Leader
		rf.mu.Unlock()
		if !isLeader {
			break
		}
		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
		}

		for i := range rf.peers {
			go func(i int, args *AppendEntriesArgs) {
				if i == rf.me {
					return
				}
				reply := &AppendEntriesReply{}
				rf.mu.Lock()
				isLeader := rf.state == Leader 
				rf.mu.Unlock()
				if !isLeader {
					return
				}
				rf.sendAppendEntries(i, args, reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.resetTimer()
				}
			}(i, args)
		}
		time.Sleep(genHeartbeatTime())
	}
}

func (rf *Raft) resetTimer() {
	rf.lastHeardTime = time.Now()
	rf.timeout = genElectionTime() 
}

func genElectionTime() time.Duration {
	return randTime(300, 500)
}

func genHeartbeatTime() time.Duration {
	return randTime(150, 250)
}

func randTime(min, max int) time.Duration {
	return time.Duration(randBetween(min, max)) * time.Millisecond
}

func randBetween(lo, hi int) int {
	return lo + rand.Intn(hi-lo)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
