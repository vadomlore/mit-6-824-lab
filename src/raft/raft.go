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
	"bytes"
	"encoding/gob"
	"log"
	"math"
	"math/rand"
	"strconv"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"

// import "bytes"
// import "../labgob"

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

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

//
// A Go object implementing a single Raft peer.
//

type Role int

const (
	FOLLOWER Role = iota + 1
	CANDIDATE
	LEADER
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// updated on stable storage before responding to RPCs
	currentTerm int        //latest term server has seen (initialized to 0)
	votedFor    string     //candidateId that received vote in current term
	log         []LogEntry //log entries

	role Role // roles

	//volatile state on all servers
	commitIndex int // index of highest log entry known to be committed(init to 0)

	lastApplied int // index of highest log entry known to be applied to state machine(init to 0)

	//volatile state on leaders
	//reinitialized after election
	nextIndex  []int //foreach server, index of the next log entry to send to that server(initialized to leader last log index + 1)
	matchIndex []int //foreach server, index of the highest log entry known to be replicated on server(init to 0)

	lastResponseTime int64 //rpc last response time
	resetTimer       chan bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currentTerm, rf.role == LEADER
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


	rfState := RaftState{}
	d := gob.NewDecoder(bytes.NewBuffer(data))
	if d.Decode(&rfState) != nil {
		log.Fatal("invalid decoding persist data")
	} else {
		rf.currentTerm = rfState.CurrentTerm
		rf.votedFor = rfState.VotedFor
		rf.log = rfState.Log
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int    //candidate's term
	CandidateId  string // candidate requesting vote
	LastLogIndex int    // index of candidate's last log entry
	LastLogTerm  int    // index of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int // currentTerm, for candidate to update itself
	VoterId     string
	VoteGranted bool // true means candidate received vote
}

// compare to see if A's log is more update to b's log
// return true indicates that A's log is at least more update to B's log
func MoreUpdateTo(aLastIndex int, aLastTerm int, bLastIndex int, bLastTerm int) bool {

	if aLastIndex == 0 && bLastIndex == 0 {
		return false
	}
	if aLastIndex == 0 && bLastIndex != 0 {
		return false
	}
	if aLastIndex != 0 && bLastIndex == 0 {
		return true
	}
	if aLastTerm != bLastTerm {
		return aLastTerm-bLastTerm > 0
	}
	if aLastTerm == bLastTerm {
		return aLastIndex-bLastIndex > 0
	}
	return false
}

func GetLogIndicatorBefore(i int, log []LogEntry) []LogEntry {
	if i < 0 {
		i = 0
	}
	return log[0:i]
}

func (rf *Raft) GetLastLogIndicator() (index int, term int) {
	log := GetLogIndicatorBefore(len(rf.log), rf.log)
	if len(log) > 0 {
		return log[len(log)-1].Index, log[len(log)-1].Term
	}
	return 0, 0
}

func (rf *Raft) GetPrevLogIndicator() (index int, term int) {
	log := GetLogIndicatorBefore(len(rf.log)-1, rf.log)
	if len(log) > 0 {
		return log[len(log)-1].Index, log[len(log)-1].Term
	}
	return 0, 0
}

func (rf *Raft) GetPrevLogEntry() LogEntry {
	log := GetLogIndicatorBefore(len(rf.log)-1, rf.log)
	if len(log) > 0 {
		return log[len(log)-1]
	}
	return rf.log[0]
}

func (rf *Raft) GetLastLogEntry() LogEntry {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1]
	}
	return rf.log[0]
}

func (rf *Raft) GetEntryByLogIndex(index int) LogEntry {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == index {
			return rf.log[i]
		}
	}
	return rf.log[0]
}


func (rf *Raft) ExistsEntryWith(index int, term int) bool {
	if rf.GetLastLogEntry().Index < index {
		return false
	}
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == index && rf.log[i].Term == term{
			return true
		}
	}
	return false
}


//CheckConflicts between two logs and return the first conflict position log index
func (rf *Raft) CheckConflicts(b[]LogEntry) (int, int, bool) {
	if rf.GetLastLogEntry().Index == 0 {
		return len(rf.log), 0, false
	}
	if len(b) == 0 {
		return len(rf.log), 0, false
	}
	for j, v := range b {
	next:
		for i:=len(rf.log) - 1; i>=0; i--{
			if rf.log[i].Index > v.Index {
				continue
			} else if rf.log[i].Index == v.Index && rf.log[i].Term != v.Term {
				return i, j, true
			} else if rf.log[i].Index == v.Index && rf.log[i].Term == v.Term {
				continue next
			}
		}
	}
	return len(rf.log), 0, false
}

func (rf *Raft) MergeLog(b []LogEntry) {
	if len(b) <=0 {
		return
	}
	DPrintf("before merge log [%v], entry[%v]", rf.log, b)
	if rf.GetLastLogEntry().Index < b[0].Index {
		rf.log = append(rf.log, b...)
	} else {
		i, j, conflict:=rf.CheckConflicts(b)
		if conflict {
			rf.log = append(rf.log[:i], b[j:]...)
		}
	}
	DPrintf("merge result:%v", rf.log)
}


func (rf *Raft) GetLogStoreIndex(index int) int {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == index {
			return i
		}
	}
	return 0
}


//RPC handler candidate request vote
// !!! import notes: Vote success reset timer or else do nothing and wait for the time to timeout !!!
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	//now := time.Now().UnixNano()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func(r *RequestVoteReply) {
		DPrintf("[peer-%v:%v] give my vote to %v in candidateTerm[%v] with myTerm[%v]===>result:%v.\n", rf.me, rf.GetRoleName(), args.CandidateId, args.Term, rf.currentTerm, r.VoteGranted)
	}(reply)
	DPrintf("peer->%v[%v] give my vote to %v in candidateTerm[%v] with myTerm[%v]?\n", rf.me, rf.GetRoleName(), args.CandidateId, args.Term, rf.currentTerm)

	defer rf.SaveRaftState(RaftState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	})

	//Rule for All servers
	if reply.Term = args.Term; args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
	}

	if args.Term < rf.currentTerm { //request stale
		reply.VoteGranted = false
		return
	}

	index, term := rf.GetLastLogIndicator()

	DPrintf("peer-%v args.Candidate %v LastLogIndex %v, LastLogTerm %v,  args LastLogIndex %v, LastLogTerm %v, argsTerm %v ,currentTerm%v", rf.me, args.CandidateId, index, term, args.LastLogIndex, args.LastLogTerm, args.Term, rf.currentTerm)
	DPrintf("Is more update to %v", MoreUpdateTo(index, term, args.LastLogIndex, args.LastLogTerm))
	DPrintf("already votedFor %v", rf.votedFor)

	now := time.Now().UnixNano()

	if args.Term > rf.currentTerm {
		rf.votedFor = "" //reset vote
		rf.currentTerm = args.Term
		rf.TransformToFollower()

		if (rf.votedFor == "" || rf.votedFor == args.CandidateId) && !MoreUpdateTo(index, term, args.LastLogIndex, args.LastLogTerm) { //? which condition
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			if rf.lastResponseTime < now {
				rf.lastResponseTime = now
			}
		} else {
			reply.VoteGranted = false //already vote
		}
		return
	}
	if args.Term == rf.currentTerm {
		if (rf.votedFor == "" || rf.votedFor == args.CandidateId) && !MoreUpdateTo(index, term, args.LastLogIndex, args.LastLogTerm) { //? which condition
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			if rf.lastResponseTime < now {
				rf.lastResponseTime = now
			}
		} else {
			reply.VoteGranted = false //already vote
		}
		return
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != LEADER {
		isLeader = false
		return index, term, isLeader
	}

	index = rf.GetLastLogEntry().Index + 1
	term = rf.currentTerm

	log := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}

	//DPrintf("Before append command fom peer-%v,  cmd:%v, rfLog %v\n", rf.me, log, rf.log)
	rf.log = append(rf.log, log)
	rf.SaveRaftState(RaftState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	})
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//invoke on leader after a election
func (rf *Raft) ReinitializeIndexes() {
	for i := range rf.nextIndex {
		rf.nextIndex[i] = rf.GetLastLogEntry().Index + 1
	}

	for i := range rf.matchIndex {
		rf.matchIndex[i] = 0
	}
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
	rf.votedFor = ""
	rf.log = append(rf.log, LogEntry{
		Index:   0,
		Term:    0,
		Command: nil})

	rf.role = FOLLOWER
	rf.commitIndex = 0
	rf.lastApplied = 0


	rf.resetTimer = make(chan bool, 1)
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = -1
	}

	rf.lastResponseTime = time.Now().UnixNano()

	// initialize from state persisted before a crash
	gob.Register(RaftState{})
	rf.readPersist(persister.ReadRaftState())

	rf.ElectionLoop()
	rf.ApplierLoop(applyCh)
	return rf
}

type AppendEntriesArgs struct {
	Term     int // leader's term
	LeaderId int // so follower can redirect clients

	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex
	Entries      []LogEntry // log entries to store, first index is 1, //0 remains for zero log indicate
	LeaderCommit int        // leader's commit index

}

type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	now := time.Now().UnixNano()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("peer-%v Receive msg args %#v, myTerm[%v] voted:%v", rf.me, args, rf.currentTerm, rf.votedFor)
	defer rf.SaveRaftState(RaftState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	})


	//If AppendEntries RPC received from new leader: convert to
	//follower
	if rf.role == CANDIDATE {
		rf.TransformToFollower()
	}

	reply.Term = args.Term
	if reply.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
	}


	//if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)

	//reply false if term < currentTerm §5.1
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		DPrintf("%v try to convert  to FOLLOWER", rf.me)
		rf.currentTerm = args.Term
		if rf.lastResponseTime < now {
			rf.lastResponseTime = now
		}
		rf.TransformToFollower()
	} else if args.Term == rf.currentTerm {
		if rf.lastResponseTime < now { //reset heart beat timer
			rf.lastResponseTime = now
		}
	}

	//	DPrintf("AppendEntriesRPCHandler peer-%v: %#v\n, prevLogIndexTerm %v, args.PrevLogTerm %v, starting logs %v", rf.me, args, rf.GetEntryByLogIndex(args.PrevLogIndex).Term, args.PrevLogTerm, rf.log[0:rf.GetLogStoreIndex(args.PrevLogIndex)+1])

	//reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm §5.3
	//follower's log is shorter than leader's log (before append log entry command to local leader) (follower even didn't have that index leader specified)
	// or follower has that index but term diverse (reverse find to avoid log compaction)
	//e.g
	// 2 situations
	// on is prevIndex > max(log).Index then not exists
	//index 1 2 3 4     entries  2 3    args:<prevIndex, prevTerm> (1 1)  so the args(1, 1) exists
	//term  1 1                  4 4
	if !rf.ExistsEntryWith(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Success = false
		return
	} else {
		DPrintf("peer-%v Receive new log: original log[%v], entries [%v] argsPrevLogIndex %v, args.PrevLogTerm %v myLogHere %v", rf.me, rf.log, args.Entries, args.PrevLogIndex, args.PrevLogTerm, rf.GetEntryByLogIndex(args.PrevLogIndex))
		rf.MergeLog(args.Entries)
		DPrintf("peer-%v Receive new log: new log[%v], entries [%v]", rf.me, rf.log, args.Entries)
		reply.Success = true
	}

	//5
	if args.LeaderCommit > rf.commitIndex {
		DPrintf("Try Update commit index rf.commitIndex[%v] args.LeaderCommit[%v] lastEntry.Index %v ", rf.commitIndex, args.LeaderCommit, rf.GetLastLogEntry().Index)
		rf.commitIndex = args.LeaderCommit
		if rf.GetLastLogEntry().Index < args.LeaderCommit {
			rf.commitIndex = rf.GetLastLogEntry().Index
			DPrintf("UpdateCommitIndex [Follower-%v] commitIndex %v\n", rf.me, rf.commitIndex)
		}
	}
	return
}

func (rf *Raft) GetRoleName() string {
	return GetRoleName(rf.role)
}

func GetRoleName(role Role) string {
	switch role {
	case CANDIDATE:
		return "Candiate"
	case FOLLOWER:
		return "Follower"
	case LEADER:
		return "Leader"
	default:
		return "Unknown"
	}
}

func (rf *Raft) DoNextRoundElection() {
	rf.mu.Lock()
	role := rf.role
	var term int
	var candidate string
	if role == LEADER {
		return
		log.Fatalf("invalid transition %v->%v.\n", GetRoleName(LEADER), GetRoleName(CANDIDATE))
	}
	if role == FOLLOWER {
		rf.role = CANDIDATE
		go func() {
			rf.resetTimer <- true
		}()
		DPrintf("peer-%v transition %v->%v. preparing vote.\n", rf.me, GetRoleName(FOLLOWER), GetRoleName(CANDIDATE))
	} else if role == CANDIDATE {
		DPrintf("peer-%v CANDIDATE next election. preparing vote.\n", rf.me)
	}
	rf.currentTerm += 1
	rf.SaveRaftState(RaftState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	})
	term = rf.currentTerm
	rf.lastResponseTime = time.Now().Unix()
	candidate = strconv.Itoa(rf.me)
	var lastLogIndex, lastLogTerm int

	if lastLogIndex, lastLogIndex = 0, 0; len(rf.log) > 0 {
		lastLogIndex, lastLogTerm = rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term
	}

	req := RequestVoteArgs{
		Term:         term,
		CandidateId:  candidate,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	role = rf.role
	majorCount := len(rf.peers)/2 + 1
	totalPeer := len(rf.peers)

	rf.mu.Unlock()
	rf.DoElection(role, &req, majorCount, totalPeer)
}

func (rf *Raft) DoElection(role Role, req *RequestVoteArgs, majorCount int, totalPeer int) {
	//vote for myself
	rf.mu.Lock()
	voteCount := 1
	finished := 1
	rf.votedFor = strconv.Itoa(rf.me)
	rf.SaveRaftState(RaftState{
		CurrentTerm: rf.currentTerm,
		VotedFor:    rf.votedFor,
		Log:         rf.log,
	})
	rf.mu.Unlock()
	//send rpc to request vote from other peer
	cond := sync.NewCond(&rf.mu)

	for iPeer := 0; iPeer < totalPeer; iPeer++ {
		if iPeer != rf.me {
			go func(index int) {
				reply := RequestVoteReply{}
				DPrintf("[role:%v %v]---requestVote--->%v.\n", GetRoleName(role), rf.me, index)
				rf.sendRequestVote(index, req, &reply)
				rf.mu.Lock()
				finished++
				if req.Term == reply.Term && req.Term == rf.currentTerm && reply.VoteGranted {
					voteCount = voteCount + 1
					DPrintf("%v<---vote---%v.\n", rf.me, reply.VoterId)
				} else if rf.currentTerm < reply.Term { //current state stale
					rf.currentTerm = reply.Term
					rf.TransformToFollower()
					rf.SaveRaftState(RaftState{
						CurrentTerm: rf.currentTerm,
						VotedFor:    rf.votedFor,
						Log:         rf.log,
					})
				}
				rf.mu.Unlock()
				cond.Broadcast()
			}(iPeer)
		}
	}
	cond.L.Lock()
	for voteCount < majorCount && finished != totalPeer {
		cond.Wait()
	}
	cond.L.Unlock()

	if voteCount >= majorCount { //majority agree this peer to be leader, and must test role is CANDIDATE
		rf.mu.Lock()
		DPrintf("Election succeed:[Leader-%v] in term [%v]!!!.\n", rf.me, rf.currentTerm)
		if rf.role != CANDIDATE {
			return
			log.Fatalf("peer-%v invalid transition %v->%v.\n", rf.me, GetRoleName(rf.role), GetRoleName(LEADER))
		}
		rf.role = LEADER
		rf.ReinitializeIndexes()
		DPrintf("[Leader %v] nextIndex %v, matchIndex %v,  .\n", rf.me, rf.nextIndex, rf.matchIndex)
		rf.AppendEntriesLoop()
		rf.mu.Unlock()
	}
	if voteCount < majorCount {
		DPrintf("voteCount %v,  split brain (%v) finished %v.\n", voteCount, rf.me, finished)
	}
}

func (rf *Raft) TransformToFollower() {
	if rf.role != FOLLOWER {
		DPrintf("[RoleTransfer] peer-%v Transfer from Role %v to role %v\n", rf.me, rf.GetRoleName(), GetRoleName(FOLLOWER))
		rf.role = FOLLOWER
		rf.ElectionLoop()
	}
}

//// todo candidate transfer to follower should reset electionTimeout?
//func (rf *Raft) TransitionFromCandidateToFollower() {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	if rf.role != CANDIDATE {
//		return
//	}
//	rf.role = FOLLOWER
//}
//
//func (rf *Raft) TransitionFromLeaderToFollower() {
//	rf.mu.Lock()
//	defer rf.mu.Unlock()
//	if rf.role != LEADER {
//		return
//	}
//	oldRole := rf.role
//	rf.role = FOLLOWER
//
//	DPrintf("%v Role Transfer [%v --> %v]\n", rf.me, GetRoleName(oldRole), rf.GetRoleName())
//	rf.ElectionLoop()
//}

func RandPeriod() time.Duration {
	rand.Seed(time.Now().UnixNano())
	rand := 150*time.Millisecond + time.Duration(rand.Intn(150))*time.Millisecond
	return rand
}

//everytime call this method will restart the election process
func (rf *Raft) ElectionLoop() {

	go func() {
		period := RandPeriod()
		DPrintf("%v start election loop period %v.\n", rf.me, period)
		for {
			if rf.killed() {
				return
			}
			if _, isLeader := rf.GetState(); isLeader {
				return
			}

			select {
			case _, ok := <-rf.resetTimer:
				if ok {
					DPrintf("trigger time reset %v start election loop period %v.\n", rf.me, period)
					period = RandPeriod()
				}
			default:
			}

			nano := time.Now().UnixNano()
			if nano-rf.lastResponseTime > period.Nanoseconds() {
				// timeout
				DPrintf("%v election loop timeout.\n", rf.me)
				period = RandPeriod()
				rf.DoNextRoundElection()
			}
			time.Sleep(time.Duration(10) * time.Millisecond)
		}
	}()
}

func (rf *Raft) AppendEntriesLoop() {
	const heartBeatInterval = time.Duration(120) * time.Millisecond
	DPrintf("peer-%v start heart beat with interval %v.\n", rf.me, heartBeatInterval)
	go func() {
		for {
			if _, isLeader := rf.GetState(); !isLeader {
				return
			}
			if rf.killed() {
				return
			}
			rf.AppendLogEntries()
			time.Sleep(heartBeatInterval)
		}
	}()
}

func (rf *Raft) AppendLogEntries() {
	rf.mu.Lock()
	totalPeer := len(rf.peers)
	rf.nextIndex[rf.me] = rf.GetLastLogEntry().Index + 1 //update leader's nextIndex index
	rf.matchIndex[rf.me] = rf.GetLastLogEntry().Index    // update leader's matchIndex
	rf.mu.Unlock()

	for iPeer := 0; iPeer < totalPeer; iPeer++ {
		if iPeer != rf.me {
			go func(index int) {
				rf.RetryableAppendLogEntries(index) // closure should not use iPeer here, otherwise it would produce error
			}(iPeer)
		}
	}
}

func (rf *Raft) RetryableAppendLogEntries(index int) {
	//additionalLogEntry := rf.GetLastLogEntry().Index >= rf.nextIndex[index]
	//prevIndex, prevTerm := rf.GetPrevLogIndicator()
	//term := rf.currentTerm
	//leaderId := rf.me
	//req := AppendEntriesArgs{
	//	Term:         term,
	//	LeaderId:     leaderId,
	//	PrevLogIndex: prevIndex,
	//	PrevLogTerm:  prevTerm,
	//	Entries:      make([]LogEntry, 0),
	//	LeaderCommit: rf.commitIndex,
	//}

	//rf.mu.Unlock()
	//DPrintf("peer-%v send heart-beat to peer-%v in term currentTerm %v\n", rf.me, index, rf.currentTerm)
	//if !additionalLogEntry { //heart beat only
	//	reply := AppendEntriesReply{}
	//	rf.sendAppendEntries(index, &req, &reply) // send AppendEntries RPC
	//	DPrintf("peer-%v recv heart-beat from peer-%v in term [%v] req:%v, res:%v\n", rf.me, index, rf.currentTerm, req, reply)
	//
	//	rf.mu.Lock()
	//	if rf.role != LEADER {
	//		rf.mu.Unlock()
	//		return
	//	}
	//	if reply.Term > rf.currentTerm {
	//		rf.currentTerm = reply.Term
	//		rf.TransformToFollower()
	//	}
	//	rf.mu.Unlock()
	//	return
	//}else {
	for {
		_, isLeader := rf.GetState()
		if !isLeader {
			break
		}
		rf.mu.Lock()
		DPrintf("Suppose[Leader] peer-%v is  with LeaderLog[%v] in [term:%v] send to peer-%v, nextLogIndex[%v] matchIndex[%v]\n", rf.me, rf.log, rf.currentTerm, index, rf.nextIndex, rf.matchIndex)

		req := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.GetEntryByLogIndex(rf.nextIndex[index] - 1).Index,
			PrevLogTerm:  rf.GetEntryByLogIndex(rf.nextIndex[index] - 1).Term,
			Entries:      rf.log[rf.nextIndex[index]:],
			LeaderCommit: rf.commitIndex,
		}

		reply := AppendEntriesReply{}
		rf.mu.Unlock()
		rf.sendAppendEntries(index, &req, &reply) // send AppendEntries RPC
		DPrintf("peer-%v recv reply from peer-%v in term [%v] req:%#v, res:%#v\n", rf.me, index, rf.currentTerm, req, reply)
		rf.mu.Lock()
		if rf.role != LEADER {
			rf.mu.Unlock()
			return
		}
		if reply.Term > rf.currentTerm {
			DPrintf("transfer to follower reply Term %v, currentTerm:%v", reply.Term, rf.currentTerm)
			rf.currentTerm = reply.Term
			rf.SaveRaftState(RaftState{
				CurrentTerm: rf.currentTerm,
				VotedFor:    rf.votedFor,
				Log:         rf.log,
			})
			rf.TransformToFollower()
			rf.mu.Unlock()
			return
		} else if rf.currentTerm != req.Term {
			DPrintf("something odd happened currentTerm %v, replyTerm %v", rf.currentTerm, reply.Term)
			rf.mu.Unlock()
			return
		} else if req.Term == reply.Term && req.Term == rf.currentTerm {
			if reply.Success {
				rf.nextIndex[index] = rf.GetLastLogEntry().Index + 1
				rf.matchIndex[index] = req.PrevLogIndex + len(req.Entries)
				DPrintf("==>reply success peer-%v nextIndex[%v] matchIndex[%v].\n", index, rf.nextIndex[index], rf.matchIndex[index])
				updated := rf.UpdateCommitIndex()
				if updated {
					DPrintf("UpdateCommitIndex [leader] Leader %v, current status log [%v], commitIndex %v, matchIndex %v, nextIndex %v, lastApplied %v", rf.me, rf.log, rf.commitIndex, rf.matchIndex, rf.nextIndex, rf.lastApplied)
				}
			} else {
				if rf.nextIndex[index] >= 1 { //why only contains == 1 here pass the test ?
					rf.nextIndex[index]--
				}
			}
		}
		additionalLogEntry := rf.GetLastLogEntry().Index >= rf.nextIndex[index]
		if !additionalLogEntry {
			rf.mu.Unlock()
			break
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(5) * time.Millisecond)
	}
}

// commitIndex 10    N = 11
// match indexes

//    10   11   12   13   14  logIndex
//s0   2    3    4    4    4
//s1   2    3    4
//s2   2
//rf there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
//and log[N].term == currentTerm:set commitIndex = N (§5.3, §5.4).
func (rf *Raft) UpdateCommitIndex() bool {
	majority := len(rf.peers)/2 + 1
	min, max := minmax(rf.matchIndex)
	N := max
	//DPrintf("begin update commitIndex currentTerm[%v]\n", rf.currentTerm)
	//DPrintf("begin update commitIndex matchIndex[%v]\n", rf.matchIndex)
	//DPrintf("begin update commitIindex log [%v]\n", rf.log)
	//
	//DPrintf("begin update commitIndex min:%v max:%v\n", min, max)
	for {
		DPrintf("iter N:%v\n", N)
		count := 0
		for i := range rf.matchIndex {
			if rf.matchIndex[i] >= N {
				count++
			}
		}

	//	DPrintf("iter count:%v majority:%v nTerm:%v currentTerm:%v \n.", count, majority, rf.GetEntryByLogIndex(N).Term, rf.currentTerm)
		if count >= majority && rf.GetEntryByLogIndex(N).Term == rf.currentTerm {
			old := rf.commitIndex
			rf.commitIndex = N
			DPrintf("peer %v update commitIndex from [%v] to [%v]\n", rf.me, old, rf.commitIndex)
			return true
		} else {
			N--
		}
		if N < min {
			return false
		}
	}
	return false // previous write return false in the inner for loop cause the N iter fail to execute
}

//single go routine to check for apply command
func (rf *Raft) ApplierLoop(applyCh chan ApplyMsg) {
	go func() {
		for !rf.killed() {
			time.Sleep(time.Duration(10) * time.Millisecond)
			rf.mu.Lock()
			if rf.commitIndex > rf.lastApplied {
				for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
					cmd := rf.GetEntryByLogIndex(i)
					msg := ApplyMsg{
						CommandValid: true,
						Command:      cmd.Command,
						CommandIndex: cmd.Index,
					}
					applyCh <- msg
					DPrintf("peer-%v role-%v ApplyMsg %v\n", rf.me, GetRoleName(rf.role), msg)
					rf.lastApplied++
				}
			}
			rf.mu.Unlock()
		}
	}()

}

func minmax(s []int) (int, int) {
	min := math.MaxInt32
	max := math.MinInt32
	for _, v := range s {
		if v < min {
			min = v
		}
		if v > max {
			max = v
		}
	}
	return min, max
}

type RaftState struct {
	CurrentTerm int
	VotedFor string
	Log	[]LogEntry
}


func (raft *Raft) SaveRaftState(raftState RaftState) {
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(raftState)
	data := w.Bytes()
	raft.persister.SaveRaftState(data)
}