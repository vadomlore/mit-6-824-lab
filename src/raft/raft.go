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
// return >=0 indicates that A's log is at least more update to B's log
func CompareLogUpdateToDate(aLastIndex int, aLastTerm int, bLastIndex int, bLastTerm int) int {
	if aLastIndex == 0 && bLastIndex == 0 {
		return 0
	}
	if aLastIndex == 0 && bLastIndex != 0 {
		return -1
	}
	if aLastIndex != 0 && bLastIndex == 0 {
		return 1
	}
	if aLastTerm != bLastTerm {
		return aLastTerm - bLastTerm
	}
	if aLastTerm != bLastTerm {
		return aLastIndex - bLastIndex
	}
	return 0
}

func GetLogIndicatorBefore(i int, log []LogEntry) []LogEntry {
	if i < 0 {
		i = 0
	}
	return log[0: i]
}

func (rf *Raft) GetLastLogIndicator() (index int, term int) {
	log := GetLogIndicatorBefore(len(rf.log), rf.log)
	if len(log) > 0 {
		return log[len(log) - 1].Index, log[len(log) - 1].Term
	}
	return 0, 0
}

func (rf *Raft) GetPrevLogIndicator() (index int, term int)  {
	log := GetLogIndicatorBefore(len(rf.log)-1, rf.log)
	if len(log) > 0 {
		return log[len(log) - 1].Index, log[len(log) - 1].Term
	}
	return 0, 0
}

func (rf *Raft) GetPrevLogEntry() LogEntry  {
	log := GetLogIndicatorBefore(len(rf.log)-1, rf.log)
	if len(log) > 0 {
		return log[len(log) - 1]
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

func (rf *Raft) GetLogStoreIndex(index int) int {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == index {
			return i
		}
	}
	return 0
}

func (rf *Raft) AppendLog(appendLog []LogEntry) {
	rf.log = append(rf.log, appendLog...)
}

//
//RPC handler candidate request vote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	now := time.Now().UnixNano()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer func(r *RequestVoteReply) {
		DPrintf("[peer-%v:%v] give my vote to %v in candidateTerm[%v] with myTerm[%v]===>result:%v.\n", rf.me, rf.GetRoleName(), args.CandidateId, args.Term, rf.currentTerm, r.VoteGranted)
	}(reply)
	DPrintf("peer->%v[%v] give my vote to %v in candidateTerm[%v] with myTerm[%v]?\n", rf.me, rf.GetRoleName(), args.CandidateId, args.Term, rf.currentTerm)
	//Rule for All servers
	if reply.Term = args.Term; args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
	}

	if args.Term > rf.currentTerm {
		rf.votedFor = "" //reset vote
		//if (rf.votedFor == "" ) || (rf.votedFor == args.CandidateId && CompareLogUpdateToDate(args.LastLogIndex, args.LastLogTerm, index, term) >= 0) { ? // why it's wrong here to add this condition
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true

		if rf.lastResponseTime < now {
			rf.lastResponseTime = now
		}

		if rf.role != FOLLOWER {
			rf.role = FOLLOWER
			rf.ElectionLoop()
		}
		return
	}

	if args.Term < rf.currentTerm { //request stale
		reply.VoteGranted = false
		return
	}

	index, term := rf.GetLastLogIndicator()
	if args.Term == rf.currentTerm {
		//if (rf.votedFor == "") || (rf.votedFor == args.CandidateId && (CompareLogUpdateToDate(args.LastLogIndex, args.LastLogTerm, index, term)) >= 0) { //? which condition
		if (rf.votedFor == "" || rf.votedFor == args.CandidateId) && CompareLogUpdateToDate(args.LastLogIndex, args.LastLogTerm, index, term) >= 0 { //? which condition
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

	DPrintf("add log %v\n", log)

	rf.log = append(rf.log, log)
	DPrintf("rfLog %v\n", rf.log)
	//prevLogIndex, pervLogTerm := rf.GetPrevLogIndicator()
	//
	//for i := range rf.nextIndex {
	//	rf.nextIndex[i] = rf.GetLastLogEntry().Index + 1
	//}
	//DPrintf("Start command\n")
	//
	//rf.appendEntriesCh <- AppendEntriesArgs{
	//	Term:         rf.currentTerm,
	//	LeaderId:     rf.me,
	//	PrevLogIndex: prevLogIndex,
	//	PrevLogTerm:  pervLogTerm,
	//	Entries:      rf.log[rf.nextIndex[i]:],
	//	LeaderCommit: rf.commitIndex,
	//}

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
	rf.role = FOLLOWER
	rf.commitIndex = 0
	rf.lastApplied = 0

	//load raft log
	rf.log = append(rf.log, LogEntry{
		Index:   0,
		Term:    0,
		Command: nil})

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(rf.matchIndex); i++ {
		rf.matchIndex[i] = -1
	}

	rf.lastResponseTime = time.Now().UnixNano()
	rf.ElectionLoop()
	rf.ApplierLoop(applyCh)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
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

	if reply.Term = args.Term; args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
	}
	//if an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)

	//reply false if term < currentTerm §5.1
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		if rf.lastResponseTime < now {
			rf.lastResponseTime = now
		}
		if rf.role != FOLLOWER {
			rf.role = FOLLOWER
			rf.ElectionLoop()
		}
	} else if args.Term == rf.currentTerm {
		if rf.lastResponseTime < now { //reset heart beat timer
			rf.lastResponseTime = now
		}
	}

	DPrintf("AppendEntriesRPC: %#v\n, prevLogIndexTerm %v, args.PrevLogTerm %v, starting logs %v", args,
		rf.GetEntryByLogIndex(args.PrevLogIndex).Term, args.PrevLogTerm,
		rf.log[0:rf.GetLogStoreIndex(args.PrevLogIndex)+1])

	//reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm §5.3
	//follower's log is shorter than leader's log (before append log entry command to local leader) (follower even didn't have that index leader specified)
	// or follower has that index but term diverse (reverse find to avoid log compaction)
	if rf.GetLastLogEntry().Index < args.PrevLogIndex {
		reply.Success = false
		return
	} else if rf.GetEntryByLogIndex(args.PrevLogIndex).Term != args.PrevLogTerm {
		//if an existing entry conflicts with a new one (same index but different terms),
		//delete the existing entry and all that follow it (§5.3
		rf.log = rf.log[0:rf.GetLogStoreIndex(args.PrevLogIndex)]
		reply.Success = false
	} else if rf.GetEntryByLogIndex(args.PrevLogIndex).Term == args.PrevLogTerm {
		rf.log = append(rf.log[0:rf.GetLogStoreIndex(args.PrevLogIndex)+1], args.Entries...)

		DPrintf("follower %v current log %v  args.LeaderCommit[%v]  rf.commitIndex[%v] rf.GetLastLogEntry().Index[%v]\n", rf.me, rf.log,
			args.LeaderCommit,rf.commitIndex, rf.GetLastLogEntry().Index)
		reply.Success = true
	}
	//5
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		if rf.GetLastLogEntry().Index < args.LeaderCommit {
			rf.commitIndex = rf.GetLastLogEntry().Index
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
		log.Fatalf("invalid transition %v->%v.\n", GetRoleName(LEADER), GetRoleName(CANDIDATE))
	}
	if role == FOLLOWER {
		rf.role = CANDIDATE
		DPrintf("peer-%v transition %v->%v. preparing vote.\n", rf.me, GetRoleName(FOLLOWER), GetRoleName(CANDIDATE))
	} else if role == CANDIDATE {
		DPrintf("peer-%v CANDIDATE next election. preparing vote.\n", rf.me)
	}
	rf.currentTerm += 1
	term = rf.currentTerm
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
					rf.role = FOLLOWER
					rf.ElectionLoop()
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
		DPrintf("majority voteCount %v,  %v->[Leader] finshedCount [%v] .\n", voteCount, rf.me, finished)
		if rf.role != CANDIDATE {
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

			nano := time.Now().UnixNano()
			if nano-rf.lastResponseTime > period.Nanoseconds() {
				// timeout
				DPrintf("%v election loop timeout.\n", rf.me)
				period = RandPeriod()
				rf.DoNextRoundElection()
			}
			time.Sleep(time.Duration(20) * time.Millisecond)
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

	//DPrintf("Start rf.AppendLogEntries()")
	//var req AppendEntriesArgs
	//DPrintf("do append log entries...........")
	//select {
	//case ok := <-rf.receivedCmd:
	//	rf.mu.Lock()
	//	if ok {
	//		DPrintf("receive command from application %v.\n")
	//	}
	//	term := rf.currentTerm
	//	leaderId := rf.me
	//	prevLogIndex, prevLogTerm := rf.GetPrevLogIndicator()
	//	req = AppendEntriesArgs{
	//		Term:         term,
	//		LeaderId:     leaderId,
	//		PrevLogIndex: prevLogIndex,
	//		PrevLogTerm:  prevLogTerm,
	//		LeaderCommit: rf.commitIndex,
	//	}
	//	rf.mu.Unlock()
	//default:
	//	rf.mu.Lock()
	//	prevIndex, prevTerm := rf.GetPrevLogIndicator()
	//	term := rf.currentTerm
	//	leaderId := rf.me
	//	req = AppendEntriesArgs{
	//		Term:         term,
	//		LeaderId:     leaderId,
	//		PrevLogIndex: prevIndex,
	//		PrevLogTerm:  prevTerm,
	//		Entries:      make([]LogEntry, 0),
	//		LeaderCommit: rf.commitIndex,
	//	}
	//	rf.mu.Unlock()
	//	DPrintf("No command, heart beat sync command.\n")
	//}

	rf.mu.Lock()
	totalPeer := len(rf.peers)
	rf.mu.Unlock()

	for iPeer := 0; iPeer < totalPeer; iPeer++ {
		if iPeer != rf.me {
			go func(index int) {
				//DPrintf("total peer counts %v\n", totalPeer)
				//DPrintf("heart beat for peer-%v\n", index)
				rf.RetryableAppendLogEntries(index) // closure should not use iPeer here, otherwise it would produce error
			}(iPeer)
		}
	}
}


func (rf *Raft) RetryableAppendLogEntries(index int) {
	rf.mu.Lock()
	additionalLogEntry := rf.GetLastLogEntry().Index >= rf.nextIndex[index]
	prevIndex, prevTerm := rf.GetPrevLogIndicator()
	term := rf.currentTerm
	leaderId := rf.me
	req := AppendEntriesArgs{
		Term:         term,
		LeaderId:     leaderId,
		PrevLogIndex: prevIndex,
		PrevLogTerm:  prevTerm,
		Entries:      make([]LogEntry, 0),
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	if !additionalLogEntry { //heart beat only
		reply := AppendEntriesReply{}
		rf.sendAppendEntries(index, &req, &reply) // send AppendEntries RPC
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.role = FOLLOWER
			rf.ElectionLoop()
		}
		rf.mu.Unlock()
		return
	}
	for {
		time.Sleep(time.Duration(5*time.Microsecond))
		rf.mu.Lock()
		additionalLogEntry := rf.GetLastLogEntry().Index >= rf.nextIndex[index]
		if !additionalLogEntry {
			rf.mu.Unlock()
			break
		}

		if additionalLogEntry {
			//DPrintf("rf.log %v, peer-%v,  prevLogEntry[%v], lastLogEntry[%v], nextIndex[%v]\n", rf.log, index, rf.GetPrevLogEntry(), rf.GetLastLogEntry(), rf.nextIndex[index])
			DPrintf("peer %v , rfLog [%v], rf.log LastLogEntry-%v,  nextLogIndex[%v] matchIndex[%v]", index, rf.log, rf.GetLastLogEntry(), rf.nextIndex[index], rf.matchIndex)

			DPrintf("Peer-%v LastIndex %v, NextIndex %v\n", index, rf.GetLastLogEntry().Index, rf.nextIndex[index])

			req := AppendEntriesArgs{
				Term:         term,
				LeaderId:     leaderId,
				PrevLogIndex: rf.GetEntryByLogIndex(rf.nextIndex[index] - 1).Index,
				PrevLogTerm:  rf.GetEntryByLogIndex(rf.nextIndex[index] - 1).Term,
				Entries:      rf.log[rf.nextIndex[index]:],
				LeaderCommit: rf.commitIndex,
			}
			//req.Entries = rf.log[rf.nextIndex[index]:] // get the just filled command fill in the nextIndex slot or all the back off log slices if log conflict
			//
			////back off to find the match log
			//req.PrevLogIndex = req.PrevLogIndex - 1
			//req.PrevLogTerm = rf.GetEntryByLogIndex(req.PrevLogIndex).Term

			reply := AppendEntriesReply{}
			rf.mu.Unlock()
			rf.sendAppendEntries(index, &req, &reply) // send AppendEntries RPC
			rf.mu.Lock()
			if rf.currentTerm != req.Term {
				rf.mu.Unlock()
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.role = FOLLOWER
				rf.ElectionLoop()
				rf.mu.Unlock()
			}
			if req.Term == reply.Term && req.Term == rf.currentTerm {
				if reply.Success {
					DPrintf("")
					rf.nextIndex[index] = rf.GetLastLogEntry().Index + 1
					rf.matchIndex[index] = req.PrevLogIndex + len(req.Entries)
					DPrintf("==>reply success peer-%v nextIndex[%v] matchIndex[%v].\n", index, rf.nextIndex[index], rf.matchIndex[index])
					rf.UpdateCommitIndex()
				} else {
					rf.nextIndex[index]--
				}
			}
			rf.mu.Unlock()
		} else {
			rf.mu.Unlock()
		}
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
	DPrintf("begin update commitIndex currentTerm[%v]\n", rf.currentTerm)
	DPrintf("begin update commitIndex matchIndex[%v]\n", rf.matchIndex)
	DPrintf("begin update commitIindex log [%v]\n", rf.log)

	DPrintf("begin update commitIndex min:%v max:%v\n", min, max)
	for {
		DPrintf("iter N:%v\n", N)
		count := 0
		for i := range rf.matchIndex {
			if rf.matchIndex[i] >= N {
				count++
			}
		}

		DPrintf("iter count:%v majority:%v nTerm:%v currentTerm:%v \n.", count, majority, rf.GetEntryByLogIndex(N).Term, rf.currentTerm)
		if count >= majority && rf.GetEntryByLogIndex(N).Term == rf.currentTerm {
			old := rf.commitIndex
			rf.commitIndex = N
			DPrintf("update commit index from [%v] to [%v]\n", old, rf.commitIndex)
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
		time.Sleep(time.Duration(20) * time.Microsecond)
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied {
			for i := rf.lastApplied + 1; i <= rf.commitIndex && !rf.killed(); i++ {
				cmd := rf.GetEntryByLogIndex(i)
				applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      cmd.Command,
					CommandIndex: cmd.Index,
				}
				rf.lastApplied++
			}
		}
		rf.mu.Unlock()
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
