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
	Term int
	data []byte
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
	currentTerm      int        //latest term server has seen (initialized to 0)
	votedFor         string     //candidateId that received vote in current term
	role             Role       // roles
	log              []LogEntry //log entries
	lastResponseTime int64      //rpc last response time
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
	Term        int    //candidate's term
	CandidateId string // candidate requesting vote
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
	if rf.role == FOLLOWER {
		if args.Term < rf.currentTerm { //request stale
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			return
		}
		if args.Term == rf.currentTerm {
			if rf.votedFor == "" /* ||  2B*/ {
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
			} else if rf.votedFor != "" {
				reply.VoteGranted = false //already vote
			}
			if rf.lastResponseTime < now {
				rf.lastResponseTime = now
			}
			return
		}

		//Rule for All servers
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			reply.Term = args.Term
			reply.VoterId = strconv.Itoa(rf.me)
			if rf.lastResponseTime < now {
				rf.lastResponseTime = now
			}
			return
		}
		return
	} else if rf.role == CANDIDATE {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			reply.Term = args.Term
			reply.VoterId = strconv.Itoa(rf.me)
			rf.role = FOLLOWER
			return
		}
		return
	} else if rf.role == LEADER {
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			reply.Term = args.Term
			reply.VoterId = strconv.Itoa(rf.me)
			rf.role = FOLLOWER
			rf.ElectionLoop()
			return
		}
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
	rf.lastResponseTime = time.Now().UnixNano()
	rf.ElectionLoop()
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int  //currentTerm, for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//only follower can append entries
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	now := time.Now().UnixNano()
	rf.mu.Lock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		rf.mu.Unlock()
		return
	}

	if args.Term == rf.currentTerm {
		reply.Term = args.Term
		if rf.role == FOLLOWER {
			if rf.lastResponseTime < now {
				rf.lastResponseTime = now
			}
			reply.Success = true

		} else {
			if rf.lastResponseTime < now {
				rf.lastResponseTime = now
			}
			reply.Success = true
		}
		rf.mu.Unlock()
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = args.Term

		role := rf.role
		if rf.lastResponseTime < now {
			rf.lastResponseTime = now
		}
		rf.mu.Unlock()
		if role == FOLLOWER {
			reply.Success = true
		} else if role == CANDIDATE {
			rf.role = FOLLOWER
			reply.Success = true
		} else if role == LEADER {
			rf.role = FOLLOWER
			reply.Success = true
		}
		return
	} else { //if args.Term == rf.currentTerm {
		rf.currentTerm = args.Term
		reply.Term = args.Term
		if rf.role == FOLLOWER {
			reply.Success = true
		}
		rf.mu.Unlock()
		return
	}
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
	req := RequestVoteArgs{
		Term:        term,
		CandidateId: candidate,
	}
	role = rf.role
	majorCount := len(rf.peers)/2 + 1
	totalPeer := len(rf.peers)
	rf.mu.Unlock()
	rf.DoElection(role, term, &req, majorCount, totalPeer)
}

func (rf *Raft) DoElection(role Role, oldTerm int, req *RequestVoteArgs, majorCount int, totalPeer int) {
	//vote for myself

	voteCount := 1
	finished := 1
	rf.votedFor = strconv.Itoa(rf.me)
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
				if oldTerm == reply.Term && oldTerm == rf.currentTerm && reply.VoteGranted {
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
		rf.mu.Unlock()
		rf.AppendEntriesLoop()
	}
	if voteCount < majorCount {
		DPrintf("voteCount %v,  split brain (%v) finished %v.\n", voteCount, rf.me, finished)
	}
}

// todo candidate transfer to follower should reset electionTimeout?
func (rf *Raft) TransitionFromCandidateToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != CANDIDATE {
		return
	}
	rf.role = FOLLOWER
}

func (rf *Raft) TransitionFromLeaderToFollower() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != LEADER {
		return
	}
	oldRole := rf.role
	rf.role = FOLLOWER

	DPrintf("%v Role Transfer [%v --> %v]\n", rf.me, GetRoleName(oldRole), rf.GetRoleName())
	rf.ElectionLoop()
}

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

			nano :=time.Now().UnixNano()
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
	const heartBeatInterval = time.Duration(130) * time.Millisecond
	DPrintf("peer-%v start heart beat with interval %v.\n", rf.me, heartBeatInterval)
	go func() {
		for {
			if _, isLeader := rf.GetState(); !isLeader {
				return
			}
			if rf.killed() {
				return
			}
			rf.AppendlogEntries()
			time.Sleep(heartBeatInterval)
		}
	}()
}

func (rf *Raft) AppendlogEntries() {
	rf.mu.Lock()
	DPrintf("do append log entries...........")
	totalPeer := len(rf.peers)
	term := rf.currentTerm
	leaderId := rf.me
	rf.mu.Unlock()
	req := AppendEntriesArgs{Term: term, LeaderId: leaderId}
	for iPeer := 0; iPeer < totalPeer; iPeer++ {
		if iPeer != rf.me {
			go func(index int) {
				reply := AppendEntriesReply{}
				DPrintf("[term:%v] %v ---heartBeat--->%v.", term, leaderId, index)
				rf.sendAppendEntries(index, &req, &reply)
				DPrintf("[term:%v] %v<---heartBeat---%v.", reply.Term, leaderId, reply.Success)
				rf.mu.Lock()
				if rf.currentTerm == term {
					if rf.currentTerm < reply.Term { // current state stale
						rf.currentTerm = reply.Term
						oldRole := rf.role
						rf.role = FOLLOWER
						DPrintf("%v Role Transfer [%v --> %v]\n", rf.me, GetRoleName(oldRole), rf.GetRoleName())
						rf.ElectionLoop()
					}
				}
				rf.mu.Unlock()
			}(iPeer)
		}
	}
}
