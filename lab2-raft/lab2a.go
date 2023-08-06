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
	//	"bytes"

	"fmt"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type State int

const (
	follower State = iota
	candidate
	leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	PeerNum int

	PeerState State
	//lastLogIndex int
	//lastLogTerm  int

	Timeout   time.Duration
	LastStamp time.Time

	// persistent state
	CurrentTerm int // 这台服务器的最近任期
	VoteFor     int // 给哪个服务器投过票，存candidateId
	VoteCount   int // 被投了几票

	// Volatile state on all servers
	//CommitIndex int // 已被提交的日志的最高索引号
	//LastApplied int // 被应用的日志的最高索引号

	// Volatile state on leader
	//NextIndex  []int // 需要发送给每个服务器下一条日志条目索引号
	//MatchIndex []int // 已知要复制到每个服务器上的最高日志条目号
}

type Log struct {
	Term int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	term = rf.CurrentTerm
	isleader = rf.PeerState == leader
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int // 任期号
	CandidateId int // candidate的ID
	//LastLogIndex int // candidate的最高日志条目索引
	//LastLogTerm  int // candidate的最高日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 任期号
	VoteGranted bool // 如果是true，则收到了选票
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 如果 term < currentTerm返回 false
	// 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他

	reply.Term = rf.CurrentTerm
	reply.VoteGranted = false

	if args.Term < rf.CurrentTerm {
		return
	}

	if args.Term > rf.CurrentTerm {
		if rf.VoteFor == -1 || rf.VoteFor == args.CandidateId {
			// 投票，并且更新自己的状态
			rf.CurrentTerm = args.Term
			reply.VoteGranted = true
			rf.VoteFor = args.CandidateId

			rf.BeFollower(args.Term, args.CandidateId)
		}
	} else {
		rf.CurrentTerm = args.Term
		reply.VoteGranted = true
		rf.VoteFor = args.CandidateId
		rf.BeFollower(args.Term, args.CandidateId)
		return
	}
}

func (rf *Raft) BeFollower(term int, CandidateId int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.CurrentTerm = term
	rf.PeerState = follower
	rf.VoteFor = CandidateId
	rf.VoteCount = 0
	rf.Timeout = rf.getTimeout()
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

// Start the service using Raft (e.g. a k/v server) wants to start
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		now := time.Now()
		if (rf.PeerState == follower || rf.PeerState == candidate) && now.Sub(rf.LastStamp) > rf.Timeout {
			rf.mu.Unlock()
			rf.startElect()
		} else if rf.PeerState == leader && now.Sub(rf.LastStamp) > 100*time.Millisecond {
			rf.mu.Unlock()
			fmt.Println(rf.me, " startHearBeat")
			rf.startHeartBeat()
		} else {
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) startElect() {
	rf.mu.Lock()
	rf.CurrentTerm++
	rf.PeerState = candidate
	rf.LastStamp = time.Now()
	rf.VoteFor = rf.me
	rf.VoteCount = 1
	rf.Timeout = rf.getTimeout()

	arg := &RequestVoteArgs{
		Term:        rf.CurrentTerm,
		CandidateId: rf.me,
	}
	rf.mu.Unlock()

	for idx := 0; idx < rf.PeerNum; idx++ {
		if idx != rf.me {
			go func(idx int) {
				reply := &RequestVoteReply{}
				rf.sendRequestVote(idx, arg, reply)

				rf.mu.Lock()
				defer rf.mu.Unlock()

				// 判断这个节点的状态有没有发生改变
				if rf.CurrentTerm == arg.Term && rf.PeerState == candidate {
					if rf.CurrentTerm < reply.Term {
						rf.BeFollower(reply.Term, -1)
					} else if reply.VoteGranted {
						// 表示投票成功
						rf.VoteCount++
						fmt.Println("if reply.VoteGranted", rf.VoteCount, rf.PeerNum, rf.VoteCount >= (rf.PeerNum/2)+1)
						if rf.VoteCount >= (rf.PeerNum/2)+1 {
							fmt.Println("leader")
							rf.LastStamp = time.Now()
							rf.PeerState = leader
							rf.startHeartBeat()
						}
					}
				}
			}(idx)
		}
	}
}

func (rf *Raft) startHeartBeat() {
	if rf.PeerState != leader {
		log.Printf("peer:%d(%d) became follower", rf.me, rf.CurrentTerm)
		return
	}
	for idx := 0; idx < rf.PeerNum; idx++ {
		if rf.me == idx {
			continue
		}
		args := &AppendEntriesArgs{
			Term:     rf.CurrentTerm,
			LeaderId: rf.me,
		}
		reply := &AppendEntriesReply{
			Term:    0,
			Success: false,
		}

		go func(server int) {
			//log.Printf("Peer %d(%d) : Be Leader and Sending void entry to %d", rf.me,rf.currentTerm,server)
			rf.sendAppendEntry(server, args, reply)
			return
		}(idx)
		time.Sleep(110 * time.Millisecond)
	}
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntry(arg *AppendEntriesArgs, reply *AppendEntriesReply) {
	if arg.Term > rf.CurrentTerm {
		rf.BeFollower(arg.Term, arg.LeaderId)
		return
	}
	if arg.Term == rf.CurrentTerm {
		return
	}
}

func (rf *Raft) getTimeout() time.Duration {
	return time.Duration(50*(4+rand.Int()%7)) * time.Millisecond
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.PeerState = follower
	rf.PeerNum = len(peers)
	rf.VoteFor = -1

	rf.LastStamp = time.Now()
	rf.Timeout = rf.getTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
