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
	"6824/labrpc"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"
var logger *log.Logger

func init() {
	logger = log.New(os.Stdout, "", log.Lshortfile|log.Lmicroseconds)
}

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

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int64               // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// persistent state on all servers
	currentTerm int64
	//如果为-1说明没有给别人投票
	votedFor int64
	log      map[int]*Entry

	// volatile state on all servers
	commitIndex int64
	lastApplied int64

	// volatile state on leaders
	nextIndex  map[int]int // map[serverid]logindex
	matchIndex map[int]int // map[serverid]appliedlogindex

	// self defined
	// 互斥使用
	currentState agent

	//记录上一次心跳的时间
	//互斥使用
	lastHeartbeatTime time.Time
	//记录选举延时
	electionTimeout time.Duration

	//保存的leader 的id，用于重定向请求
	leaderID int64

	//最大日志条目记录
	lastLogIndex int64
	lastLogTerm  int64
}

//client的角色
type agent string

const (
	FOLLOWER  agent = "FOLLOWER"
	CANDIDATE       = "CANDIDATE"
	LEADER          = "LEADER"
)

//entry 表示每一条日志条目
//包含具体指令以及任期term
type Entry struct {
	Command interface{}
	Term    int
}

func (en *Entry) copy() *Entry {
	return &Entry{
		Command: en.Command,
		Term:    en.Term,
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
	rf.me = int64(me)

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.log = make(map[int]*Entry)

	rf.commitIndex = -1
	rf.lastApplied = -1

	rf.nextIndex = map[int]int{}
	rf.matchIndex = map[int]int{}

	rf.currentState = FOLLOWER

	rf.lastHeartbeatTime = time.Now()

	rf.leaderID = -1

	rf.lastLogIndex = -1
	rf.lastLogTerm = -1

	//开启一个后台线程，作为时钟
	//处理心跳和选举延时
	go rf.timerloop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	//设置选举延时
	rf.resetElectionTimer()

	logger.Printf("Make a Raft instance, id [%d], total clients [%d], electionTimeout [%v]\n", me, len(peers), rf.electionTimeout)
	return rf
}

//重设选举延时
//并发不安全
func (rf *Raft) resetElectionTimer() {
	rf.electionTimeout = electionTimeoutGenerator()
}

// 如果是leader，只关注心跳间隔
// 如果是candidate，关注选举延时，如果到时了就进行新一轮选举
// 如果是follower，关注选举延时，如果到时了就进行选举
func (rf *Raft) timerloop() {
	//只要服务不停止就一直运行
	for !rf.killed() {
		//时钟滴答
		time.Sleep(sleepDuration)
		// logger.Printf("Raft [%d] tick...\n", rf.me)

		var state agent
		var last time.Time
		var curELeT time.Duration
		rf.mu.Lock()
		state = rf.currentState
		last = rf.lastHeartbeatTime
		curELeT = rf.electionTimeout
		rf.mu.Unlock()

		var now = time.Now()
		var sub = now.Sub(last)

		if state == CANDIDATE {
			if sub >= curELeT {

				//开始新一轮选举
				go rf.startElection()
				logger.Printf("\n..........Raft [%d] election timeout.........\n\n", rf.me)
			}
		}
		if state == LEADER {
			if sub >= heartbeatDuration {

				//leader需要发送心跳了
				go rf.sendHeartbeat()
				// logger.Printf("Tick...Tick...Raft [%d] heartbeat timeout\n", rf.me)
			}
		}
		if state == FOLLOWER {
			if sub >= curELeT {
				//转为candidate
				rf.mu.Lock()
				rf.currentState = CANDIDATE
				rf.mu.Unlock()

				//以candidate的身份开始选举
				go rf.startElection()
				logger.Printf("\n..........Raft [%d] election timeout..........\n\n", rf.me)
			}
		}
	}
	logger.Printf("Raft [%d] is killed, so timerloop() will go down\n", rf.me)
}
func (rf *Raft) startElection() {

	//进行选举

	//增加任期号
	atomic.AddInt64(&rf.currentTerm, 1)
	var curTerm = atomic.LoadInt64(&rf.currentTerm)

	//给自己投票
	atomic.StoreInt64(&rf.votedFor, rf.me)

	rf.mu.Lock()
	//重设定时器
	rf.lastHeartbeatTime = time.Now()

	//生成新的选举延时
	rf.resetElectionTimer()
	rf.mu.Unlock()

	logger.Printf("Raft [%d] start term[%d]'s election and vote for self.\n", rf.me, curTerm)

	//投票计数
	var votes int64 = 1

	var args RequestVoteArgs
	args.CandidateID = rf.me
	args.Term = curTerm

	args.LastLogIndex = atomic.LoadInt64(&rf.lastLogIndex)
	args.LastLogTerm = atomic.LoadInt64(&rf.lastLogTerm)

	for followeid := range rf.peers {
		if followeid == int(rf.me) {
			continue
		}

		go func(i int) {

			var reply RequestVoteReply
			var res = rf.sendRequestVote(i, &args, &reply)
			if res == false {
				//网络问题
				logger.Printf("The requestVote sent from [%d] to [%d] is lost\n", rf.me, i)
				return
			}

			//如果回复的term比自己的大
			//更新currentTerm
			//转为follower
			if reply.Term > atomic.LoadInt64(&rf.currentTerm) {
				atomic.StoreInt64(&rf.currentTerm, reply.Term)

				rf.mu.Lock()
				if rf.currentState != FOLLOWER {
					rf.currentState = FOLLOWER

					//很重要，用来判断是否已经给人投票了
					rf.votedFor = -1
				}
				rf.mu.Unlock()
				return
			}

			//赢得选票
			if reply.VoteGranted && reply.Term == args.Term {
				atomic.AddInt64(&votes, 1)
				logger.Printf("Raft [%d] has granted Raft [%d]'s vote\n", rf.me, i)

				var majori = majority(len(rf.peers))
				if atomic.LoadInt64(&votes) >= int64(majori) {
					//可以成为leader了，其余的投票也可以忽略
					rf.mu.Lock()
					if rf.currentState != LEADER {
						rf.currentState = LEADER
						logger.Printf("Raft [%d] become leader, start sendheartbeat\n", rf.me)

						// rf.lastHeartbeatTime = time.Now()
					}
					rf.mu.Unlock()
				}
			}
		}(followeid)
	}
}
func majority(total int) int {
	return total/2 + 1
}

// 只有leader发送心跳，
//给所有的peers发送心跳
//新建一个新的goroutine来处理
func (rf *Raft) sendHeartbeat() {

	//设置新的心跳记录
	rf.mu.Lock()
	rf.lastHeartbeatTime = time.Now()
	rf.mu.Unlock()

	//构建RPC请求参数
	var args AppendEntriesArgs
	rf.mu.Lock()
	args.Term = rf.currentTerm
	args.LeaderID = rf.me
	rf.mu.Unlock()

	for servid := range rf.peers {
		//跳过自己
		if servid == int(rf.me) {
			continue
		}
		go func(i int) {
			var reply AppendEntriesReply
			var res = rf.sendAppendEntries(i, &args, &reply)
			if res == false {
				//网络问题不可达
				// logger.Printf("Leader [%d] in term [%d] send to [%d]'s heartbeat is lost\n", rf.me, args.Term, i)
				return
			}
			//当前任期、请求任期、回复任期
			//首先判断当前任期和请求中的任期是否相同，
			// 如果不同就忽略该回复\

			var cur = atomic.LoadInt64(&rf.currentTerm)
			if cur != args.Term {
				return
			}
			if reply.Term > args.Term {
				//任期不一致表示该leader的任期不是最高的
				atomic.StoreInt64(&rf.currentTerm, reply.Term)
				rf.mu.Lock()
				rf.currentState = FOLLOWER
				rf.lastHeartbeatTime = time.Now()
				rf.mu.Unlock()
				atomic.StoreInt64(&rf.votedFor, -1)
				logger.Printf("Leader [%d] in term [%d] learned a high term [%d] in heartbeat, convert to follower\n", rf.me, args.Term, reply.Term)
			}
		}(servid)
	}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = int(atomic.LoadInt64(&rf.currentTerm))
	rf.mu.Lock()
	if rf.currentState == LEADER {
		isleader = true
	}
	rf.mu.Unlock()

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

// LAB 2A

// AppendEntriesArgs used for AppendEntries RPC
type AppendEntriesArgs struct {
	Term     int64
	LeaderID int64

	PrevLogTerm int64
	// gob编码的二进制
	Entries []byte

	LeaderCommit int64
}

// AppendEntriesReply used for AppendEntries RPC
type AppendEntriesReply struct {
	Term    int64
	Success bool
}

// AppendEntries used for commit entries, and heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	var curTerm = atomic.LoadInt64(&rf.currentTerm)

	//设置回复的term
	reply.Term = curTerm

	// 1.Reply false if term < currentTerm(§5.1)
	if args.Term < curTerm {
		reply.Success = false
		return
	}

	//参数中的term>=currentTerm
	//更新currentTerm
	if args.Term > curTerm {
		rf.mu.Lock()
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			//转为FOLOWER
			rf.currentState = FOLLOWER
		}
		curTerm = rf.currentTerm
		rf.mu.Unlock()
	}

	reply.Term = curTerm

	//更新leaderID
	rf.mu.Lock()
	if rf.leaderID != args.LeaderID {
		rf.leaderID = args.LeaderID
	}
	rf.mu.Unlock()

	//判断是否为心跳
	//即entries是否为空(§5.2)
	if len(args.Entries) == 0 {
		//没有携带日志
		//处理心跳
		//重设心跳时间
		rf.mu.Lock()
		rf.lastHeartbeatTime = time.Now()
		rf.mu.Unlock()
		logger.Printf("Raft [%d] received hearbeat from [%d] in term [%d]\n", rf.me, args.LeaderID, args.Term)

		return
	}

	// 2.Reply false if log doesn’t
	// contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)

	// 3.If an existing entry conflicts with
	// a new one (same indexbut different terms),
	// delete the existing entry and all that
	// follow it (§5.3)
	// 4.Append any new entries not
	// already in the log5.  If leaderCommit > commitIndex,
	// set commitIndex =min(leaderCommit, index of last new entry)
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//2A
	Term         int64
	CandidateID  int64
	LastLogIndex int64
	LastLogTerm  int64
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int64
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 2A
	logger.Printf("Raft [%d] get request:RequestVote from candidate [%d] with term [%d]\n", rf.me, args.CandidateID, args.Term)

	var curTerm = atomic.LoadInt64(&rf.currentTerm)

	// 1.  Reply false if term < currentTerm (§5.1)
	reply.Term = curTerm
	if args.Term < curTerm {
		reply.VoteGranted = false
		logger.Printf("Raft [%d]'s term [%d] is higher than term [%d], so can't vote for [%d]\n", rf.me, rf.currentTerm, args.Term, args.CandidateID)
		return
	}
	//如果任期相同，就不能给投票
	//此时会出现分裂投票现象
	if curTerm == args.Term {
		reply.VoteGranted = false
		logger.Printf("Raft [%d] is candidate too in term [%d], so can't vote for [%d]\n", rf.me, args.Term, args.CandidateID)
		return
	}
	//防止leader宕机重启之后继续发心跳
	//更改当前角色
	rf.mu.Lock()
	rf.currentTerm = args.Term
	if rf.currentState != FOLLOWER {
		rf.currentState = FOLLOWER
	}
	rf.mu.Unlock()

	reply.Term = args.Term

	//重设选举定时器
	rf.mu.Lock()
	rf.lastHeartbeatTime = time.Now()
	rf.mu.Unlock()
	// 2.  If votedFor is null or candidateId,
	// and candidate’s log is atleast as up-to-date
	// as receiver’s log, grant vote (§5.2,§5.4)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//此时args的任期是高于这个follower的
	//所以其voteFor是无效的，也就不用判断是否为-1
	// if (rf.votedFor == -1 ||
	// 	rf.votedFor == args.CandidateID) &&
	if args.LastLogIndex >= rf.lastLogIndex {

		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		logger.Printf("Raft [%d] vote for [%d]\n", rf.me, args.CandidateID)

		return
	}
	logger.Printf("Raft [%d] has voted for [%d], instead [%d]\n", rf.me, rf.votedFor, args.CandidateID)
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
	logger.Printf("Raft [%d] sendRequestVote to [%d] for vote\n", rf.me, server)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// LAB 2A DONE

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
