##  实现过程

6.824的Lab2A内容是实现Raft的选举以及心跳，并且有一定的容错率，可以容忍机器宕机重新加入集群。

课程的网址：

```http
https://pdos.csail.mit.edu/6.824/labs/lab-raft.html
```

如果之前看了Raft论文的话，代码的实现是很简单的。但是`talk is cheap, show me the code`。实现的时候具体的细节还是很多的，一不小心都会导致测试失败。说到测试，6.824的课程中测试Raft的框架实现的很巧妙，具体内容可以看我另一篇文章。

课程给出了一些hint：一个事件使用一个goroutine来执行，一个goroutine中不能即处理选举又处理心跳。这会导致系统过于复杂，每个事件就新起一条goroutine就会很方便。

多个goroutine就要涉及到并发了，课程建议使用锁而不是channe来处理并发，我个人认为是因为需要处理的数据太多了，如果每个数据都使用一个channel，会增加系统的复杂性。

那么，Lab2A的整体结构就是：使用一个后台goroutine作为Raft的时钟，每个滴答检测是否触发心跳或者选举，如果满足条件且能够触发相关事件，就新起一条goroutine来处理该事件。

##  1.  初始化

该Lab一开始需要通过Make函数创建一个Raft实例，同时新起一条goroutine来执行后台时钟，触发不同的事件。

###  1.1.  Make

在实现过程中，共享变量能够使用atomic包下的功能就尽可能避免用Mutex，毕竟硬件支持的原子操作更好使。

首先是Make(...)函数，这个函数就是创建一个Raft实例，并开启一条后台时钟goroutine。

```go
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
    // Fig.2中的状态
	// persistent state on all servers
	currentTerm int64
	//如果为-1说明没有给别人投票
	votedFor int
	log      map[int]*Entry

	// volatile state on all servers
	commitIndex int64
	lastApplied int64

	// volatile state on leaders
	nextIndex  map[int]int // map[serverid]logindex
	matchIndex map[int]int // map[serverid]appliedlogindex

	// self defined
	// 互斥使用
	currentState Agent

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
```

在Make函数中，votedFor、commitIndex、lastApplied、leaderID、lastLogIndex和lastLogTerm初始化为-1；currentState设置为FOLLOWER，表明Raft实例一开始是一个follower；lastHeartbeatTime初始化为当前时间，用来触发心跳和选举。通过Raft.resetElectionTimer()设置electionTimeout，每个Raft实例持有的是不同值，防止出现分裂投票，这个函数使用随机数实现。

同时新起一条goroutine，运行Raft.timerloop()函数，详情见[时钟]章节。

```go
var elecrand = rand.New(rand.NewSource(time.Now().Unix()))

//生成随机选举延时
func electionTimeoutGenerator() time.Duration {
	var start = 1200
	var end = 1500
	return time.Duration(start+elecrand.Intn(end-start)) * time.Millisecond
}
```

###  1.2.  AppendEntriesArgs和AppendEntriesReply

```go
// AppendEntriesArgs used for AppendEntries RPC
type AppendEntriesArgs struct {
	Term     int
	LeaderID int

	PrevLogTerm int
	// gob编码的二进制
	Entries []byte

	LeaderCommit int
}

// AppendEntriesReply used for AppendEntries RPC
type AppendEntriesReply struct {
	Term    int
	Success bool
}
```

这两个结构体是用在AppendEntries RPC调用中的，是leader向follower传递日志条目或者维持心跳的。如果AppendEntriesArgs中的Entries是空的，就说明这是一个心跳。接收者也要按照论文图2的内容严格实现。

###  1.3.  RequestVoteArgs和RequestVoteReply

```go
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}
```

这两个结构体是candidate向follower发送投票请求RequestVote RPC使用的。具体内容严格按照论文图2实现。

##  2.  时钟: timerloop()

timerloop()函数其实就是一个循环，只要Raft没有被调用Kill()就会一直执行。循环体中使用time.Sleep()来执行每一个滴答。

Lab给的要求中说明一秒内不能超过十次心跳，所以设置心跳时间为150ms；选举延迟最大为1500ms，为了避免分裂投票，使用随机数将其控制在1200ms~1500ms。

每一个滴答都执行相关的检测。

* 如果currentState==CANDIDATE，判断当前时间time.Now().Sub(rf.lastHeartbeatTime)是否大于rf.electionTimeout，如果成立，说明上次的选举没有成功，就执行下一次选举。通过go关键字新起goroutine执行startElection()。
* 如果currentState==LEADER，并且time.Now().Sub(rf.lastHeartbeatTime)大于heartbeatDuration，说明leader需要发送心跳给followers了。新开goroutine执行sendHeartbeat()函数。
* 如果currentState==FOLLOWER，且sub大于选举延时electionTimeout，说明当前leader已经挂了，自己可以成为candidate执行选举过程。设置currentState为CANDIDATE，新起一条goroutine执行startElection()。

##  3.  选举

candidate在startElection中调用其他服务器的RequestVote RPC，判断是否获得大多数的选票，如果获得，就成为leader。

###  3.1.  startElection

在startElection()函数中，首先Raft增加自己的任期号并给自己投票，重置lastHeartbeatTime和electionTimeout。

接下来，向所有的服务器发送RequestVote RPC调用。

1. 如果回复中的Term比当前的任期要新，Raft实例会转为FOLLOWER状态，同时设置votedFor为-1表明转为follower之后还没有给别人投过票，退出。

2. 如果回复中VoteGranted为true并且请求和回复中的任期相同，表明该candidate已经赢得了选票。选票统计加一并判断是否满足大多数要求，如果满足，直接转为leader，剩余的选票已经不重要了。

3. 剩下的事情就是等待时钟触发心跳事件，周期发送心跳。

```go
//请求选票的部分代码
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
```

###  3.2.  RequestVote

RequestVote()函数处理其他Raft实例发送来的投票请求。

1. 如果请求中携带的Term比当前任期currentTerm要小，就直接返回false，因为不会给任期低的实例投票。
2. 如果请求中的任期Term和currentTerm相等，也不会投票，因为自己可能投过别人了并且学习了别人的任期。
3. 为了防止leader宕机恢复之后继续发送心跳，更改当前的状态，如果当前状态为leader，已经收到了更高任期的投票请求，说明自己这个leader已经过时了，就转为follower。
4. 重新记录lastHeartbeatTime，这样避免触发选举。
5. 此时请求参数的Term是高于currentTerm，那肯定会投票，所以就记录votedFor为请求中携带的CandidateID，将reply设置为true返回。

```go
//去掉锁操作的代码
	var curTerm = atomic.LoadInt64(&rf.currentTerm)
	// 1.  Reply false if term < currentTerm (§5.1)
	reply.Term = curTerm
	if args.Term < curTerm {
		reply.VoteGranted = false
		return
	}
	//如果任期相同，就不能给投票
	//此时会出现分裂投票现象
	if curTerm == args.Term {
		reply.VoteGranted = false
		return
	}
	//防止leader宕机重启之后继续发心跳
	//更改当前角色
	rf.currentTerm = args.Term
	if rf.currentState != FOLLOWER {
		rf.currentState = FOLLOWER
	}

	reply.Term = args.Term
	//重设选举定时器
	rf.lastHeartbeatTime = time.Now()
	// 2.  If votedFor is null or candidateId,
	// and candidate’s log is atleast as up-to-date
	// as receiver’s log, grant vote (§5.2,§5.4)

	//此时args的任期是高于这个follower的
	//所以其voteFor是无效的，也就不用判断是否为-1
	if args.LastLogIndex >= rf.lastLogIndex {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		return
	}
```

##  4.  心跳

当timerloop()中触发心跳事件后，leader会向所有的服务器发送心跳。但是，如果服务器宕机之后又恢复了，再次发送心跳会失败，因为此时集群有了新的leader。该章节也会涉及这部分内容。

###  4.1.  sendHeartbeat

sendHeartbeat()中，leader会依次向其他实例发送AppendEntries RPC请求，并在处理回复。

1. 设置lastHeartbeatTime为当前时间
2. 依次给所有的实例发送AppendEntries RPC，如果RPC调用返回false，说明有网络丢包出现。
3. 由于RPC可能会有延迟，所以首先判断当前任期和请求中的任期是否相同，如果不同直接返回。
4. 如果回复中的任期比请求中的任期高，说明当前实例的任期不是最新的，它已经不是leader了。将其状态切换为folower，重新设置lastHeartbeatTime和voteFor。

```go
var reply AppendEntriesReply
			var res = rf.sendAppendEntries(i, &args, &reply)
			if res == false {
				//网络问题不可达
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
			}
```

###  4.2.  AppendEntries

AppendEntries使用来处理leader发送的RPC调用的。这个函数比较复杂，在Lab2A中只实现了心跳的部分，添加日志的部分留待之后实现。

1. 如果当前任期比请求中的任期还要高，发送请求的实例就不能当leader，所以直接返回false。
2. 假如一个leader1宕机恢复之后，收到了新leader2的AppendEntries RPC，leader1意识到有了新的leader了，就直接转为follower。
3. leader1转为follower需要记录leader2的id，以便于之后重定向。
4. 判断请求参数中的Entries为空就表示心跳。实例作为follower会重新设置lastHearbeatTime。
5. 剩余的内容是添加日志，之后的文章会实现。

```go
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
		return
	}
```

##  总结

这个Lab其实挺简单的，主要是得把Raft那一篇论文搞明白。然后就是一些细节了，比如votedFor什么时候设置为-1。RPC的调用者和接收者的一些实现细节。如果不理解的同学可以读一下，Lab2A的hints中给出的Lock和Structure的内容，对理解和设计整个Raft十分有用。