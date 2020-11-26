package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
	"sync"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// worker通过rpc获取map任务。执行完之后再通过rpc提交任务
// worker通过rpc获取reduce任务，执行完之后再通过rpc提交任务
// Add your RPC definitions here.

// TaskType 任务类型
type TaskType int

const (
	// MAP map类型
	MAP TaskType = 1
	// REDUCE reduce 类型
	REDUCE = 2
	// NONE 没有可分配任务
	NONE = 0
)

// Task 当前任务信息
type Task struct {
	TaskID int
	// 当前任务类型
	CurType TaskType
	// 当CurType是MAP的时候使用
	FileName string
	Content  string

	// 当CurType是REDUCE的时候使用
	Words  map[string][]string
	CurIdx int

	// 执行任务的worker的ID
	WorkerID int
}

// Result 包含worker执行的结果
type Result struct {
	TaskContent Task
	WorkerID    int
	// TaskContent.CurType  TaskType
	// 当CurType是MAP的时候使用
	// 保存的是KV值
	Words []KeyValue
	// 当TaskContentCurType是REDUCE的时候使用
	Count []KeyValue
}

// WorkerInfo worker的具体信息
type WorkerInfo struct {
	WorkerID int
}

// StatueCode 状态码
type StatueCode int

const (
	// SUCCESS 任务执行成功
	SUCCESS StatueCode = 200
	// FAIL 任务执行失败
	FAIL StatueCode = 300
	// TIMEOUT 任务执行超时
	TIMEOUT StatueCode = 400
)

// TaskExecuteStatus 任务执行结果
type TaskExecuteStatus struct {
	Code StatueCode
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

var worker int
var mu sync.Mutex

func workID() int {
	mu.Lock()
	defer mu.Unlock()
	worker++
	return worker
}
