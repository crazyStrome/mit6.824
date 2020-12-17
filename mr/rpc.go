package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/GUAIK-ORG/go-snowflake/snowflake"
)

// initialization
var timeoutduration = 10 * time.Second
var workerId *snowflake.Snowflake
var taskId *snowflake.Snowflake
var generalId *IDgenerator

func init() {
	var err error
	workerId, err = snowflake.NewSnowflake(int64(0), int64(0))
	if err != nil {
		log.Println("init workerid error:", err)
	}
	taskId, err = snowflake.NewSnowflake(int64(1), int64(0))
	if err != nil {
		log.Println("init taskid error:", err)
	}
	generalId = &IDgenerator{}
}
func obtainWorkerID() string {
	if workerId != nil {
		return fmt.Sprintf("%d", workerId.NextVal())
	}
	return generalId.generateID()
}
func obtainTaskID() string {
	if taskId != nil {
		return fmt.Sprintf("%d", taskId.NextVal())
	}
	return generalId.generateID()
}

type IDgenerator struct {
	sync.Mutex
	id uint64
}

func (ig *IDgenerator) generateID() string {
	var res string
	ig.Lock()
	ig.id++
	res = fmt.Sprintf("%d-%d", time.Now().UnixNano(), ig.id)
	ig.Unlock()
	return res
}

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type TaskType uint8

const (
	NOTASK TaskType = iota
	MAP
	REDUCE
	RETRY
	COMPLETE
)

type WorkerInfo struct {
	ID string
}
type Task struct {
	ID       string
	Type     TaskType
	Content  string
	FileName string
	Worker   *WorkerInfo
}
type Result struct {
	Type         TaskType
	TaskFileName string
	TaskID       string
	Content      string
}
type StatusCode int

const (
	SUCCESS StatusCode = iota
	TIMEOUT
	FAIL
)

type Status struct {
	ResCode StatusCode
}
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
