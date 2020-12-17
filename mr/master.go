package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

// Master 主线程
type Master struct {

	// 通信通道，这样就避免锁的使用
	workerCh chan *WorkerInfo
	taskCh   chan *Task
	resultCh chan *Result
	statusCh chan *Status

	removeTaskCh chan string
	removeDoneCh chan string
	taskMapDone  map[string]chan struct{}

	// map[fileName]*Task
	taskMap map[string]*Task
	files   map[string]bool

	mapTaskCount int

	nReduce int

	reduceBuckets   []Bucket
	reduceTaskCount int

	doneReq   chan struct{}
	doneReply chan bool
}
type Bucket map[string][]string

func (m *Master) HandleRequest() {
	log.Printf("master process channel request background\n")
	for {
		select {
		case worker := <-m.workerCh:
			m.taskCh <- m.getTask(worker)
		case result := <-m.resultCh:
			m.statusCh <- m.submit(result)
		case fileName := <-m.removeTaskCh:
			delete(m.taskMap, fileName)
		case fileName := <-m.removeDoneCh:
			delete(m.taskMapDone, fileName)
		case <-m.doneReq:
			m.doneReply <- (len(m.files) == m.mapTaskCount && m.nReduce == m.reduceTaskCount)
		}
	}
}
func getFileNameByIndex(idx int) string {
	return fmt.Sprintf("mr-out-%d", idx)
}
func (m *Master) getTask(worker *WorkerInfo) *Task {
	var task = &Task{}
	task.ID = obtainTaskID()
	task.Worker = worker
	task.Type = NOTASK

	// 这意味着map任务还没有分配完
	if m.mapTaskCount < len(m.files) {
		if len(m.files) == len(m.taskMap) {
			// 当前没有多余的map任务，所有的map任务都在执行
			task.Type = RETRY
			return task
		}
		var fileName string
		for f := range m.files {
			if m.taskMap[f] == nil {
				fileName = f
				break
			}
		}
		var cs, err = ioutil.ReadFile(fileName)
		if err != nil {
			// 如果读文件出错，直接将该文件删除
			task.Type = RETRY
			delete(m.files, fileName)
			return task
		}
		task.Type = MAP
		task.FileName = fileName
		task.Content = string(cs)
		m.taskMap[fileName] = task
		var done = make(chan struct{})
		m.taskMapDone[fileName] = done
		go func() {
			select {
			case <-done:
			case <-time.After(timeoutduration):
				m.removeTaskCh <- fileName
			}
			m.removeDoneCh <- fileName
		}()
		log.Printf("master assigned MAP task [%s] with fileName of [%s] to worker [%s]\n", task.ID, task.FileName, worker.ID)
	} else if m.nReduce > m.reduceTaskCount {
		if len(m.taskMap) == len(m.files)+m.nReduce {
			// 说明taskMap中有MAP任务，但是MAP已经执行完成了，taskMap中还有REDUCE任务正在执行，且没有其他能够分配的任务了
			task.Type = RETRY
			return task
		}
		var idx int
		var fname string
		for i := range m.reduceBuckets {
			var name = getFileNameByIndex(i)
			if m.taskMap[name] == nil {
				if len(m.reduceBuckets[i]) == 0 {
					m.taskMap[name] = &Task{}
					m.reduceTaskCount++
				} else {
					fname = name
					idx = i
					break
				}
			}
		}
		task.FileName = fname
		var cs, err = json.Marshal(m.reduceBuckets[idx])
		if err != nil {
			task.Type = RETRY
			m.reduceBuckets = append(m.reduceBuckets[:idx], m.reduceBuckets[idx+1:]...)
			m.nReduce--
			return task
		}
		task.Content = string(cs)
		task.Type = REDUCE
		m.taskMap[fname] = task
		var done = make(chan struct{})
		m.taskMapDone[fname] = done
		go func() {
			select {
			case <-done:
			case <-time.After(timeoutduration):
				m.removeTaskCh <- fname
			}
			m.removeDoneCh <- fname
		}()
		log.Printf("master assigned REDUCE task [%s] with fileName of [%s] to worker [%s]\n", task.ID, task.FileName, worker.ID)

	}
	return task
}
func (m *Master) submit(result *Result) *Status {
	var status = &Status{}
	if result.Type == NOTASK {
		status.ResCode = FAIL
		return status
	}
	var task = m.taskMap[result.TaskFileName]
	if task == nil || task.ID != result.TaskID {
		log.Printf("the taskid of file [%s] has changed from [%s] to another\n", result.TaskFileName, result.TaskID)
		status.ResCode = TIMEOUT
		return status
	}

	if result.Type == MAP {
		var kvs []KeyValue
		var err = json.Unmarshal([]byte(result.Content), &kvs)
		if err != nil {
			status.ResCode = FAIL
			return status
		}
		status.ResCode = SUCCESS
		var done = m.taskMapDone[result.TaskFileName]
		close(done)
		m.mapTaskCount++
		m.taskMap[result.TaskFileName].Type = COMPLETE

		//将key和value分别加入reduceBuckets中
		for _, kv := range kvs {
			var idx = ihash(kv.Key) % m.nReduce
			m.reduceBuckets[idx][kv.Key] = append(m.reduceBuckets[idx][kv.Key], kv.Value)
		}
		log.Printf("The data len of submit from MAP task [%s] with fileName of [%s] is [%d]\n", result.TaskID, result.TaskFileName, len(kvs))
	}
	if result.Type == REDUCE {
		status.ResCode = SUCCESS
		var err = os.Rename(result.Content, result.TaskFileName)
		if err != nil {
			status.ResCode = FAIL
			return status
		}
		var done = m.taskMapDone[result.TaskFileName]
		close(done)
		m.reduceTaskCount++
		m.taskMap[result.TaskFileName].Type = COMPLETE
		log.Printf("The data fileName of submit from REDUCE task [%s] with fileName [%s] is [%s]\n", result.TaskID, result.TaskFileName, result.TaskFileName)
	}
	return status
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) DistributeTask(worker *WorkerInfo, task *Task) error {
	m.workerCh <- worker
	var t = <-m.taskCh
	*task = *t
	return nil
}
func (m *Master) SubmitResult(result *Result, status *Status) error {
	m.resultCh <- result
	var s = <-m.statusCh
	*status = *s
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
	log.Printf("master is serving at [%+v]\n", l)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	m.doneReq <- struct{}{}
	ret = <-m.doneReply
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.workerCh = make(chan *WorkerInfo)
	m.taskCh = make(chan *Task)
	m.resultCh = make(chan *Result)
	m.statusCh = make(chan *Status)

	m.removeTaskCh = make(chan string)
	m.removeDoneCh = make(chan string)
	m.taskMapDone = make(map[string]chan struct{})

	m.taskMap = make(map[string]*Task)
	m.files = make(map[string]bool)
	for _, f := range files {
		m.files[f] = true
	}
	m.nReduce = nReduce
	m.reduceBuckets = make([]Bucket, nReduce)
	for i := range m.reduceBuckets {
		m.reduceBuckets[i] = make(Bucket)
	}
	m.doneReply = make(chan bool)
	m.doneReq = make(chan struct{})
	go m.HandleRequest()
	m.server()
	return &m
}
