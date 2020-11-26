package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

// TaskInfo 包含了任务相关的信息，包括workerid和过期时间
type TaskInfo struct {
	// worker 的 id
	workerID int
	// timeStamp 是任务执行的deadline
	timeStamp time.Time
}

// Master 主线程
type Master struct {
	mu sync.Mutex
	// Your definitions here.
	// fielProcessing 保存的是正在处理的file
	fileProcessing map[string]TaskInfo

	// fileProcceed 保存的是已经处理完成的任务
	fileProcceed map[string]bool

	// buckets 对map产生的kv对进行分类，宽度是nReduce
	buckets []map[string][]string
	// reduceMap contains the word & worker id
	// reduceMap       map[string]int
	bucketProccessing map[int]TaskInfo
	bucketProcceed    map[int]bool

	files   []string
	nReduce int

	generatedID int

	timeoutDuration time.Duration

	// race
	mapDone bool
	// race
	reduceDone bool

	ofile *os.File
}

// GetTaskID 用于生成taskID
func (m *Master) GetTaskID() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.generatedID++
	return m.generatedID
}

// MapDone 是线程安全的
func (m *Master) MapDone() bool {
	return m.mapDone
}

// ReduceDone 是线程安全的
func (m *Master) ReduceDone() bool {
	return m.reduceDone
}

// Your code here -- RPC handlers for the worker to call.

// FetchTask 用来获取task
func (m *Master) FetchTask(worker *WorkerInfo, task *Task) error {
	// map和reduce都完成了就没有任务了
	if m.MapDone() && m.ReduceDone() {
		log.Println("master has no task for worker")
		task.CurType = NONE
		return nil
	}
	if !m.MapDone() {
		return m.fetchMap(worker, task)
	}
	return m.fetchReduce(worker, task)
}
func (m *Master) fetchMap(worker *WorkerInfo, mapTask *Task) error {
	defer func() {
		log.Printf("master assign task [%+v] to worker [%+v]\n", mapTask.TaskID, worker)
	}()
	mapTask.CurType = MAP
	mapTask.WorkerID = worker.WorkerID

	var now = time.Now()
	for f, ti := range m.fileProcessing {
		//如果时间戳ti比当前时间还早，就说明任务已经过期了
		//就把当前任务给worker
		if ti.timeStamp.Before(now) {
			mapTask.FileName = f
			mapTask.TaskID = m.GetTaskID()
			mapTask.Content = getContent(f)

			m.fileProcessing[f] = TaskInfo{
				workerID:  worker.WorkerID,
				timeStamp: now.Add(m.timeoutDuration),
			}
			return nil
		}
	}
	//如果需要处理的文件还没有被分发下去，就进行第一次分发
	for _, f := range m.files {
		if m.fileProcceed[f] {
			continue
		}
		mapTask.FileName = f
		mapTask.TaskID = m.GetTaskID()
		mapTask.Content = getContent(f)

		m.fileProcessing[f] = TaskInfo{
			workerID:  worker.WorkerID,
			timeStamp: now.Add(m.timeoutDuration),
		}
		return nil
	}
	return nil
}
func getContent(fileName string) string {
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()
	return string(content)
}

func (m *Master) fetchReduce(worker *WorkerInfo, reduceTask *Task) error {
	defer func() {
		log.Printf("fetchReduce by worker: [%+v], task.Words length: [%+v]\n", worker, len(reduceTask.Words))
	}()
	reduceTask.CurType = REDUCE
	reduceTask.WorkerID = worker.WorkerID

	var now = time.Now()
	// 先查找超时的任务
	for i, ti := range m.bucketProccessing {
		if ti.timeStamp.Before(now) {
			reduceTask.TaskID = m.GetTaskID()
			reduceTask.Words = m.buckets[i]
			reduceTask.CurIdx = i

			m.bucketProccessing[i] = TaskInfo{
				workerID:  worker.WorkerID,
				timeStamp: now.Add(m.timeoutDuration),
			}
			return nil
		}
	}
	for i, kvs := range m.buckets {
		if m.bucketProcceed[i] {
			continue
		}
		reduceTask.TaskID = m.GetTaskID()
		reduceTask.Words = kvs
		reduceTask.CurIdx = i

		m.bucketProccessing[i] = TaskInfo{
			workerID:  worker.WorkerID,
			timeStamp: now.Add(m.timeoutDuration),
		}
		return nil
	}
	return nil
}

// SubmitResult 用来提交worker的结果数据
func (m *Master) SubmitResult(res *Result, status *TaskExecuteStatus) error {
	log.Printf("Master get result from worker [%+v]\n", res.WorkerID)
	if res.TaskContent.CurType == MAP {
		// 如果结果的WorkerID和master中保存的WorkerID不一致
		// 说明之前出现了超时，该任务已经分配给其他worker了
		if res.WorkerID != m.fileProcessing[res.TaskContent.FileName].workerID {
			status.Code = TIMEOUT
			return nil
		}

		// 将KeyValues切片进行排序，然后分配进不同的bucket
		sort.Sort(ByKey(res.Words))
		m.mu.Lock()

		i := 0
		for i < len(res.Words) {
			j := i + 1
			for j < len(res.Words) && res.Words[j].Key == res.Words[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, res.Words[k].Value)
			}
			var idx = ihash(res.Words[i].Key) % m.nReduce
			m.buckets[idx][res.Words[i].Key] = append(m.buckets[idx][res.Words[i].Key], values...)

			i = j
		}

		m.fileProcceed[res.TaskContent.FileName] = true
		delete(m.fileProcessing, res.TaskContent.FileName)
		if len(m.fileProcceed) == len(m.files) {
			m.mapDone = true
		}
		m.mu.Unlock()

		status.Code = SUCCESS
		return nil
	}
	if res.TaskContent.CurType == REDUCE {
		// 当前任务已超时
		if res.WorkerID != m.bucketProccessing[res.TaskContent.CurIdx].workerID {
			status.Code = NONE
			return nil
		}

		m.mu.Lock()

		m.bucketProcceed[res.TaskContent.CurIdx] = true
		delete(m.bucketProccessing, res.TaskContent.CurIdx)
		if len(m.bucketProcceed) == len(m.buckets) {
			m.reduceDone = true
		}

		log.Printf("writing res.Count to file: [%+v]\n", m.ofile)
		for _, kv := range res.Count {
			fmt.Fprintf(m.ofile, "%v %v\n", kv.Key, kv.Value)
		}

		m.mu.Unlock()
		return nil
	}
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

	ret = m.MapDone() && m.ReduceDone()

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		files:           files,
		nReduce:         nReduce,
		timeoutDuration: 10 * time.Second,
		fileProcessing:  make(map[string]TaskInfo),

		// fileProcceed 保存的是已经处理完成的任务
		fileProcceed: make(map[string]bool),
		// reduceMap contains the word & worker id
		// reduceMap       map[string]int
		bucketProccessing: make(map[int]TaskInfo),
		bucketProcceed:    make(map[int]bool),
	}

	// Your code here.
	oname := "mr-out-0"
	ofile, _ := os.Create(oname)
	m.ofile = ofile

	m.buckets = make([]map[string][]string, m.nReduce)
	for i := range m.buckets {
		m.buckets[i] = make(map[string][]string)
	}
	m.server()
	return &m
}
