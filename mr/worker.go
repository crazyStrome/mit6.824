package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// var limit = make(chan struct{}, 10)
	// var wg sync.WaitGroup
	// for i := 0; i < 20; i++ {
	// 	wg.Add(1)
	// 	limit <- struct{}{}
	// 	go func(wg *sync.WaitGroup) {
	// 		defer wg.Done()
	// 		singleWorker(mapf, reducef)
	// 		<-limit
	// 	}(&wg)
	// }
	// wg.Wait()
	singleWorker(mapf, reducef)
	singleWorker(mapf, reducef)
	// // singleWorker(mapf, reducef)
	// CallExample()

}
func singleWorker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	var worker = &WorkerInfo{
		ID: obtainWorkerID(),
	}
	for {
		task := fetchTask(worker)
		if task.Type == NOTASK {
			break
		}
		if task.Type == MAP {
			log.Printf("worker [%s] has fetched a MAP task with fileName [%s]\n", worker.ID, task.FileName)
			kvs := mapf(task.FileName, task.Content)
			var res = Result{}
			// 如果err!=nil，意味着转换失败``
			var bs, err = json.Marshal(kvs)
			if err != nil {
				res.Type = NOTASK
			} else {
				res.Type = MAP
			}
			res.Content = string(bs)
			res.TaskID = task.ID
			res.TaskFileName = task.FileName

			var status = submitResult(&res)
			log.Printf("worker [%s] submit result of MAP task [%s] with fileName of [%s], and get resultCode [%q]\n", worker.ID, task.ID, task.FileName, status)

			if status.ResCode != SUCCESS {
				break
			}
		} else if task.Type == REDUCE {
			// worker 执行reduce任务是把结果写到一个临时文件中，然后由master把临时文件名改为目标文件名
			log.Printf("worker [%s] has fetched a REDUCE task with fileName [%s]\n", worker.ID, task.FileName)
			var bt Bucket
			var res = Result{}
			var err = json.Unmarshal([]byte(task.Content), &bt)
			log.Printf("worker [%s] get the data [%d] in REDUCE task [%s] with fileName [%s]\n", worker.ID, len(bt), task.ID, task.FileName)
			if err != nil {
				res.Type = NOTASK
			} else {
				res.Type = REDUCE
			}
			var tName = fmt.Sprintf("%d%s", time.Now().UnixNano(), task.FileName)
			var ofile, _ = os.Create(tName)
			for k, vs := range bt {
				var out = reducef(k, vs)
				fmt.Fprintf(ofile, "%v %v\n", k, out)
			}
			ofile.Close()
			res.TaskFileName = task.FileName
			res.TaskID = task.ID
			res.Content = tName

			var status = submitResult(&res)
			log.Printf("worker [%s] submit result of REDUCE task [%s] with fileName of [%s] and tempfile [%s], and get resultCode [%q]\n", worker.ID, task.ID, task.FileName, tName, status)
			if status.ResCode != SUCCESS {
				break
			}
		} else if task.Type == RETRY {
			log.Printf("worker [%s] has fetched a RETRY task [%s]\n", worker.ID, task.ID)
			time.Sleep(2 * time.Second)
		}

	}
	log.Printf("worker [%s] has no task to do, bye\n", worker.ID)
}
func fetchTask(worker *WorkerInfo) *Task {
	var task = Task{}
	call("Master.DistributeTask", worker, &task)
	return &task
}
func submitResult(res *Result) *Status {
	var status = Status{}
	call("Master.SubmitResult", res, &status)
	return &status
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {

	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	log.Printf("worker called rpc with name: [%s]\n", rpcname)
	return false
}
