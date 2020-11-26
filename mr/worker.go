package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"sync"
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

	// executeTask(mapf, reducef)
	var mu sync.Mutex
	var flag bool
	var wg sync.WaitGroup
	var ch = make(chan struct{}, 10)

	for !flag {
		ch <- struct{}{}
		wg.Add(1)
		go func(mapf func(string, string) []KeyValue,
			reducef func(string, []string) string) {
			var t = executeTask(mapf, reducef)
			if t == NONE {
				mu.Lock()
				flag = true
				mu.Unlock()
			}
			<-ch
			wg.Done()
		}(mapf, reducef)
		time.Sleep(1 * time.Second)
	}
	wg.Wait()
	log.Printf("every task is done, bye bye")

	// uncomment to send the Example RPC to the master.

	// CallExample()

}
func executeTask(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) TaskType {
	var worker = WorkerInfo{
		WorkerID: workID(),
	}
	var task = Task{}
	call("Master.FetchTask", &worker, &task)
	if task.CurType == NONE {
		log.Printf("there is no task to execute: [%+v]\n", task)
		return NONE
	}
	if task.CurType == MAP {
		intermidate := mapf(task.FileName, task.Content)
		log.Printf("the len of data out of mapf is [%+v]\n", len(intermidate))

		var res = Result{
			TaskContent: task,
			WorkerID:    worker.WorkerID,
			Words:       intermidate,
		}
		var code = TaskExecuteStatus{}
		call("Master.SubmitResult", &res, &code)
		log.Printf("the status code of submit is [%+v]\n", code)
		return MAP
	}
	if task.CurType == REDUCE {
		var count = []KeyValue{}
		for word, vs := range task.Words {
			var c = reducef(word, vs)
			count = append(count, KeyValue{
				Key:   word,
				Value: c,
			})
		}
		var res = Result{
			TaskContent: task,
			WorkerID:    worker.WorkerID,
			Count:       count,
		}
		var code = TaskExecuteStatus{}
		call("Master.SubmitResult", &res, &code)
		log.Printf("the status code of submit is [%+v]\n", code)
		return REDUCE
	}
	return NONE
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
