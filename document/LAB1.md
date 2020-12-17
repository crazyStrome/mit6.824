---
title: CS6.824 Lab1
tag:
  - golang
  - 分布式
---

#  CS6.824 Lab1 

6.824 Lab1的具体要求见如下链接：

[lab1](https://pdos.csail.mit.edu/6.824/labs/lab-mr.html)

建议：做lab之前建议先读一下该课程给的论文，然后学习它的上课视频。论文在[schedule](https://pdos.csail.mit.edu/6.824/schedule.html)中都给出了，上课视频有同学搬运到了B站，搜索关键字就可以看到。

lab1的内容是实现一个MapReduce服务，主要包括一个Master和许多个Worker，Worker向Master获取任务，执行之后再汇总给Master。lab1中已经实现了map函数和reduce函数，用来执行任务，但是任务的调度等则需要具体实现。

这个lab看起来很简单，但是因为涉及到并发则比较麻烦。处理并发的方法一般是给临界资源加锁，但是加锁的粒度和顺序也有待商榷，粒度太大会导致并发效率低下，而锁的粒度小的话，加锁顺序不一致会导致死锁。

Golang的思想是“通过通信来共享内存，而不是通过共享内存来通信”，所以可以通过golang的channel来实现并发的处理。

##  MapReduce结构体

WorkerInfo是和Worker相关的结构体对象，内部包含了Worker的ID。Worker通过RPC向Master请求任务时会携带WorkerInfo信息。

```golang
type WorkerInfo struct {
	ID string
}
```

Task包含了Master分配给Worker的任务的具体内容。

* ID： Master分配唯一任务ID
* Type： Master分配任务类型。分别有NOTASK、MAP、REDUCE、RETRY、COMPLETE
* Content：Master分配任务的具体内容。当任务类型是MAP时，Content是需要计算词频的文件内容；当任务类型是REDUCE时，Content是Bucket序列化的json字符串
* FileName： Master分配任务的文件名。当任务类型是MAP时，FileName是需要计算词频的文件名；当任务类型是REDUCE时，FileName是Worker需要保存到的文件名，但是Worker不会保存到该文件中，而是先保存到一个临时文件，再通过Master把该文件命名为FileName。

```golang
type Task struct {
	ID       string
	Type     TaskType
	Content  string
	FileName string
	Worker   *WorkerInfo
}
```

TaskType表示Task的具体类型。

* NOTASK： Master没有可用的Task可以分配给Worker，此时表示所有的工作已经完成了；Worker收到此类任务类型的话直接退出执行任务。
* MAP： Master分配了一个Map任务给Worker
* REDUCE： Master分配了一个REDUCE任务给Worker
* RETRY： Master当前没有可分配的任务，所有的任务要么正在执行要么已经执行成功了；Worker会等待一段时间再向Master获取任务。
* COMPLETE： 在Worker提交完执行结果后，Master把该任务标记为COMPLETE

```golang
type TaskType uint8
const (
	NOTASK TaskType = iota
	MAP
	REDUCE
	RETRY
	COMPLETE
)
```

Result中包含了Worker执行Task后的结果。

* Type： 执行的任务类型，如果Worker在执行过程中出错的话会返回一个NOTASK的Result，Master收到后会返回一个FAIL的statusCode
* TaskFileName：执行的Task中的FileName，便于Master通过FileName查找对应的Task
* TaskID： 执行的Task中的ID
* Content：如果是Map任务的执行结果，Content中是Map获得的KeyValue切片构成的json字符串；如果是Reduce任务的执行结果，Content中则是Worker创建的临时文件名，Master收到后会进行rename。

```golang
type Result struct {
	Type         TaskType
	TaskFileName string
	TaskID       string
	Content      string
}
```

Status包含Master回复Worker执行的任务结果，其中包含了一个StatusCode。

* SUCCESS： 表示任务执行成功，Master会标记相关的任务为完成；Worker继续获取下一个任务。
* TIMEOUT： Master发现该返回结果的Worker和之前记录的Worker不同或者不存在记录中，说明该Task已经因为超时被移除了，Worker收到该回复后也会退出不再执行。
* FAIL： 在执行过程中，如果Worker执行出错，会返回一个NOTASK的Result；或者Master在处理Result的过程出错都会返回FAIL给Worker。

```golang
type StatusCode int

const (
	SUCCESS StatusCode = iota
	TIMEOUT
	FAIL
)

type Status struct {
	ResCode StatusCode
}
```

##  Master实现

Master是表示服务端的一个实体，用来实现任务分配等相关内容。

* workerCh： 用来传输workerInfo
* taskCh： 用来传输Task
* resultCh： 用来传输Result
* statusCh： 用来传输Status
* taskMap： 记录FileName对应的Task，不论是Map任务还是Reduce任务
* removeTaskCh：传输FileName，将taskMap中FileName对应的Task删除。在超时的时候使用该channel
* taskMapDone： 记录FileName对应的Done channel，Done channel是一个随Task一起创建的channel，当任务完成时会调用Close(Done)实现关闭广播。
* removeDoneCh： 传输FileName，将taskMapDone中FileName对应的Done删除掉。在超时或者close(done)时都会使用。
* files：记录了需要使用Map函数处理的文件列表，使用map来保存是为了方便删除，当对文件读取出错时会将该文件删除，同时mapTaskCount++
* mapTaskCount： 记录执行完成的Map任务，当Worker返回的Result中有正确的数据时才表示执行完成，将mapTaskCount++；此外，Master读取文件出错时也会将mapTaskCount++
* nReduce： Master将reduce的分片数量，在初始化的时候赋值
* reduceBuckets： Master把map任务的结果KeyValue对分别通过Key的hash值放到对应的Bucket，Bucket是一个`map[string][]string`类型，保存Key以及他对应的Value切片
* reduceTaskCount：记录执行完成的Reduce任务，当Worker返回的Result中有正确的数据
* doneReq：向Master查询是否完成的通道
* doneReply：Master回复是否完成的channel

```golang
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
```

###  后台poller

Master在后台开启一个goroutine，用来处理channel到达的请求。如果是workerCh，会调用getTask获取任务；如果是resultCh，会调用submit提交任务；如果是removeTaskCh，会从taskMap中删除fileName对应的Task；如果是removeDoneCh，会通taskMapDone中删除对应的Done channel；如果是doneReq，说明有调用者查询是否任务全完成了，通过判断len(files)==mapTaskCount && len(reduceButkets) == reduceTaskCount

```golang
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
```

###  getTask

这个方法是用来获取可用的任务的。

如果mapTaskCount<len(files)，意味着map任务还没有完全执行完，就可以进行Map任务的分配。

* 首先判断len(files)==len(m.taskMap)，如果相等，则表明当前所有的Map任务都被分配出去了，并没有完全执行结束，Master会将task.Type设置为RETRY，表明Worker可以重新获取任务，等待别的任务超时失效。
* Master从files中查找一个没有被分配的fileName，读取它的内容，生成一个新的Task，并记录在TaskMap中；同时，构建一个done channel并开启一个新的goroutine，监控done和一个超时定时器，如果任务超时，Master会通过removeTaskCh把记录的Task删除。

如果nReduce > reduceTaskCount，意味着reduce任务还没有完全执行完，进行reduce任务的分配。

* 如果len(taskMap)==len(files)+nReduce，说明当前的所有任务都分配出去了但是没有完全执行完成，Master将Task设置为RETRY，然后Worker可以重新获取任务，等待别的任务超时失效。
* Master从reduceBuckets查找一个没有记录的Bucket，封装为Task，记录在TaskMap中，同时设置超时取消。

###  submit

Master通过submit判断Worker是否执行完成了。

* 如果result.Type == NOTASK，说明Worker出错了，没有返回内容。需要把这个任务删除掉，并返回FAIL作为ResCode。代码中没有实现删除任务，因为等待任务超时之后会自动删除任务的。
* 通过result.TaskFileName在taskMap中查找，如果已经不存在对应的Task或者Task.ID和result.TaskID不同，说明该Task已经因为超时删除了，Master会返回TIMEOUT作为ResCode
* 接下来处理Worker返回的数据，如果是MAP任务，就把每个Key对应的Values对放在相应的reduceBucket中，通过Key的hash值查找对应的Bucket；如果是REDUCE任务，就把Content中的临时文件名Rename为FileName

##  Worker实现

Worker就比较简单了，其实就是不停的获取任务然后执行任务。这部分代码实现在singleWorker函数中，在Worker主函数中使用limit来控制并发数量，然后使用WaitGroup等待所有goroutine退出。

```golang
	var limit = make(chan struct{}, 10)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		limit <- struct{}{}
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			singleWorker(mapf, reducef)
			<-limit
		}(&wg)
	}
	wg.Wait()
```

但是在实际实现中，只使用了一个singleWorker来循环执行就可以了。

###  singleWorker实现

singleWorker实现很简单，就是不停的getTask、执行任务然后submit结果。

* 首先判断Task.Type，如果是NOTASK，说明当前Master的所有任务都已经完成了，singleWorker就直接退出了
* 如果是RETRY，singleWorker会sleep一段时间然后重新开始循环执行任务
* 如果是MAP，singleWorker会把map函数的结果Marshal为json字符串返回
* 如果是REDUCE，singleWorker会把Task.Content内容储存到一个临时文件中，然后把临时文件名放到result.Content中返回给Master
* singleWorker通过判断Result的内容，如果不是SUCCESS，说明任务执行失败，singleWorker就会直接退出

##  总结

Lab1的MapReduce说简单也简单，说难也难。我一开始使用了各种锁来实现并发的处理，但是因为锁加的顺序容易导致死锁，运行的时候也会产生数据的竞争。所以我就是用go的特性，直接使用channel来进行所有的处理，把所有对数据的操作都放到一个goroutine中。可能有人会说，所有的操作都放到一个goroutine中，不会导致任务被积压吗，其实不会的，这些数据操作都是很快的，使用的系统调用也是很少的，这些用时相比于Worker的网络和磁盘IO来说都是很短的，所以这个模式也是可以使用的。