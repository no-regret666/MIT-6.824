package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files   []string
	nReduce int

	mapTasks    []MapReduceTask
	reduceTasks []MapReduceTask

	mapTasksDone    bool
	reduceTasksDone bool
	mu              sync.Mutex
}
type MapReduceTask struct {
	taskId      int
	taskType    string
	status      int //0:未开始 1:正在进行 2:已完成
	startTime   time.Time
	mapFile     string
	reduceFiles []string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Handle(args *Args, reply *Reply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.mapTasksDone {

	}
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.nReduce = nReduce
	c.mapTaskId = 0
	c.mapStatus = make(map[string]int)
	for _, file := range files {
		c.mapStatus[file] = 0
	}
	c.reduceStatus = make(map[int]int)
	for i := 0; i < nReduce; i++ {
		c.reduceStatus[i] = 0
	}
	c.intermediateFiles = make(map[int][]string)
	c.mapTasksDone = false
	c.reduceTasksDone = false

	c.server()
	return &c
}
