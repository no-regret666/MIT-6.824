package mr

import (
	"fmt"
	"log"
	"sync"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	mapTaskNum      int //map任务总数
	reduceTaskNum   int //reduce任务总数
	mapTask         []Task
	reduceTask      []Task
	mapTasksDone    bool //map任务是否全部完成
	reduceTasksDone bool //reduce任务是否全部完成

	mu sync.Mutex
}

type Task struct {
	taskType string
	taskId   int
	status   int //0:未开始 1:正在进行 2:已完成

	//map
	mapTask string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) taskNum(args *TaskNumArgs, reply *TaskNumReply) error {
	reply.MapTaskNum = c.mapTaskNum
	reply.ReduceTaskNum = c.reduceTaskNum
	return nil
}

func (c *Coordinator) assignTask(args *TaskArgs, reply *Task) error {

}

func (c *Coordinator) taskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {

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
	c.mapTaskNum = len(files)
	c.reduceTaskNum = nReduce
	for i, file := range files {
		c.mapTask = append(c.mapTask, Task{
			taskType: "map",
			taskId:   i,
			status:   0,
			mapTask:  file,
		})
	}
	for i := 0; i < nReduce; i++ {
		c.reduceTask = append(c.reduceTask, Task{
			taskType: "reduce",
			status:   0,
			taskId:   i,
		})
	}
	c.mapTasksDone = false
	c.reduceTasksDone = false

	c.mu = sync.Mutex{}
	fmt.Printf("Coodinator 初始化完毕!")
	c.server()
	return &c
}
