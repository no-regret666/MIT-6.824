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
	MapTaskNum      int //map任务总数
	ReduceTaskNum   int //reduce任务总数
	MapTask         []Task
	ReduceTask      []Task
	MapTasksDone    bool //map任务是否全部完成
	ReduceTasksDone bool //reduce任务是否全部完成

	Mu sync.Mutex
}

type Task struct {
	TaskType string
	TaskId   int
	Status   int //0:未开始 1:正在进行 2:已完成

	//map
	MapTask string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskNum(args *TaskNumArgs, reply *TaskNumReply) error {
	reply.MapTaskNum = c.MapTaskNum
	reply.ReduceTaskNum = c.ReduceTaskNum
	return nil
}

func (c *Coordinator) AssignTask(args *TaskArgs, reply *TaskReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	if !c.MapTasksDone {
		for i, _ := range c.MapTask {
			if c.MapTask[i].Status == 0 {
				reply.Task = c.MapTask[i]
				//fmt.Printf("assign map %d\n", c.ReduceTask[i].TaskId)
				c.MapTask[i].Status = 1
			} else {
				continue
			}
		}
	} else {
		if !c.ReduceTasksDone {
			for i, _ := range c.ReduceTask {
				if c.ReduceTask[i].Status == 0 {
					reply.Task = c.ReduceTask[i]
					fmt.Printf("assign reduce %d\n", c.ReduceTask[i].TaskId)
					c.ReduceTask[i].Status = 1
				} else {
					continue
				}
			}
		} else {
			reply.Task.TaskType = ""
		}
	}
	return nil
}

func (c *Coordinator) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	c.Mu.Lock()
	defer c.Mu.Unlock()
	task := args.Task
	switch args.Msg {
	case "single":
		if task.TaskType == "map" {
			c.MapTask[task.TaskId].Status = 2
		} else {
			c.ReduceTask[task.TaskId].Status = 2
		}
	case "all":
		if task.TaskType == "map" {
			c.MapTask[task.TaskId].Status = 2
			c.MapTasksDone = true
		} else {
			c.ReduceTask[task.TaskId].Status = 2
			c.ReduceTasksDone = true
		}
	}
	reply.IsDone = true
	return nil
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
	if c.MapTasksDone && c.ReduceTasksDone {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	// Your code here.
	c.MapTaskNum = len(files)
	c.ReduceTaskNum = nReduce
	for i, file := range files {
		c.MapTask = append(c.MapTask, Task{
			TaskType: "map",
			TaskId:   i,
			Status:   0,
			MapTask:  file,
		})
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTask = append(c.ReduceTask, Task{
			TaskType: "reduce",
			Status:   0,
			TaskId:   i,
		})
	}
	c.MapTasksDone = false
	c.ReduceTasksDone = false

	c.Mu = sync.Mutex{}
	//fmt.Printf("Coodinator 初始化完毕!")
	c.server()
	return &c
}
