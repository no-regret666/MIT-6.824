package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	taskNumArgs := TaskNumArgs{}
	taskNumReply := TaskNumReply{}
	call("Coordinator.TaskNum", &taskNumArgs, &taskNumReply)
	mapNum := taskNumReply.MapTaskNum
	reduceNum := taskNumReply.ReduceTaskNum

	for {
		taskArgs := TaskArgs{}
		taskReply := TaskReply{}
		call("Coordinator.AssignTask", &taskArgs, &taskReply)
		task := taskReply.Task
		switch task.TaskType {
		case "":
			//log.Printf("所有任务完成!")
			break
		case "map":
			doMapTask(task.MapTask, task.TaskId, reduceNum, mapf)
			if task.TaskId < mapNum {
				taskDone("single", task)
			} else {
				taskDone("all", task)
			}
		case "reduce":
			doReduceTask(task.TaskId, mapNum, reducef)
			if task.TaskId < reduceNum {
				taskDone("single", task)
			} else {
				taskDone("all", task)
			}
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// 取消注释以将Example RPC发给协调器
	// CallExample()

}

func taskDone(msg string, task Task) bool {
	taskDoneArgs := TaskDoneArgs{}
	taskDoneArgs.Msg = msg
	taskDoneArgs.Task = task
	taskDoneReply := TaskDoneReply{}
	call("Coordinator.TaskDone", &taskDoneArgs, &taskDoneReply)
	return taskDoneReply.IsDone
}

func doMapTask(InputFile string, taskId int, nReduce int, mapf func(string, string) []KeyValue) {
	// 解析Map函数参数key/value
	file, err := os.Open(InputFile)
	if err != nil {
		log.Fatalf("cannot open file %v", InputFile)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file %v", InputFile)
	}
	defer file.Close()
	kva := mapf(InputFile, string(content))

	for _, kv := range kva {
		reduceId := ihash(kv.Key) % nReduce
		filename := fmt.Sprintf("mr-%d-%d", taskId, reduceId)
		file, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
		if err != nil {
			log.Fatalf("cannot open file %v", filename)
		}
		enc := json.NewEncoder(file)
		enc.Encode(&kv)
		file.Close()
	}
}

func doReduceTask(taskId int, nMap int, reducef func(string, []string) string) {
	kva := []KeyValue{}
	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, taskId)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open file %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	oname := fmt.Sprintf("mr-out-%d", taskId)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot open file %v", oname)
	}
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)

	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
	return false
}
