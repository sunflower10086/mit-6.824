package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

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

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		task := CallGetTask()
		// fmt.Println("Worker", task.TaskId, task.TaskState)
		switch task.TaskState {
		case Map:
			doMapTask(mapf, task)
		case Reduce:
			doReduceTask(reducef, task)
		case Wait:
			time.Sleep(2 * time.Second)
		case Exit:
			return
		}
	}
}

func doMapTask(mapf func(string, string) []KeyValue, task *Task) {
	content, err := os.ReadFile(task.Input)
	if err != nil {
		log.Fatal("Failed to read file: "+task.Input, err)
	}

	intermediates := mapf(task.Input, string(content))

	//缓存后的结果会写到本地磁盘，并切成R份
	//切分方式是根据key做hash
	buffer := make([][]KeyValue, task.ReduceNum)

	for _, intermediate := range intermediates {
		slot := ihash(intermediate.Key) % task.ReduceNum
		buffer[slot] = append(buffer[slot], intermediate)
	}

	// TODO: 额外写一个函数，用于把map产生的结果存储到本地
	// 存储每个文件的名字
	mapOutput := make([]string, 0)
	for i := 0; i < task.ReduceNum; i++ {
		mapOutput = append(mapOutput, writeToLocalFile(task.TaskId, i, &buffer[i]))
	}
	task.Intermediates = mapOutput
	TaskCompleted(task)
}

// 返回添加文件的文件名字
func writeToLocalFile(taskId int, Id int, kvs *[]KeyValue) string {
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	enc := json.NewEncoder(tempFile)
	for _, kv := range *kvs {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-%d-%d", taskId, Id)
	os.Rename(tempFile.Name(), outputName)
	return filepath.Join(dir, outputName)
}

func doReduceTask(reducef func(string, []string) string, task *Task) {
	// TODO: 额外一个函数用于从Coordinator中取出中间结果的文件名字
	intermediate := *readFromLocalFile(task.Intermediates)

	sort.Sort(ByKey(intermediate))

	dir, _ := os.Getwd()
	tempFile, err := os.CreateTemp(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
	os.Rename(tempFile.Name(), oname)
	task.Output = oname
	TaskCompleted(task)
}

func readFromLocalFile(files []string) *[]KeyValue {
	var kva []KeyValue
	for _, filepath := range files {
		file, err := os.Open(filepath)
		if err != nil {
			log.Fatal("Failed to open file "+filepath, err)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return &kva
}

func TaskCompleted(task *Task) {
	reply := ExampleReply{}
	ok := call("Coordinator.TaskCompleted", task, &reply)
	if !ok {
		fmt.Printf("TaskCompleted 失败 Task类型为%d, TaskId 为%d\n", task.TaskState, task.TaskId)
	}
}

func CallGetTask() *Task {
	args := ExampleArgs{}
	resp := Task{}

	// fmt.Println("Worker CallGetTask use")

	ok := call("Coordinator.GetTask", &args, &resp)
	if !ok {
		log.Fatal("Coordinator.GetTask err")
	}

	return &resp
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
