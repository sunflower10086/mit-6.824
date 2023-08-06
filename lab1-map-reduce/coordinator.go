package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskState int

const (
	// Idle 还未开始
	Idle TaskState = iota
	// InProgress 正在进行
	InProgress
	// Completed 已经结束
	Completed
)

// 把任务的状态和master的状态合在一起
type State int

const (
	Map State = iota
	Reduce
	Exit
	Wait
)

var mu sync.Mutex

type Coordinator struct {
	// Your definitions here.
	TasksQueue       chan *Task // 所有的任务
	TaskMeta         map[int]*TaskMeta
	CoordinatorPhase State // Coordinator的阶段
	ReduceNum        int
	InputFileName    []string
	Intermediates    [][]string // Map任务产生的R个中间文件的信息
}

type Task struct {
	Input         string
	TaskId        int
	TaskState     State
	ReduceNum     int
	Intermediates []string // Map产生的中间结果
	Output        string   // 产生的结果的名字
}

type TaskMeta struct {
	TaskState     TaskState
	StartTime     time.Time
	TaskReference *Task
}

// Your code here -- RPC handlers for the worker to call.

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
	mu.Lock()
	defer mu.Unlock()

	ret := c.CoordinatorPhase == Exit

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		TasksQueue:       make(chan *Task, max(len(files), nReduce)),
		TaskMeta:         make(map[int]*TaskMeta),
		CoordinatorPhase: Map,
		ReduceNum:        nReduce,
		InputFileName:    files,
		Intermediates:    make([][]string, nReduce),
	}

	// 切成16MB-64MB的文件
	// 创建map任务
	c.createMapTask()
	// Your code here.

	c.server()

	go c.catchTimeOut()
	return &c
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func (c *Coordinator) catchTimeOut() {
	for {
		time.Sleep(5 * time.Second)
		mu.Lock()
		// 所有作业已经完成
		if c.CoordinatorPhase == Exit {
			mu.Unlock()
			return
		}

		// 判断作业是否超时，如果超时要从新设置作业
		for i, taskMeta := range c.TaskMeta {
			// 说明运行超时
			if taskMeta.TaskState == InProgress && time.Now().Sub(taskMeta.StartTime) > 10*time.Second {
				c.TasksQueue <- taskMeta.TaskReference
				c.TaskMeta[i].TaskState = Idle
			}
		}
		mu.Unlock()
	}
}

func (c *Coordinator) createMapTask() {
	for idx, filename := range c.InputFileName {
		taskMeta := Task{
			Input:     filename,
			TaskId:    idx,
			TaskState: Map,
			ReduceNum: c.ReduceNum,
		}

		c.TasksQueue <- &taskMeta

		c.TaskMeta[idx] = &TaskMeta{
			TaskState:     Idle,
			TaskReference: &taskMeta,
		}
	}
}

func (c *Coordinator) createReduceTask() {
	c.TaskMeta = make(map[int]*TaskMeta)
	for idx, intermediate := range c.Intermediates {
		taskMeta := Task{
			TaskId:        idx,
			TaskState:     Reduce,
			ReduceNum:     c.ReduceNum,
			Intermediates: intermediate,
		}

		c.TasksQueue <- &taskMeta

		c.TaskMeta[idx] = &TaskMeta{
			TaskState:     Idle,
			TaskReference: &taskMeta,
		}
		// fmt.Printf("第%dReduce\n", idx)
	}
}

func (c *Coordinator) GetTask(req *ExampleArgs, resp *Task) error {
	mu.Lock()
	defer mu.Unlock()

	// fmt.Println(c.CoordinatorPhase)

	// switch c.CoordinatorPhase {
	// case Map:
	// 	*resp = *<-c.MapTasks
	// 	c.TaskMeta[resp.TaskId].TaskState = InProgress
	// 	c.TaskMeta[resp.TaskId].StartTime = time.Now()
	// case Reduce:
	// 	*resp = *<-c.ReduceTasks
	// 	// fmt.Printf("Reduce任务 %d\n", resp.TaskId)
	// 	c.TaskMeta[resp.TaskId].TaskState = InProgress
	// 	c.TaskMeta[resp.TaskId].StartTime = time.Now()
	// case Exit:
	// 	*resp = Task{TaskState: Exit}
	// default:
	// 	*resp = Task{TaskState: Wait}
	// }
	if len(c.TasksQueue) > 0 {
		*resp = *<-c.TasksQueue
		c.TaskMeta[resp.TaskId].TaskState = InProgress
		c.TaskMeta[resp.TaskId].StartTime = time.Now()
	} else if c.CoordinatorPhase == Wait {
		*resp = Task{TaskState: Wait}
	} else {
		*resp = Task{TaskState: Exit}
	}

	return nil
}

func (c *Coordinator) TaskCompleted(task *Task, reply *ExampleReply) error {
	//更新task状态
	mu.Lock()
	defer mu.Unlock()
	// 这个作业的状态和当前master的状态不一致，或者这个任务已经完成，直接返回
	if task.TaskState != c.CoordinatorPhase || c.TaskMeta[task.TaskId].TaskState == Completed {
		// 因为worker写在同一个文件磁盘上，对于重复的结果要丢弃
		return nil
	}

	// 更新一下这个任务的状态
	c.TaskMeta[task.TaskId].TaskState = Completed
	go c.processTaskResult(task)

	// fmt.Println("TaskCompleted ---end---")

	return nil
}

func (c *Coordinator) processTaskResult(task *Task) {
	mu.Lock()
	defer mu.Unlock()
	switch task.TaskState {
	case Map:
		//收集intermediate信息
		for reduceTaskId, filePath := range task.Intermediates {
			c.Intermediates[reduceTaskId] = append(c.Intermediates[reduceTaskId], filePath)
		}
		if c.allTaskDone() {
			//获得所以map task后，进入reduce阶段
			c.createReduceTask()
			c.CoordinatorPhase = Reduce
		}
	case Reduce:
		if c.allTaskDone() {
			//获得所有reduce task后，进入exit阶段
			c.CoordinatorPhase = Exit
		}
	}

}

func (c *Coordinator) allTaskDone() bool {
	for _, task := range c.TaskMeta {
		if task.TaskState != Completed {
			return false
		}
	}
	return true
}
