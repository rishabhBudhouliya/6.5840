package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

// status can be
// A not started yet
// B in progress
// C done
type TaskInfo struct {
	inputFilename        string
	status               string
	intermediateFilename []string
	createdAt            time.Time
}

type Coordinator struct {
	mu                 sync.Mutex
	taskmap            map[string]TaskInfo
	intermediateState  [][]string
	reduceTaskMap      map[string]TaskInfo
	mapTasksCounter    int
	reduceTasksCounter int
	nReduce            int
}

func (c *Coordinator) incrementMapTasksCounter() {
	c.mapTasksCounter++
}

func (c *Coordinator) decrementMapTasksCounter() {
	c.mapTasksCounter--
}

func (c *Coordinator) incrementReduceTasksCounter() {
	c.reduceTasksCounter++
}

func (c *Coordinator) decrementReduceTasksCounter() {
	c.reduceTasksCounter--
}

func (c *Coordinator) getStatusByTaskId(taskId string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	taskInfo := c.taskmap[taskId]
	return taskInfo.status
}

func (c *Coordinator) getReduceStatusByTaskId(taskId string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	taskInfo := c.reduceTaskMap[taskId]
	return taskInfo.status
}

func (c *Coordinator) setStatusByTaskId(taskId string, status string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	taskInfo := c.taskmap[taskId]
	taskInfo.status = status
	c.taskmap[taskId] = taskInfo
}

func (c *Coordinator) getIntermediateFilesByTaskId(taskId string) []string {
	taskInfo := c.taskmap[taskId]
	return taskInfo.intermediateFilename
}

func (c *Coordinator) setIntermediateFilesByTaskId(taskId string, intermediateFile []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	taskInfo := c.taskmap[taskId]
	taskInfo.intermediateFilename = intermediateFile
	c.taskmap[taskId] = taskInfo
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *WorkerArgs, reply *CoordinatorReply) error {
	// fmt.Printf("Got a task request!! from worker %s", args.WorkerId)
	// if args.WorkerId != "" && c.getStatusByTaskId(args.WorkerId) != "A" {
	// 	return errors.New("task already in progress or stalled")
	// }
	c.mu.Lock()
	for taskId, taskInfo := range c.taskmap {
		if taskInfo.status == "A" {
			taskInfo.createdAt = time.Now()
			reply.WorkerId = taskId
			reply.Filename = taskInfo.inputFilename
			reply.NReduce = c.nReduce
			taskInfo.status = "B"
			c.taskmap[taskId] = taskInfo
			c.incrementMapTasksCounter()
			break
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) FinishTask(args *WorkerFinishTaskArgs, reply *WorkerFinishTaskReply) error {
	taskId := args.WorkerId
	status := c.getStatusByTaskId(taskId)
	// TODO: check if a valid file exist or not - don't depend on just the name
	if status == "B" && len(args.IntermediateFilenames) > 0 {
		c.setStatusByTaskId(taskId, "C") // put the worker to done for map that it can start some reduce stuff
		c.setIntermediateFilesByTaskId(taskId, args.IntermediateFilenames)
		c.addReduceTask(args.IntermediateFilenames)
		c.mu.Lock()
		defer c.mu.Unlock()
		c.decrementMapTasksCounter()
	}
	fmt.Printf("Worker %s came with his files: %v\n", taskId, args.IntermediateFilenames)
	return nil
}

func (c *Coordinator) scheduleUnprocessedTasks() bool {
	pending := false
	for taskId, taskInfo := range c.taskmap {
		if taskInfo.status == "B" && time.Since(taskInfo.createdAt) > 10*time.Second {
			pending = true
			taskInfo.status = "A"
			c.taskmap[taskId] = taskInfo
		}
	}
	return pending
}

func (c *Coordinator) GetReduceTask(args *WorkerReduceTaskArgs, reply *WorkerReduceTaskReply) error {
	taskId := args.WorkerId
	// fmt.Printf("Got a reduce task request!! from worker %s", taskId)
	status := c.getReduceStatusByTaskId(taskId)
	// only assign work if the worker is idle
	if args.WorkerId != "" && status != "A" {
		return errors.New("task needs to be idle to pick up reduce task")
	}
	c.mu.Lock()
	pendingMapTasks := c.scheduleUnprocessedTasks()
	//ensure all map tasks are done
	if pendingMapTasks {
		reply.WorkerId = "NA"
	} else {
		for taskId, taskInfo := range c.reduceTaskMap {
			if taskInfo.status == "A" {
				reply.WorkerId = taskId
				reply.IntermediateFilenames = taskInfo.intermediateFilename
				reply.NReduce = c.nReduce
				taskInfo.status = "B"
				c.reduceTaskMap[taskId] = taskInfo
				c.incrementReduceTasksCounter()
				break
			} else {
				reply.WorkerId = "NA"
			}
		}
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) MarkTaskDone(args *WorkerFinishTaskArgs, reply *WorkerFinishTaskReply) error {
	taskId := args.WorkerId
	status := c.getReduceStatusByTaskId(taskId)
	if args.WorkerId != "" && status != "B" {
		return errors.New("task needs to be in-progress to be transition done")
	}
	c.mu.Lock()
	taskInfo := c.reduceTaskMap[taskId]
	taskInfo.status = "C"
	c.reduceTaskMap[taskId] = taskInfo
	c.decrementReduceTasksCounter()
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) ExitWorker(args *ExitWorkerArgs, reply *ExitWorkerReply) error {
	if c.Done() {
		reply.IsDone = true
	} else {
		reply.IsDone = false
	}
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
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := true
	for _, taskInfo := range c.reduceTaskMap {
		// if any task is in progress, Done is false
		if taskInfo.status != "C" {
			ret = false
			return ret
		}
	}
	// if c.reduceTasksCounter != 0 {
	// 	ret = false
	// }
	// if c.reduceTasksCounter == 0 {
	// 	fmt.Printf("Counter is now zero")
	// }
	return ret
}

func (c *Coordinator) MapDone() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := true
	for _, taskInfo := range c.taskmap {
		// if any task is in progress, Done is false
		if taskInfo.status != "C" {
			ret = false
			return ret
		}
	}
	if c.mapTasksCounter != 0 {
		ret = false
	}
	return ret

}

func (c *Coordinator) addReduceTask(intermediateFileNames []string) {
	re := regexp.MustCompile(`mr-(\d+)-(\d+)`)
	var reduceBucket string
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, file := range intermediateFileNames {
		match := re.FindStringSubmatch(file)
		if len(match) > 1 {
			reduceBucket = match[2]
			index, error := strconv.Atoi(reduceBucket)
			if error == nil {
				c.intermediateState[index] = append(c.intermediateState[index], file)
				if len(c.intermediateState) == c.nReduce {
					for key := range c.intermediateState {
						c.reduceTaskMap[strconv.Itoa(key)] = TaskInfo{inputFilename: fmt.Sprintf("mr-out-%d", key),
							intermediateFilename: c.intermediateState[key], status: "A"}
					}
				}
			}
		}
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.taskmap = make(map[string]TaskInfo, len(files))
	c.reduceTaskMap = make(map[string]TaskInfo)
	c.intermediateState = make([][]string, nReduce)
	for i, filename := range files {
		c.taskmap[strconv.Itoa(i)] = TaskInfo{status: "A", inputFilename: filename}
		c.reduceTaskMap[strconv.Itoa(i)] = TaskInfo{}
		c.intermediateState[i] = []string{}
	}
	c.nReduce = nReduce
	c.server()
	return &c
}
