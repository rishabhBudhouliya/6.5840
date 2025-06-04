package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
)

// status can be
// A not started yet
// B in progress
// C done
type TaskInfo struct {
	inputFilename        string
	status               string
	intermediateFilename []string
}

type Coordinator struct {
	// a map for mapping between task id and their TaskInfo
	taskmap map[string]TaskInfo
	nReduce int
}

func (c *Coordinator) getStatusByTaskId(taskId string) string {
	taskInfo := c.taskmap[taskId]
	return taskInfo.status
}

func (c *Coordinator) setStatusByTaskId(taskId string, status string) {
	taskInfo := c.taskmap[taskId]
	taskInfo.status = status
	c.taskmap[taskId] = taskInfo
}

func (c *Coordinator) getIntermediateFilesByTaskId(taskId string) []string {
	taskInfo := c.taskmap[taskId]
	return taskInfo.intermediateFilename
}

func (c *Coordinator) setIntermediateFilesByTaskId(taskId string, intermediateFile []string) {
	taskInfo := c.taskmap[taskId]
	taskInfo.intermediateFilename = intermediateFile
	c.taskmap[taskId] = taskInfo
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *WorkerArgs, reply *CoordinatorReply) error {
	fmt.Printf("Got a task request!! from worker %s", args.WorkerId)
	if args.WorkerId != "" && c.getStatusByTaskId(args.WorkerId) != "A" {
		return errors.New("task already in progress or stalled")
	}
	for taskId, taskInfo := range c.taskmap {
		if taskInfo.status == "A" {
			reply.WorkerId = taskId
			reply.Filename = taskInfo.inputFilename
			reply.NReduce = c.nReduce
			c.setStatusByTaskId(taskId, "B")
			break
		}
	}
	return nil
}

func (c *Coordinator) FinishTask(args *WorkerFinishTaskArgs, reply *WorkerFinishTaskReply) error {
	taskId := args.WorkerId
	status := c.getStatusByTaskId(taskId)
	// TODO: check if a valid file exist or not - don't depend on just the name
	if status == "B" && len(args.IntermediateFilenames) > 0 {
		c.setStatusByTaskId(taskId, "A") // put the worker to idle so that it can start some reduce stuff
		c.setIntermediateFilesByTaskId(taskId, args.IntermediateFilenames)
	}
	fmt.Printf("Worker %s came with his files: %v\n", taskId, args.IntermediateFilenames)
	return nil
}

func (c *Coordinator) GetReduceTask(args *WorkerReduceTaskArgs, reply *WorkerReduceTaskReply) error {
	taskId := args.WorkerId
	fmt.Printf("Got a reduce task request!! from worker %s", taskId)
	status := c.getStatusByTaskId(taskId)
	// only assign work if the worker is idle
	if args.WorkerId != "" && status != "A" {
		return errors.New("task needs to be idle to pick up reduce task")
	}
	for taskId, taskInfo := range c.taskmap {
		if taskInfo.status == "A" && c.nReduce == len(taskInfo.intermediateFilename) {
			reply.WorkerId = taskId
			reply.IntermediateFilenames = taskInfo.intermediateFilename
			reply.NReduce = c.nReduce
			c.setStatusByTaskId(taskId, "B")
			break
		}
	}
	return nil
}

func (c *Coordinator) MarkTaskDone(args *WorkerFinishTaskArgs, reply *WorkerFinishTaskReply) error {
	taskId := args.WorkerId
	status := c.getStatusByTaskId(taskId)
	if args.WorkerId != "" && status != "B" {
		return errors.New("task needs to be in-progress to be transition done")
	}
	taskInfo := c.taskmap[taskId]
	taskInfo.status = "C"
	c.taskmap[taskId] = taskInfo
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
	ret := true
	for _, taskInfo := range c.taskmap {
		if taskInfo.status != "C" {
			ret = false
		}
	}
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.taskmap = make(map[string]TaskInfo, len(files))
	for i, filename := range files {
		c.taskmap[strconv.Itoa(i)] = TaskInfo{status: "A", inputFilename: filename}
	}
	c.nReduce = nReduce
	c.server()
	return &c
}
