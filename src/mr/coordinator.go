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
	filename string
	status   string
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

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *WorkerArgs, reply *CoordinatorReply) error {
	fmt.Printf("Got a task request!! from worker %s", args.WorkerId)
	if args.WorkerId != "" && c.getStatusByTaskId(args.WorkerId) != "A" {
		return errors.New("task already in progress or stalled")
	}
	for taskId, taskInfo := range c.taskmap {
		if taskInfo.status == "A" {
			reply.WorkerId = taskId
			reply.Filename = taskInfo.filename
			reply.NReduce = c.nReduce
			c.setStatusByTaskId(taskId, "B")
			break
		}
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
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.taskmap = make(map[string]TaskInfo, len(files))
	for i, filename := range files {
		c.taskmap[strconv.Itoa(i)] = TaskInfo{status: "A", filename: filename}
	}
	c.nReduce = nReduce
	c.server()
	return &c
}
