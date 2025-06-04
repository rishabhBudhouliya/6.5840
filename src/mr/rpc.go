package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type WorkerArgs struct {
	WorkerId string
}

type CoordinatorReply struct {
	Filename string
	WorkerId string
	NReduce  int
}

type WorkerFinishTaskArgs struct {
	WorkerId              string
	IntermediateFilenames []string
}

type WorkerFinishTaskReply struct{}

type WorkerReduceTaskArgs struct {
	WorkerId string
}

type WorkerReduceTaskReply struct {
	WorkerId              string
	IntermediateFilenames []string
	NReduce               int
}

type WorkerTaskDoneArgs struct {
	WorkerId string
}

type WorkerTaskDoneReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
