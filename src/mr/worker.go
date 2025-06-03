package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

type WorkerId struct {
	id string
}

var wid *WorkerId

func (w *WorkerId) setWorkerId(workerId string) {
	w.id = workerId
}

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

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	reply := CallForTask()
	// step 1: call coordinator for a task
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()
	kv := mapf(reply.Filename, string(content))
	files, err := createIntermediateFiles(reply.WorkerId, reply.NReduce)
	partition(files, kv, reply.NReduce)
	if err != nil {
		panic(err)
	}
}

func CallForTask() CoordinatorReply {
	wid = &WorkerId{}
	args := WorkerArgs{WorkerId: wid.id}
	reply := CoordinatorReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	fmt.Print(reply.Filename)
	wid.setWorkerId(reply.WorkerId)
	if !ok {
		fmt.Print("Coordinator get task failed!")
		// do something
	}
	return reply
}

func createIntermediateFiles(taskId string, nReduce int) ([]*os.File, error) {
	files := make([]*os.File, nReduce)
	for i := 0; i < nReduce; i++ {
		fileName := fmt.Sprintf("mr-%s-%d", taskId, i)
		f, err := os.Create(fileName)
		if err != nil {
			for j := 0; j < i; j++ {
				files[j].Close()
			}
			return nil, errors.New("unable to create a file")
		}
		files[i] = f
	}
	return files, nil
}

func partition(intermediateFiles []*os.File, intermediate []KeyValue, nReduce int) {
	for _, kv := range intermediate {
		bucket := ihash(kv.Key) % nReduce
		enc := json.NewEncoder(intermediateFiles[bucket])
		enc.Encode(kv)
	}
	// chore: close all files
	for _, f := range intermediateFiles {
		f.Close()
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
