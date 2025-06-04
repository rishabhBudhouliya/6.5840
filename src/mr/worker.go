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
	"sort"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	// reply := go CallForTask()
	mapReply := CallForMapTask()
	file, err := os.Open(mapReply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", mapReply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", mapReply.Filename)
	}
	file.Close()
	kv := mapf(mapReply.Filename, string(content))
	files, err := createIntermediateFiles(mapReply.WorkerId, mapReply.NReduce)
	intermediateFileNames := partition(files, kv, mapReply.NReduce)
	if err != nil {
		panic(err)
	}
	CallToFinishMapTask(wid.id, intermediateFileNames)
	fmt.Print("are we reaching here?")
	// free for a reduce task then?
	reply := CallForReduceTask()
	filenames := reply.IntermediateFilenames
	intermediate := readIntermediateFiles(filenames)
	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%s", reply.WorkerId)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	ofile.Close()
	CallToMarkDone(wid.id)
}

func CallForMapTask() CoordinatorReply {
	wid = &WorkerId{}
	args := WorkerArgs{WorkerId: wid.id}
	reply := CoordinatorReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	wid.setWorkerId(reply.WorkerId)
	if !ok {
		fmt.Print("Coordinator get task failed!")
		// do something
	}
	return reply
}

func CallForReduceTask() WorkerReduceTaskReply {
	wid = &WorkerId{}
	args := WorkerReduceTaskArgs{WorkerId: wid.id}
	reply := WorkerReduceTaskReply{}
	fmt.Print("are we calling reduce task?")
	ok := call("Coordinator.GetReduceTask", &args, &reply)
	wid.setWorkerId(reply.WorkerId)
	if !ok {
		fmt.Println("Could not get a reduce task from coordinator!")
	}
	return reply
}

func CallToFinishMapTask(workerId string, intermediateFiles []string) {
	args := WorkerFinishTaskArgs{WorkerId: workerId, IntermediateFilenames: intermediateFiles}
	reply := WorkerFinishTaskReply{}
	ok := call("Coordinator.FinishTask", &args, &reply)
	if !ok {
		fmt.Print("Unable to tell Coordinator I'm done!")
	}
}

func CallToMarkDone(workerId string) {
	args := WorkerTaskDoneArgs{WorkerId: workerId}
	reply := WorkerTaskDoneReply{}
	ok := call("Coordinator.MarkTaskDone", &args, &reply)
	if !ok {
		fmt.Print("Unable to tell Coordinator I'm done!")
	}
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

func readIntermediateFiles(filenames []string) []KeyValue {
	kva := []KeyValue{}
	for i := 0; i < len(filenames); i++ {
		f, err := os.Open(filenames[i])
		if err != nil {
			fmt.Printf("could not open %v: %w", f, err)
			return nil
		}
		defer f.Close()
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}
	return kva
}

// return partitioned filenames
func partition(intermediateFiles []*os.File, intermediate []KeyValue, nReduce int) []string {
	for _, kv := range intermediate {
		bucket := ihash(kv.Key) % nReduce
		enc := json.NewEncoder(intermediateFiles[bucket])
		enc.Encode(kv)
	}
	// chore: close all files and return filenames
	var output []string
	for _, f := range intermediateFiles {
		output = append(output, f.Name())
		f.Close()
	}
	return output
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
