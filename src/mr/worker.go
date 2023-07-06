package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		resp, err := Heartbeat() // when err occur, master has already quit.
		switch {
		case resp.Type == ResponseTypeMap:
			doMapTask(mapf, resp.Task, resp.NReduce)
		case resp.Type == ResponseTypeReduce:
			doReduceTask(reducef, resp.Task, resp.NMap, resp.NReduce)
		case resp.Type == ResponseTypeWait:
			wait()
		case resp.Type == ResponseTypeComplete || err != nil:
			return
		default:
			log.Printf("invalid type of response from master: type: %v", resp.Type)
			wait()
		}
	}
}

func doMapTask(mapf func(string, string) []KeyValue, task *Task, nReduce int) {
	intermediate := []KeyValue{}
	filename := task.Filename
	file, err := os.Open(filename)
	defer file.Close()
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	kva := mapf(filename, string(content))
	intermediate = append(intermediate, kva...)

	// Each mapper should create nReduce intermediate files for consumption by the reduce tasks.
	// think about the hashSort or bucket sort
	buckets := make([][]KeyValue, nReduce)
	for i := range buckets {
		buckets[i] = []KeyValue{}
	}
	for _, kv := range intermediate {
		bucketIdx := ihash(kv.Key) % nReduce
		buckets[bucketIdx] = append(buckets[bucketIdx], kv)
	}

	for i, b := range buckets {
		oFilename := fmt.Sprintf("mr-%v-%v", task.Id, i)
		// use temp file write, then rename to assure atomic file write
		tempFile, err := os.CreateTemp("", "temp-*")
		if err != nil {
			log.Fatalf("cannot create temp file for %v", oFilename)
		}
		enc := json.NewEncoder(tempFile)
		for _, kv := range b {
			if err = enc.Encode(&kv); err != nil {
				log.Fatalf("cannot encode %v", kv)
			}
		}
		if err = os.Rename(tempFile.Name(), oFilename); err != nil {
			log.Fatalf("cannot rename from %v to %v", tempFile.Name(), oFilename)
		}
		tempFile.Close()
	}
	mapTaskComplete(task.Id)
}

func doReduceTask(reducef func(string, []string) string, task *Task, nMap, nReduce int) {
	// collect all bucket for this reduce task
	intermediate := []KeyValue{}
	for i := 0; i < nMap; i++ {
		inputFile := fmt.Sprintf("mr-%v-%v", i, task.Id)
		file, err := os.Open(inputFile)
		if err != nil {
			log.Fatalf("cannot open reduce task file: %v")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	// sort by key in RAM by Key
	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%v", task.Id)
	tempFile, err := os.CreateTemp("", "mr-out-temp-*")
	if err != nil {
		log.Fatalf("failed to create temp file for reduce id:%v", task.Id)
	}
	// do reduce func
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j = j + 1
		}
		vals := []string{}
		for k := i; k < j; k++ {
			vals = append(vals, intermediate[k].Value)
		}
		res := reducef(intermediate[i].Key, vals)
		tempFile.WriteString(fmt.Sprintf("%v %v\n", intermediate[i].Key, res))
		i = j
	}

	os.Rename(tempFile.Name(), oname)
	tempFile.Close()

	reduceTaskComplete(task.Id)
}

func wait() {
	time.Sleep(time.Second)
}

// worker called this rpc when assigned map task is complete
func mapTaskComplete(taskID int) {
	args := WorkerArgs{
		MapTaskID:    taskID,
		ReduceTaskID: -1,
	}
	resp := WorkerReply{}
	call("Master.HandleTaskComplete", &args, &resp)
}

// worker called this rpc when assigned reduce task is complete
func reduceTaskComplete(taskID int) {
	args := WorkerArgs{
		MapTaskID:    -1,
		ReduceTaskID: taskID,
	}
	resp := WorkerReply{}
	call("Master.HandleTaskComplete", &args, &resp)
}

// worker call this rpc to ask for next task
func Heartbeat() (*WorkerReply, error) {
	args := WorkerArgs{}
	resp := WorkerReply{}
	if res := call("Master.HandleHeartbeat", &args, &resp); !res {
		log.Fatal("something goes wrong when call rpc Master.HandleHeartbeat")
		return nil, errors.New("something is wrong during heartbeat")
	}
	return &resp, nil
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
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
