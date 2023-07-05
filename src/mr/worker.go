package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
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

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		resp := Heartbeat()
		switch resp.Type {
		case ResponseTypeMap:
			doMapTask(mapf, resp.Task, resp.NReduce)
		case ResponseTypeReduce:
			doReduceTask(reducef, resp.Task, resp.NMap, resp.NReduce)
		case ResponseTypeWait:
			wait()
		case ResponseTypeComplete:
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
		tempFile, err := ioutil.TempFile("", "temp-*")
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

}

func wait() {
	time.Sleep(time.Second)
}

// worker called this rpc when assigned task is completed
func mapTaskComplete(taskID int) {
	args := WorkerArgs{
		MapTaskID:    taskID,
		ReduceTaskID: -1,
	}
	resp := WorkerReply{}
	call("Master.HandleTaskComplete", &args, &resp)
}

// worker call this rpc to ask for next task
func Heartbeat() *WorkerReply {
	args := WorkerArgs{}
	resp := WorkerReply{}
	if res := call("Master.HandleHeartbeat", &args, &resp); !res {
		log.Fatal("something goes wrong when call rpc Master.HandleHeartbeat")
	}
	return &resp
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	//c, err := rpc.DialHTTP("unix", "mr-socket")
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
