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

type taskStatus string

const (
	taskIdle     taskStatus = "idle"    // task has not been assigned to any worker
	taskPending  taskStatus = "pending" // task has been assigned to a worker
	taskComplete taskStatus = "complete"
)

type Task struct {
	Id        int
	Filename  string
	Status    taskStatus
	StartTime time.Time // TODO: not used
}

type TaskList struct {
	tasks []*Task
	mu    sync.Mutex
}

// this method will not lock TaskList, requires to be operated on a locked TaskList
func (tl *TaskList) findIdle() int {
	toAssign := -1
	for i := range tl.tasks {
		if tl.tasks[i].Status == taskIdle {
			toAssign = i
			break
		}
	}
	return toAssign
}

func makeTaskList(n int, files []string) TaskList {
	tl := TaskList{}
	tl.tasks = make([]*Task, n)
	if files != nil { // make map task
		for i := range tl.tasks {
			tl.tasks[i] = &Task{
				Status:   taskIdle,
				Filename: files[i],
				Id:       i,
			}
		}
	} else {
		for i := range tl.tasks { // make reduce task
			tl.tasks[i] = &Task{
				Status: taskIdle,
				Id:     i,
			}
		}
	}

	return tl
}

type Complete struct {
	mapCompleted    int
	reduceCompleted int
	mu              sync.Mutex
}

type Master struct {
	nMap           int
	nReduce        int
	complete       Complete
	mapTaskList    TaskList
	reduceTaskList TaskList
}

// Your code here -- RPC handlers for the worker to call.

func (m *Master) HandleHeartbeat(args *WorkerArgs, reply *WorkerReply) error {
	m.complete.mu.Lock()
	defer m.complete.mu.Unlock()
	if m.complete.mapCompleted < m.nMap {
		// find the first task that is idle
		m.mapTaskList.mu.Lock()
		toAssign := m.mapTaskList.findIdle()

		if toAssign == -1 { // no task is idle
			reply.Type = ResponseTypeWait
			m.mapTaskList.mu.Unlock()
			return nil
		}
		// TODO: may need other info in reply
		reply.Task = m.mapTaskList.tasks[toAssign]
		reply.Type = ResponseTypeMap
		reply.NMap = m.nMap
		reply.NReduce = m.nReduce
		m.mapTaskList.tasks[toAssign].Status = taskPending
		m.mapTaskList.mu.Unlock()
		go func() {
			time.Sleep(10 * time.Second)
			m.mapTaskList.mu.Lock()
			if m.mapTaskList.tasks[toAssign].Status == taskPending {
				m.mapTaskList.tasks[toAssign].Status = taskIdle
			}
			// Do we need to update mapComplete here ?
			m.mapTaskList.mu.Unlock()
		}()
	} else if m.complete.mapCompleted == m.nMap && m.complete.reduceCompleted < m.nReduce {
		// find first reduce task that is idle
		m.reduceTaskList.mu.Lock()
		toAssign := m.reduceTaskList.findIdle()
		if toAssign == -1 {
			reply.Type = ResponseTypeWait
			m.reduceTaskList.mu.Unlock()
			return nil
		}
		reply.Task = m.reduceTaskList.tasks[toAssign]
		reply.Type = ResponseTypeReduce
		reply.NMap = m.nMap
		reply.NReduce = m.nReduce
		m.reduceTaskList.tasks[toAssign].Status = taskPending
		m.reduceTaskList.mu.Unlock()
		go func() {
			time.Sleep(10 * time.Second)
			m.reduceTaskList.mu.Lock()
			if m.reduceTaskList.tasks[toAssign].Status == taskPending {
				m.reduceTaskList.tasks[toAssign].Status = taskIdle
			}
			m.reduceTaskList.mu.Unlock()
		}()
	} else {
		reply.Type = ResponseTypeComplete
	}
	return nil
}

func (m *Master) HandleTaskComplete(args *WorkerArgs, reply *WorkerReply) error {
	m.complete.mu.Lock()
	defer m.complete.mu.Unlock()
	if args.ReduceTaskID == -1 {
		// a map task is complete
		m.mapTaskList.mu.Lock()
		m.complete.mapCompleted += 1
		m.mapTaskList.tasks[args.MapTaskID].Status = taskComplete
		m.mapTaskList.mu.Unlock()
	} else if args.MapTaskID == -1 {
		// a reduce task is complete
		m.reduceTaskList.mu.Lock()
		m.complete.reduceCompleted += 1
		m.reduceTaskList.tasks[args.ReduceTaskID].Status = taskComplete
		m.reduceTaskList.mu.Unlock()
	} else {
		log.Fatal("invalid task complete rpc")
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", "127.0.0.1"+":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	m.complete.mu.Lock()
	res := m.complete.reduceCompleted == m.nReduce
	m.complete.mu.Unlock()
	return res
}

// create a Master.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{
		nMap:    len(files),
		nReduce: nReduce,
	}
	m.mapTaskList = makeTaskList(m.nMap, files)
	m.reduceTaskList = makeTaskList(m.nReduce, nil)
	m.server()
	return &m
}
