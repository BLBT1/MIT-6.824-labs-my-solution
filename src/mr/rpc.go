package mr

//
// RPC definitions.
//

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ResponseType int32

const (
	// worker heartbeat response type
	ResponseTypeComplete = 0
	ResponseTypeMap      = 1
	ResponseTypeReduce   = 2
	ResponseTypeWait     = 3
)

type WorkerArgs struct {
	MapTaskID    int
	ReduceTaskID int
}

type WorkerReply struct {
	Type ResponseType

	// metadata
	NMap    int
	NReduce int

	// task
	Task *Task
}
