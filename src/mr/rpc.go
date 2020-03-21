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

//
//const (
//	ActionHealthCheck          = 0
//	ActionWorkerRequestMap     = 1
//	ActionMasterAllowMap       = 2
//	ActionWorkerRequestReduce  = 3
//	ActionMasterAllowReduce    = 4
//	ActionReportMapComplete    = 5
//	ActionReportReduceComplete = 6
//)
//
//const (
//	WorkerStateIdle            = 0
//	WorkerStateMapReady        = 1
//	WorkerStateInMap           = 2
//	WorkerStateMapCompleted    = 3
//	WorkerStateReduceReady     = 4
//	WorkerStateInReduce        = 5
//	WorkerStateReduceCompleted = 6
//	WorkerStateError           = 7
//)
//
//const (
//	TaskTypeMap    = 1
//	TaskTypeReduce = 2
//)

//type MapArgs struct {
//	TaskId string
//}
//
//type ReduceArgs struct {
//	TaskId string
//}

//type ReduceReply struct {
//	State         int // will report the reduce ready state to worker
//	ReduceContext map[string]string
//}
//
//type MapReply struct {
//	State      int // will report the ready state to worker
//	MapTaskId  string
//	MapContext map[string]string
//}
//
//type TaskTrackerArgs struct {
//	ActionType int
//	State      int //slave State
//	WorkerId   string
//	MapArgs    MapArgs
//	ReduceArgs ReduceArgs
//	Timestamp  int64
//}
//
//type TaskTrackerReply struct {
//	WorkerId    string
//	ActionType  int
//	MapReply    MapReply
//	ReduceReply ReduceReply
//	Timestamp   int64
//}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

// type PongReply struct {
// 	pongTimestamp int64
// 	piggy         interface{}
// }
