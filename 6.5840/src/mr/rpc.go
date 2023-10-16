package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

// 注册的请求参数与返回参数
type RegisterArgs struct {
}

type RegisterReply struct {
	WorkerID int
}

// TaskArgs 是用来向master寻求task的参数
type TaskArgs struct {
	WorkerID int
}
type TaskReply struct {
	Task *Task
}

// 描述任务执行状态的结构体
type ReportTaskArgs struct {
	Done     bool
	Seq      int
	Phase    TaskPhase
	WorkerID int
}
type ReportTaskReply struct{}

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
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
