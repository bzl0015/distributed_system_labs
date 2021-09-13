package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"
import "time"
import "fmt"

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

// Add your RPC definitions here.

type Task struct {
	Type			string
	Index			int
	MapInputFile	string
	WorkerID		string
	DeadLine		time.Time
}

type ApplyForTaskArgs struct {
	WorkerID		string
	LastTaskType	string
	LastTaskIndex	int
}

type ApplyForTaskReply struct {
	TaskType		string
	TaskIndex		int
	MapInputFile	string
	MapNum			int
	ReduceNum		int
}

func tmpMapOutFile(worker string, mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("tmp-Worker-%s-%d-%d", worker, mapIndex, reduceIndex)
}

func finalMapOutFile(mapIndex int, reduceIndex int) string {
	return fmt.Sprintf("mr-%d-%d", mapIndex, reduceIndex)
}

func tmpReduceOutFile(worker string, reduceIndex int) string {
	return fmt.Sprint("tmp-worker-%s-out-%d", worker, reduceIndex)
}

func finalReduceOutFile(reduceIndex int) string {
	return fmt.Sprintf("mr-out-%d", reduceIndex)
}

/* bh:

Println：

1. 用默认的类型格式显示方式将传入的参数写入到标准输出里面(即在终端中有显示)，
2. 多个传入参数之间使用空格分隔，
3. 在显示的最后追加换行符，
4. 返回值为 写入标准输出的字节数和写入过程中遇到的问题。
Printf：

1. 用传入的格式化规则符将传入的变量写入到标准输出里面(即在终端中有显示)，
2. 返回值为 写入标准输出的字节数和写入过程中遇到的问题。
Sprintf:

1. 用传入的格式化规则符将传入的变量格式化，(终端中不会有显示)
2. 返回为 格式化后的字符串。

*/


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
