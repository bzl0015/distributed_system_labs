package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "strconv"
import "os"
import "log"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type Bykey []KeyValue

func (a Bykey) Len() int           {return len(a)}
func (a Bykey) Swap(i, j int)	   {a[i], a[j] = a[j], a[i]}
func (a Bykey) Less(i, j int) bool {return a[i].key < a[j].key}


//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	id := strconv.Itoa(os.Getpid())
	log.Printf("Worker %s started...\n", id)

	var lastTaskType string
	var lastTaskIndex int
	for {
		args := ApplyForTaskArgs{
			WorkerID:	id,
			LastTaskType:	lastTaskType,
			LastTaskIndex:	lastTaskIndex,
		}
		reply := ApplyForTaskReply{}
		call("Coordinator.ApplyForTask", &args, &reply)
	}

}

// bh: https://stackoverflow.com/questions/44805984/golang-interface-to-struct for struct used as interface
func call(rpcname string, args interface{}, reply interface{}) bool {

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
