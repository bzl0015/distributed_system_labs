package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "strconv"
import "os"
import "io/ioutil"
import "sort"
import "strings"



//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

func (a ByKey) Len() int           {return len(a)}
func (a ByKey) Swap(i, j int)	   {a[i], a[j] = a[j], a[i]}
func (a ByKey) Less(i, j int) bool {return a[i].Key < a[j].Key}


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
func MakeWorker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

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
		reply := ApplyForTaskReply{}	// bh: initialize reply interface
		call("Coordinator.ApplyForTask", &args, &reply)		// bh: encapsule client call in procedure

		if reply.TaskType == "" {
			// all tasks done
			log.Printf("Received job finish signal from coordinator")
			break
		}

		log.Printf("Receive %s task %d from coordinator", reply.TaskType, reply.TaskIndex)
		if reply.TaskType == "MAP" {
			// load read data, store into map file indentified by pid, tastid, reduceid
			file, err := os.Open(reply.MapInputFile)
			if err != nil {
				log.Fatal("Failed to open map input file %s: %e", reply.MapInputFile, err)
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("Failed to read map input %s: %e", reply.MapInputFile, err)
			}
			kva := mapf(reply.MapInputFile, string(content))	// bh: filename unused; kva = [KeyValue(w1, 1), KeyValue(w2,1), ...]
			hashedKva := make(map[int][]KeyValue)	// bh: hashedKva[<reducer_id>] = [KeyValue1, KeyValue2, ...]
			for _, kv := range kva {
				hashed := ihash(kv.Key) % reply.ReduceNum
				hashedKva[hashed] = append(hashedKva[hashed], kv)
			}
			for i := 0; i < reply.ReduceNum; i ++ {	// bh: write to map file
				ofile, _ := os.Create(tmpMapOutFile(id, reply.TaskIndex, i))
				for _, kv := range hashedKva[i] {
					fmt.Fprintf(ofile, "%v\t%v\n", kv.Key, kv.Value)
				}
				ofile.Close()
			}
		} else if reply.TaskType == "REDUCE" {
			var lines []string
			for mi := 0; mi < reply.MapNum; mi++ {
				inputFile := finalMapOutFile(mi, reply.TaskIndex)
				file, err := os.Open(inputFile)
				if err != nil {
					log.Fatalf("Failed to open map file %s: %e", inputFile, err)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("Failed to read content from map file %s: %e", inputFile, err)
				}
				lines = append(lines, strings.Split(string(content), "\n")...)	// bh: slice to slice append
			}
			var kva []KeyValue
			for _, line := range lines {
				if strings.TrimSpace(line) == "" {	// bh: == chomp, skipping empty lines
					continue
				}
				parts := strings.Split(line, "\t")
				kva = append(kva, KeyValue{
					Key: parts[0],
					Value: parts[1],
				})
			}
			sort.Sort(ByKey(kva))

			ofile, _ := os.Create(tmpReduceOutFile(id, reply.TaskIndex))
			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j ++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}

				output := reducef(kva[i].Key, values)
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			ofile.Close()
		}
		lastTaskType = reply.TaskType
		lastTaskIndex = reply.TaskIndex
		log.Printf("Finished %s task %d", reply.TaskType, reply.TaskIndex)
	}

	log.Printf("Worker %s exit\n", id)
}

// bh: https://stackoverflow.com/questions/44805984/golang-interface-to-struct for struct used as interface
func call(rpcname string, args interface{}, reply interface{}) bool {
	//c, err = rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err != nil {
		return true
	}

	fmt.Println(err)
	return false
}

/* bh: rpc usage:
At server:
rpc.Register(<data or struct>)	//registered object must have at least one method matching: func (<data/struct> *<data/struct>) <method>(argType *<T1>, replyType *<T2>) error {} e.g.: func (c *Coordinator) ApplyForTask(args *ApplyForTasksArgs, reply *ApplyForTaskReply) error {}
rpc.HandleHTTP()	//attach http protocal
l, e := net.Listen(<protocal>, <port>)
http.Serve(l, nil)

At client:
client, err := rpc.DialHTTP(<protocal>, <port>)
err = client.Call(<registered rpc object>.<rpc method>, args, reply)

*/

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
/*
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
*/
