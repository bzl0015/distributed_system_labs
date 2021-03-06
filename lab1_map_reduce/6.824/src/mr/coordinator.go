package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"
import "math"
import "fmt"

// bh: go's OOD is based on struct and func as a method(method receptor is the pointer of the struct)  

type Coordinator struct {	// bh: coordinator properties, Cap started word means public, otherwise private in package-wise
	// Your definitions here.
	lock		sync.Mutex
	stage		string		// bh: "MAP"/"REDUCE"/""(finished)
	num_map		int
	num_reduce	int
	tasks		map[string] Task
	avai_tasks	chan Task	// bh: defined in rpc.go ::definition: <chan var> chan <chan data type>; instantiation: chan1 := make(chan <chan type>); use channel to communicate between goroutines
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//

func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//				   	// bh: a function: 			   func <func_name>(<var> <var_type>) <return_var> <return_type> {}
func (c *Coordinator) server() {	// bh: a method: a function with receptor: func (<var> <var_type>(receptor)) <func_name>() <return_type>(optional) {}
	rpc.Register(c)    // bh: register c as a service
	rpc.HandleHTTP()    // bh: attach rpc to http
	//l, e := net.Listen("tcp", ":1234")    // bh: listen to tcp port 1234
	sockname := coordinatorSock()    // bh: defined in rpc.go, coming up with socket name
	os.Remove(sockname)    //remove file <sockname>
	l, e := net.Listen("unix", sockname)    // bh: listen to local socket
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)    // bh: go <cmd>: kick off a new goroutine for parallel processing
}

//create coordinator, called in main/mrcoordinator.go
func MakeCoordinator (files []string, num_reduce int) *Coordinator {

	// bh: instantiate coordinator 
	c := Coordinator{
		stage:		"MAP",
		num_map:	len(files),
		num_reduce:	num_reduce,
		tasks:		make(map[string]Task),    // bh: map creation as following 
		avai_tasks:	make(chan Task, int(math.Max(float64(len(files)), float64(num_reduce)))),
	}	// bh: avai_tasks channel has a capacity (buffer size) as max(#files, #reducers); until the channel receives n + 1 send op, it won't block the sending goroutine. On the other hand, the reading goroutine won't be blocked until the channel becomes empty. here the block will error out as deadlock error.

/* bh:
        //??????new????????????map??????
        ma := new(map[string]int)                                                                                                                                          
        //????????????????????????
        *ma = map[string]int{}
        (*ma)["a"] = 44
        fmt.Println(*ma)
           
        //????????????????????????
        *ma = make(map[string]int, 0)
        (*ma)["b"] = 55
        fmt.Println(*ma)
           
        //????????????????????????
        mb := make(map[string]int, 0)
        mb["c"] = 66
        *ma = mb
        (*ma)["d"] = 77
        fmt.Println(*ma)

	1. make(map[string]string)
	
	2. make([]int, 2): make([]int, <instantiation length>)
	
	3. make([]int, 2, 4): make([]int, <instantiation length>, <retained length>)

*/

	// bh: map each file to a task and put them in the task channel
	for i, file := range files {
		task := Task{
			Type:		"MAP",
			Index:		i,
			MapInputFile: file,
		}
		c.tasks[GenTaskID(task.Type, task.Index)] = task
		c.avai_tasks <- task    // bh: task sent to channel
	}				// bh: v := <-ch receive from channel ch and assign to v


	log.Printf("Coordinator starts...")
	c.server()    // bh: a method was created for coordinator instance, fire off coordinator


	// bh: collect timeout task and resend new task
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)	// bh: check every 0.5s
			c.lock.Lock()				// bh: when check, no other check should be assigned
			for _, task := range c.tasks {
				if task.WorkerID != "" && time.Now().After(task.DeadLine) {
					log.Printf("Found timeout %s task %d on worker %s, reassigning...", task.Type, task.Index, task.WorkerID)
					task.WorkerID = ""
					c.avai_tasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()
	// bh: go func(<var>){}(<var>) : define & call anonymous function in parallel, the last parenthesis is function call with given variables.
	return &c	// bh: return address
}

/* bh: ????????????????????????error
func checkAge(age int) error {
	if age < 0 {
		err := errors.New("??????????????????")
		fmt.Println(err)
		err = fmt.Errorf("?????????????????????%d,??????????????????\n", age)
		fmt.Println(err)
		return err
	}
	fmt.Println("?????????????????????:", age)
	return nil
}
*/

/* bh: check if key in map
if _, ok := map[key]; ok {
//??????
}
*/

func (c *Coordinator) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	if args.LastTaskType != "" {	// bh: last task unfinished
		c.lock.Lock()
		lastTaskID := GenTaskID(args.LastTaskType, args.LastTaskIndex)
		if task, exists := c.tasks[lastTaskID]; exists && task.WorkerID == args.WorkerID {	// bh: ????
			log.Printf("Mark %s task %d as finished on worker %s\n", task.Type, task.Index, args.WorkerID)
			if args.LastTaskType == "MAP" {
				for reducer_id := 0; reducer_id < c.num_reduce; reducer_id++ {
					err := os.Rename(tmpMapOutFile(args.WorkerID, args.LastTaskIndex, reducer_id), finalMapOutFile(args.LastTaskIndex, reducer_id))
					if err != nil {
						log.Fatalf("Failed to mark map output file `%s` as final %e", tmpMapOutFile(args.WorkerID, args.LastTaskIndex, reducer_id), err)
					}
				}
			} else if args.LastTaskType == "REDUCE" {
				err := os.Rename(tmpReduceOutFile(args.WorkerID, args.LastTaskIndex), finalReduceOutFile(args.LastTaskIndex))
				if err != nil {
					log.Fatalf("Failed to marp reduce output file `%s` as final: %e", tmpReduceOutFile(args.WorkerID, args.LastTaskIndex), err)
				}
			}
			delete(c.tasks, lastTaskID)	// bh: delete key lastTaskID from map c.tasks

			if len(c.tasks) == 0 {	// bh: all tasks finished, moving to next stage
				c.transit()
			}
		}
		c.lock.Unlock()
	}
	// bh: check if task in channel
	task, ok := <-c.avai_tasks
	if !ok {
		return nil	// bh: no available tasks
	}

	c.lock.Lock()
	defer c.lock.Unlock()
	// bh: assign next task
	log.Printf("Assign %s task %d to worker %s\n", task.Type, task.Index, args.WorkerID)
	task.WorkerID = args.WorkerID
	task.DeadLine = time.Now().Add(10 * time.Second)
	c.tasks[GenTaskID(task.Type, task.Index)] = task
	reply.TaskType = task.Type
	reply.TaskIndex = task.Index
	reply.MapInputFile = task.MapInputFile
	reply.MapNum = c.num_map
	reply.ReduceNum = c.num_reduce
	return nil
}

/*
defer ?????? main ?????? return ????????????????????????????????????MapNum = c.n
???????????????defer ????????????????????? main ????????????????????? fmt.Println ??????????????????
defer ??????????????????????????????????????????defer ???????????????????????????
defer ??????????????? Golang ????????????????????????????????????????????????
Multiple deferred func calls will be excuted in a LIFO order at the end of the function 
defer ??????????????????????????????????????????????????????????????? Golang ???????????? panic-recover ???????????????????????????????????????
defer ??????????????????????????????defer ???????????????????????????????????????
defer ?????????????????????????????????????????????????????????????????????????????????&???????????????&???????????????&??????????????????????????????????????????????????????
*/

func GenTaskID(t string, index int) string {	// bh: if use a func as the input to another func, the return type has to be defined for the input func
	return fmt.Sprintf("%s-%d", t, index)
}

// bh: 
func (c *Coordinator) transit() {
	if c.stage == "MAP" {
		log.Printf("All map tasks finished, transit to reduce stage...\n")
		c.stage = "REDUCE"
		for i := 0; i < c.num_reduce; i++ {
			task := Task{
				Type:	"REDUCE",
				Index: i,
			}
			c.tasks[GenTaskID(task.Type, task.Index)] = task
			c.avai_tasks <- task
		}
	} else if c.stage == "REDUCE" {
		log.Printf("All reduce tasks finished, exiting...\n")
		close(c.avai_tasks)	// bh: close channel
		c.stage = ""
	}
}

func (c *Coordinator) Done() bool {
	c.lock.Lock()
	defer c.lock.Unlock()
	return c.stage == ""
}

