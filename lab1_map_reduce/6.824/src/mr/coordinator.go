package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"


// bh: go's OOD is based on struct and func as a method(method receptor is the pointer of the struct)  

type Coordinator struct {    // bh: coordinator properties, Cap started word means public, otherwise private in package-wise
	// Your definitions here.
	lock		sync.Mutex,
	stage		string,    // bh: "MAP"/"REDUCE"/""(finished)
	num_map		int,
	num_reduce	int,
	tasks		map[string] Task,
	avai_tasks	chan Task,    // bh: defined in rpc.go ::definition: <chan var> chan <chan data type>; instantiation: chan1 := make(chan <chan type>)
				     // use channel to communicate between goroutines
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
		avai_tasks	make(chan Task, int(math.Max(float64(len(files)), float64(num_reduce))))
	}	// bh: avai_tasks channel has a capacity (buffer size) as max(#files, #reducers); until the channel receives n + 1 send op, it won't block the sending goroutine. On the other hand, the reading goroutine won't be blocked until the channel becomes empty. here the block will error out as deadlock error.

/* bh:
        //使用new创建一个map指针
        ma := new(map[string]int)                                                                                                                                          
        //第一种初始化方法
        *ma = map[string]int{}
        (*ma)["a"] = 44
        fmt.Println(*ma)
           
        //第二种初始化方法
        *ma = make(map[string]int, 0)
        (*ma)["b"] = 55
        fmt.Println(*ma)
           
        //第三种初始化方法
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
			MapInputFile: file
		}
		c.tasks[GemTaskID(task.Type, task.Index)] = task
		c.avai_tasks <- task    // bh: task sent to channel
	}				// bh: v := <-ch receive from channel ch and assign to v


	log.Printf("Coordinator starts...")
	c.server()    // bh: a method was created for coordinator instance, fire off coordinator


	// bh: collect timeout task and resend new task
	go func() {
		for {
			time.Sleep(500 * time.Millisecond)	// bh: check every 0.5s
			c.lock.lock()				// bh: when check, no other check should be assigned
			for _, task := range c.tasks {
				if task.WorkerID != "" && time.Now().After(task.Deadline) {
					log.Printf("Found timeout %s task %d on worker %s, reassigning...", task.Type, task.Index, task.WorkerID)
					tast.WorkerID = ""
					c.avai_tasks <- task
				}
			}
			c.lock.Unlock()
		}
	}()
	// bh: go func(<var>){}(<var>) : define & call anonymous function in parallel, the last parenthesis is function call with given variables.
	return &c	// bh: return address
}

/* bh: 创建函数并自定义error
func checkAge(age int) error {
	if age < 0 {
		err := errors.New("年龄输入错误")
		fmt.Println(err)
		err = fmt.Errorf("您输入的年龄是%d,年龄输入错误\n", age)
		fmt.Println(err)
		return err
	}
	fmt.Println("您输入的年龄是:", age)
	return nil
}
*/

/* bh: check if key in map
if _, ok := map[key]; ok {
//存在
}
*/

func (c *Coordinator) ApplyForTask(args *ApplyForTasksArgs, reply *ApplyForTaskReply) error {
	if args.LastTaskType != "" {	// bh: last task unfinished
		c.lock.Lock()
		lastTaskID := GenTaskID(args.LastTaskType, args.LastTaskIndex)
		if task, exists := c.tasks[lastTaskID]; exists && tastk.WorkerID == args.WorkerID {
			log.Printf("Mark %s task %d as finished on worker %s\n", task.Type, task.Index, args.WorkerID)
			if args.LastTaskType == "MAP" {
				for reducer_id := 0; reducer_id < c.num_reduce; reducer_id++ {
					err := os.Rename(tmpMapOutFile(args.WorkerID, args.LastTaskIndex, reducer_id), finalMapOutFile(args.LastTaskIndex, reducer_id))
					if err != nil {
						log.Fatalf("Failed to mark map output file `%s` as final %e", tmpMapOutFile(args.WorkerID, args.LastTaskIndex, reducer_id), err)
					}
				}
			} args.LastTaskType == "REDUCE" {
				err := os.Rename(tmpReduceOutFile(args.WorkerID, args.LastTaskIndex), finalRuduceOutFile(args.LastTaskIndex))
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
	task.DeadLine = time.Now().add(10 * time.Second())
	c.tasks[GenTaskID(task.Type, task.Index)] = task
	reply.TaskType = task.Type
	reply.TaskIndex = task.Index
	reply.MapInputFile = task.MapInputFile
	reply.MapNum = c.num_map
	reply.ReduceNum = c.num_reduce
	return nil
}

/*
defer 会在 main 函数 return 之前时候调用。核心要点：
延迟调用：defer 语句本身虽然是 main 的第一行，但是 fmt.Println 是先打印的；
defer 关键字一定是处于函数上下文：defer 必须放在函数内部；
defer 其实并不是 Golang 独创，是多种高级语言的共同选择；
Multiple deferred func calls will be excuted in a LIFO order at the end of the function 
defer 最重要的一个特点就是无视异常可执行，这个是 Golang 在提供了 panic-recover 机制之后必须做的补偿机制；
defer 的作用域存在于函数，defer 也只有和函数结合才有意义；
defer 允许你把配套的两个行为代码放在最近相邻的两行，比如创建&释放、加锁&放锁、前置&后置，使得代码更易读，编程体验优秀；
*/

func GenTaskID(t string, index int) {
	return fmt.Sprintf("%s-%d", t, index)
}

// bh: 
func (c *Coordinator) transit() {
	if c.stage == "MAP" {
		log.Printf("All map tasks finished, transit to reduce stage...\n")
		c.stage = "REDUCE"
		for i := 0; i < c.num_reduce; i++ {
			task := Task{
				Type:	"REDUCE";
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
	return c.stage = ""
}
/*
//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.


	c.server()
	return &c
}
*/
