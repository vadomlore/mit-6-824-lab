package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var mchan = make(chan TaskInfo)
var rchan = make(chan TaskInfo)
var workerState = WIdle
var workerId = Generate().String()
//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type MapReduceChan struct {
	MapChan    chan int
	ReduceChan chan int
}

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

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args :=  WorkerStatusArgs { WorkerMeta:Workermeta{ WorkerId: workerId, State: workerState, lastUpdateTime: time.Now().Unix()}}

	reply := JoinMasterReply {}

	if !call("Master.JoinMaster", &args, &reply) {
		fmt.Println("join master error")
		return
	}

	// do report worker health status periodically
	go func() {
		ticker := time.NewTicker(3000 * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				args := WorkerStatusArgs {WorkerMeta:Workermeta{ WorkerId: workerId, State: workerState, lastUpdateTime: time.Now().Unix()}}
				reply := ReportStatusReply {}
				call("Master.ReportStatus", &args, &reply)
				//consider when to exit
			}
		}
	}()

	// do request job
	go func() {
		ticker := time.NewTicker(3000 * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				args := WorkerStatusArgs {WorkerMeta:Workermeta{ WorkerId: workerId, State: workerState, lastUpdateTime: time.Now().Unix()}}
				reply := ReportStatusReply {}
				call("Master.ReportStatus", &args, &reply)
				//consider when to exit
			}
		}
	}()


	go func() {
		taskInfo := <-mchan
		DoMap(mapf, taskInfo)
	}()

	go func() {
		taskInfo := <-rchan
		DoReduce(reducef, taskInfo)
	}()

	fmt.Println("init workerId %v", workerId)
	//finshed := make(chan int)

	//mapFlag := make(chan int)
	//reduceFlag := make(chan int)
	//tracker := &TaskTracker{
	//	state:         WorkerStateIdle,
	//	taskType:      ActionHealthCheck,
	//	workerId:      workerId,
	//	taskId:        "", //taskId not assigned yet, should be assigned by master
	//	mapContext:    map[string]string{},
	//	reduceContext: map[string]string{},
	//}
	//// report health periodically until master is closed
	//go HealthCheck(tracker, mapFlag, reduceFlag)
	//
	//// request to do the map work
	//go RequestMapTask(mapFlag, tracker)
	//
	//// do map job with one thead
	//go func() {
	//	<-mapFlag
	//	DoMap(mapf, tracker)
	//}()

	// // do reduce job with another thead

	// // request to do the map work
	// go RequestReduceTask(tracker)

	// go func() {
	// 	<-reduceFlag
	// 	DoReduce(reducef, tracker)
	// }()

	// CallExample()
	//<-finshed
	// request do reduce task with this thread when
	//report health status in a period
}

func DoReduce(reducef func(string, []string) string, info TaskInfo) {

	metadata := info.metadata.(ReduceMetadata)
	intermediate := make([]KeyValue, 100)
	for _, f := range metadata.Filenames {
		kv := load(f)
		intermediate = append(intermediate, kv...)
	}
	sort.Sort(ByKey(intermediate))
	oname := "mr-out-" + metadata.ReduceId
	ofile, _ := os.Create(oname)

	// call Reduce on each distinct key in intermediate[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
	info.state = Completed
	metadata.ResultFilename = oname

	args :=  CompleteTaskRequest{workerId: workerId, metadata: info}
	reply :=  CompleteTaskReply{}

	//report reduce task complete
	call("Master.CompleteReduce", &args, &reply)
}


func DoMap(mapf func(string, string) []KeyValue, info TaskInfo ) {

	metadata := info.metadata.(MapMetadata)
	file, err := os.Open(metadata.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", metadata.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", metadata.Filename)
	}
	file.Close()
	kva := mapf(metadata.Filename, string(content))

	m1 := make(map[int][]KeyValue)

	for _, kv := range kva {
		partition := ihash(kv.Key) % metadata.NReduce
		value, ok := m1[partition]
		if !ok {
			value = make([]KeyValue, 10)
		}
		value = append(value, kv)
	}
	intermediates := make([]string, 0)

	for reduceId, kv := range m1 {
		im := store(kv, info.TaskId, strconv.Itoa(reduceId))
		intermediates = append(intermediates, im)
	}
	//report Map task complete
	info.state = Completed
	metadata.IntermediateFiles = intermediates
	args := CompleteTaskRequest {metadata: info}
	reply :=  CompleteTaskReply{}
	call("Master.CompleteMap", &args, &reply)
}


// example function to show how to make an RPC call to the master.
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
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// request map task periodically
func RequestMapTask(ch chan int, taskTracker *TaskTracker) {
	if taskTracker.state == WorkerStateIdle {
		args := TaskTrackerArgs{ActionWorkerRequestMap, WorkerStateIdle, taskTracker.workerId, MapArgs{}, ReduceArgs{}, time.Now().Unix()}
		reply := TaskTrackerReply{}
		call("Master.TaskTrackerTrace", &args, &reply)
		if reply.ActionType == ActionMasterAllowMap {
			if taskTracker.state == WorkerStateIdle {
				taskTracker.taskId = reply.MapReply.MapTaskId
				taskTracker.mapContext = reply.MapReply.MapContext
			}
			ch <- 1 //start do map
		}
	}
}

// request reduce task periodically
func RequestReduceTask(taskTracker *TaskTracker) {
	if taskTracker.state == WorkerStateIdle || taskTracker.state == WorkerStateMapCompleted || taskTracker.state == WorkerStateReduceCompleted {
		args := TaskTrackerArgs{ActionWorkerRequestReduce, WorkerStateIdle, taskTracker.workerId, MapArgs{}, ReduceArgs{}, time.Now().Unix()}
		reply := TaskTrackerReply{}
		call("Master.TaskTrackerTrace", &args, &reply)
	}
}

// timer
func HealthCheck(taskTracker *TaskTracker, mapFlag chan int, reduceFlag chan int) {

	ticker := time.NewTicker(5000 * time.Millisecond)

	done := make(chan bool)

	go func() {
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				fmt.Println("do health check for worker %v", taskTracker.workerId)
				// do health check and report current task status
				args := TaskTrackerArgs{ActionHealthCheck, taskTracker.state, taskTracker.workerId, MapArgs{}, ReduceArgs{}, time.Now().Unix()}
				reply := TaskTrackerReply{}
				call("Master.TaskTrackerTrace", &args, &reply)
				// report health status
				if reply.ActionType == ActionMasterAllowMap {
					if taskTracker.state == WorkerStateIdle {
						taskTracker.taskId = reply.MapReply.MapTaskId
						taskTracker.mapContext = reply.MapReply.MapContext
					}
					mapFlag <- 1 //start do map

				} else if reply.ActionType == ActionMasterAllowReduce {
					reduceFlag <- 1 // start do reduce
				} else if reply.ActionType == ActionHealthCheck {
					fmt.Printf("health check for workerId %v", reply.WorkerId)
					switch taskTracker.state {
					case WorkerStateInMap, WorkerStateMapCompleted:
						fmt.Println("map job %v health check ...", reply.MapReply.MapTaskId)
					case WorkerStateInReduce, WorkerStateReduceCompleted:
						fmt.Println("reduce job %v health check ...", reply.ReduceReply.ReduceContext)
					}
				}
			}
		}
	}()
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	// sockname := masterSock()
	// c, err := rpc.DialHTTP("unix", sockname)
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

// store intermediate data to disk with format mr-X-Y
// X is map task id and y is reduce task id
func store(kva []KeyValue, mapTaskId string, reduceTaskId string) string{
	fileInfos, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatal("error read file")
	}
	for _, v := range fileInfos {
		if !v.IsDir() && strings.HasPrefix(v.Name(), "mr-tmp-") {
			err := os.Remove(v.Name())
			if err != nil {
				log.Fatal("remove temporary file error %v", v.Name)
			}
		}
	}
	f, err := ioutil.TempFile("", "mr-tmp-")
	enc := json.NewEncoder(f)
	for _, kv := range kva {
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatal("store data to  file %v error.", f.Name())
		}
	}
	permnateIntermediateFile := ".\\" + "mr-" + mapTaskId + "-" + reduceTaskId
	os.Rename(f.Name(), permnateIntermediateFile)
	return permnateIntermediateFile
}

// load intermediate data from disk
func load(filename string) []KeyValue {

	kva := make([]KeyValue, 100)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatal("load data to  memory failed, file %v not exists.", filename)
	}

	dec := json.NewDecoder(file)
	for {
		var kv KeyValue
		if err := dec.Decode(&kv); err != nil {
			break
		}
		kva = append(kva, kv)
	}
	return kva
}
