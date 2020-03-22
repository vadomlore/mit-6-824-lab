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

var wm = &Workermeta{
	WorkerId:       Generate().String(),
	State:          WIdle,
	lastUpdateTime: time.Now().Unix(),
}

//
// Map functions return JobTracker slice of KeyValue.
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

func (w* Workermeta) Update(state WorkerState) Workermeta {
	w.State = state
	w.lastUpdateTime = time.Now().Unix()
	nw := *w
	return nw
}

func (w* Workermeta) GetNow() Workermeta {
	w.lastUpdateTime = time.Now().Unix()
	nw := *w
	return nw
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	args :=  WorkerStatusArgs { WorkerMeta: wm.Update(WBusy)}

	reply := JoinMasterReply {}

	if !call("Master.JoinMaster", &args, &reply) {
		fmt.Println("join master error")
		return
	}

	// do health check and report to master periodically
	go func() {
		ticker := time.NewTicker(3000 * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				args := WorkerStatusArgs {wm.GetNow()}
				reply := ReportStatusReply {}
				call("Master.ReportStatus", &args, &reply)
				//consider when to exit
			}
		}
	}()

	// do request task (map & reduce) if worker is idle
	go func() {
		ticker := time.NewTicker(3000 * time.Millisecond)
		for {
			select {
			case <-ticker.C:
				if wm.State != WIdle {
					return
				}
				args := RequestTaskRequest {Workermeta: wm.Update(WBusy)}
				reply := RequestTaskReply {}
				call("Master.DoRequestTask", &args, &reply)
				if reply.Metadata.JobId != "" && reply.Metadata.Type == Map{
					DoMap(mapf, reply.Metadata)
				} else if reply.Metadata.JobId != "" && reply.Metadata.Type == Reduce {
					DoReduce(reducef, reply.Metadata)
				}
			}
		}
	}()
}

func DoReduce(reducef func(string, []string) string, info TaskInfo) {
	wm.Update(WBusy)
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
	args :=  CompleteTaskRequest{WorkerMeta: wm.GetNow(), Metadata: info}
	reply :=  CompleteTaskReply{}
	wm.Update(WIdle)
	//report reduce task complete
	call("Master.CompleteReduce", &args, &reply)
}


func DoMap(mapf func(string, string) []KeyValue, info TaskInfo ) {
	wm.Update(WBusy)
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
		partition := ihash(kv.Key) % (int)(metadata.NReduce)
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
	args :=  CompleteTaskRequest{WorkerMeta: wm.GetNow(), Metadata: info}
	reply :=  CompleteTaskReply{}
	wm.Update(WIdle)
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

	// declare JobTracker reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
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
