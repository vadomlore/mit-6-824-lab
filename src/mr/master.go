package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// An IntHeap is a min-heap of ints.
type IntHeap []int

func (h IntHeap) Len() int           { return len(h) }
func (h IntHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h IntHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *IntHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.(int))
}

func (h *IntHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type Master struct {
	//WorkerMonitor WorkerMonitor
	//jobTracker JobTracker
	a AnotherJobTracker
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

type WorkerStatusArgs struct{
	WorkerMeta Workermeta
}

type JoinMasterReply struct{
	b bool
}

type ReportStatusReply struct{
	b bool
}


//RPC call join master
func (m *Master) JoinMaster(args *WorkerStatusArgs, reply *JoinMasterReply) error {
	m.a.workerManager.WorkerMonitor.AddNewWorker(args.WorkerMeta)
	reply.b = true
	return nil
}

//RPC call update to idle if job is reduceDone
func (m *Master) ReportStatus(args *WorkerStatusArgs, reply *ReportStatusReply) error {
	m.a.workerManager.WorkerMonitor.UpdateWorker(args.WorkerMeta)
	reply.b = true
	return nil
}

//func (m *Master) TaskTrackerTrace(args *TaskTrackerArgs, reply *TaskTrackerReply) error {
//	return m.jobTracker.TaskTrackerTrace(args, reply)
//}
//
//// rpc call to assgin worker map task
//func (jt *JobTracker) TaskTrackerTrace(args *TaskTrackerArgs, reply *TaskTrackerReply) error {
//	switch args.ActionType {
//	case ActionWorkerRequestMap:
//		task := jt.createMapTask(args.WorkerId)
//		reply.WorkerId = args.WorkerId
//		reply.ActionType = ActionMasterAllowMap
//		reply.MapReply = MapReply{task.state, task.taskId, task.mapContext}
//		reply.Timestamp = time.Now().Unix()
//	case ActionWorkerRequestReduce:
//		if jt.checkAllMapComplete() {
//			task := jt.createReduceTask(args)
//			reply.WorkerId = args.WorkerId
//			reply.ActionType = ActionMasterAllowReduce
//			reply.ReduceReply = ReduceReply{task.state, task.reduceContext}
//			reply.Timestamp = time.Now().Unix()
//		}
//	case ActionHealthCheck:
//		if args.State == WorkerStateInMap {
//			jt.updateMapTask(args)
//		} else if args.State == WorkerStateInReduce {
//			jt.updateReduceTask(args)
//		} else if args.State == WorkerStateMapCompleted {
//			jt.completeMapTask(args)
//		} else if args.State == WorkerStateMapCompleted {
//			jt.completeReduceTask(args)
//		}
//		if jt.runningTask[args.WorkerId] == TaskTypeMap {
//			reply.WorkerId = args.WorkerId
//			reply.ActionType = ActionHealthCheck
//			reply.MapReply = MapReply{jt.mapTasks[args.WorkerId].state, jt.mapTasks[args.WorkerId].taskId, jt.mapTasks[args.WorkerId].mapContext}
//			reply.Timestamp = time.Now().Unix()
//		} else if jt.runningTask[args.WorkerId] == TaskTypeReduce {
//			reply.WorkerId = args.WorkerId
//			reply.ActionType = ActionHealthCheck
//			reply.ReduceReply = ReduceReply{jt.reduceTasks[args.WorkerId].state, jt.reduceTasks[args.WorkerId].reduceContext}
//			reply.Timestamp = time.Now().Unix()
//		}
//	case ActionReportMapComplete:
//		jt.completeMapTask(args)
//	case ActionReportReduceComplete:
//		jt.completeReduceTask(args)
//	}
//	return nil
//}
//
//func (jt *JobTracker) updateMapTask(args *TaskTrackerArgs) *TaskTracker {
//	jt.lock.Lock()
//	defer jt.lock.Unlock()
//	jt.mapTasks[args.WorkerId].state = args.State
//	jt.mapTasks[args.WorkerId].lastUpdateTime = args.Timestamp
//	return jt.mapTasks[args.WorkerId]
//}
//
//func (jt *JobTracker) completeMapTask(args *TaskTrackerArgs) *TaskTracker {
//	jt.lock.Lock()
//	defer jt.lock.Unlock()
//	jt.mapTasks[args.WorkerId].state = args.State
//	jt.mapTasks[args.WorkerId].lastUpdateTime = args.Timestamp
//	return jt.mapTasks[args.WorkerId]
//}
//
//func (jt *JobTracker) completeReduceTask(args *TaskTrackerArgs) *TaskTracker {
//	jt.lock.Lock()
//	defer jt.lock.Unlock()
//	jt.mapTasks[args.WorkerId].state = args.State
//	jt.mapTasks[args.WorkerId].lastUpdateTime = args.Timestamp
//	return jt.mapTasks[args.WorkerId]
//}
//
//func (jt *JobTracker) updateReduceTask(args *TaskTrackerArgs) *TaskTracker {
//	jt.lock.Lock()
//	defer jt.lock.Unlock()
//	jt.reduceTasks[args.WorkerId].state = args.State
//	jt.reduceTasks[args.WorkerId].lastUpdateTime = args.Timestamp
//	return jt.reduceTasks[args.WorkerId]
//}
//
//func (jt *JobTracker) createReduceTask(args *TaskTrackerArgs) *TaskTracker {
//	jt.lock.Lock()
//	defer jt.lock.Unlock()
//
//	reduceId := jt.popReduceId()
//
//	ids := make([]string, 2)
//	for _, v := range jt.mapTasks {
//		ids = append(ids, v.taskId)
//	}
//	reduceCtx := map[string]string{"reduceId": reduceId, "mapTaskIds": strings.Join(ids, " ")}
//
//	jt.reduceTasks[args.WorkerId] = &TaskTracker{
//		taskType:       TaskTypeReduce,
//		state:          WorkerStateReduceReady,
//		workerId:       args.WorkerId,
//		taskId:         reduceId,
//		lastUpdateTime: time.Now().Unix(),
//		mapContext:     nil,
//		reduceContext:  reduceCtx}
//	jt.runningTask[args.WorkerId] = TaskTypeReduce
//	return jt.reduceTasks[args.WorkerId]
//}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	l, e := net.Listen("tcp", ":1234")
	// sockname := masterSock()
	// os.Remove(sockname)
	// l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}



//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {

	return false
}

type MapContext interface {
	getContext() string
	getnReduce() int
}

type ReduceContext interface {
	getContext() []string
	getnReduce() int
}

type FileUrlContext struct {
	filename string
	nReduce  int
}

type TaskTracker struct {
	taskType       int // map reduce
	state          int
	workerId       string
	taskId         string
	lastUpdateTime int64
	mapContext     map[string]string
	reduceContext  map[string]string
	lock           sync.Mutex
}

type TaskType int

const (
	Map TaskType=iota
	Reduce
)

type JobPhase int

const (
	PhaseMap JobPhase=iota
	PhaseReduce
)

type TaskState int
//when task is in Idle state, the task can be delivered to any workerId specified by master
//when map task crash in InProgress and Completed state (Caused by health check that the woker machine
//doesn't work anymore), reset task to Idle state, and the master task scheduler will find another
//available machine to schedule the task. when reduce task crash in InPogress state reset to idle and
//the secheduler will do the same work as map schedule, if reduce task is in Completed state, this will
//not trigger
const (
	Idle TaskState=iota
	InProgress
	Completed
)

type WorkerState int

const (
	WIdle WorkerState=iota
	WBusy
	WError
)

type Workermeta struct {
	WorkerId string
	State	WorkerState
	lastUpdateTime int64
}

//on worker disabled action
type WorkerErrorHandler func(workerId string, manager *WorkerManager) bool

//a worker can only process one task a time
// WorkerMonitor worker health status and worker state
type WorkerMonitor struct{
	workers            []Workermeta
}

func (w *WorkerMonitor) Exists(workerMeta Workermeta) bool{
	for _, worker := range w.workers {
		if worker.WorkerId == workerMeta.WorkerId {
			return true
		}
	}
	return false
}

func (w *WorkerMonitor) AddNewWorker(workerMeta Workermeta) bool{
	if w.Exists(workerMeta) {
		log.Println("worker %v already join master", workerMeta.WorkerId)
		return false
	}
	if workerMeta.lastUpdateTime == 0 {
		workerMeta.lastUpdateTime = time.Now().Unix()
	}
	w.workers = append(w.workers, workerMeta)
	return true
}

func (w *WorkerMonitor) UpdateWorker(workerMeta Workermeta) bool{
	worker := w.GetWorkerById(workerMeta.WorkerId)
	if worker != nil {
		worker.State = workerMeta.State
		return true
	}
	return false
}

func (w *WorkerMonitor) GetWorkersByState(wstate WorkerState)[]Workermeta {
	c := make([]Workermeta, 3)
	for _, worker := range w.workers {
		if worker.State == wstate {
			c = append(c, worker)
		}
	}
	return c // reference or copy value ?
}

func (w *WorkerMonitor) GetWorkerById(workerId string ) *Workermeta {
	for _, worker := range w.workers {
		if worker.WorkerId == workerId {
			return &worker
		}
	}
	return nil
}

//set to idle
func (w *WorkerMonitor) setIdleWorker(workerId string){
	for _, e := range w.workers {
		if e.WorkerId == workerId {
			e.State = WIdle
		}
	}
}


//set to WError
func (w *WorkerMonitor) setDisableWorker(workerId string){
	for _, e := range w.workers {
		if e.WorkerId == workerId {
			e.State = WError
		}
	}
}

//set to WBusy
func (w *WorkerMonitor) setBusyWorker(workerId string){
	for _, e := range w.workers {
		if e.WorkerId == workerId {
			e.State = WBusy
		}
	}
}

// listen to all worker health and update the worker state
func (w *WorkerMonitor) WorkerHealthListener(){

}

// listen to all worker and state
func (w *WorkerMonitor) OnWorkerDisabled(workerId string){
	// reset the map task to idle and schedule another worker to run
	// reset the reduce In-Progress task to idle and schedule another worker to run
}

func (w *WorkerMonitor) getAnIdleWorker() (string, bool) {
	for _, worker := range w.workers {
		if worker.State == WIdle {
			return worker.WorkerId, true
		}
	}
	return "", false
}

type ReducePair struct {
	reduceId int
	workerId string
}

// manage worker health status and the map reduce jobs on the worker
type WorkerManager struct {
	nReduce	int
	WorkerMonitor      WorkerMonitor
	mapTaskTable	TaskLookupTable
	reduceTaskTable	TaskLookupTable
	workerErrorHandler WorkerErrorHandler
}


type WorkerTask struct {
	workerId string
	taskMeta *TaskInfo
}
// all the task stored in seq and can reference to a workerId, a worker can hold multiple task
type TaskLookupTable struct {
	taskmeta []TaskInfo
	workerTaskLookupTable map[string][]*TaskInfo
}


func(w * WorkerManager) collectAllIntermediateFiles(reduceId int)[]string{
	var filenames []string
	for _, t := range w.mapTaskTable.taskmeta {
		f := t.metadata.(MapMetadata).IntermediateFiles[reduceId]
		filenames = append(filenames, f)
	}
	return filenames
}

//update worker task to Idle and reset workerId to ""
func(t *TaskLookupTable) CreateTask(task TaskInfo)  bool{
	t.taskmeta = append(t.taskmeta, task)
	return true
}

func(t *TaskLookupTable) FindTaskByWorker(workerId string) ([]*TaskInfo, bool) {
	v, err := t.workerTaskLookupTable[workerId]
	return v, err
}

//update worker task to Idle and reset workerId to ""
func(t *TaskLookupTable) ReleaseWorkerTask(workerId string)  {
	if tasks, ok:= t.workerTaskLookupTable[workerId]; ok {
		for _, tsk := range tasks {
			tsk.state = Idle
		}
		//delete the worker task
		delete(t.workerTaskLookupTable, workerId)
	}
}

//pick an idle task and assign to a worker
func(t *TaskLookupTable) AssignNewTask(workerId string) (*TaskInfo, bool) {
	var task *TaskInfo
	for _, tsk := range t.taskmeta {
		if tsk.state == Idle {
			task = &tsk
			break
		}
	}
	if _, ok := t.workerTaskLookupTable[workerId]; !ok {
		t.workerTaskLookupTable = map[string][]*TaskInfo{}
	}
	t.workerTaskLookupTable[workerId] = append(t.workerTaskLookupTable[workerId], task)
	if task == nil {
		return nil, false
	}
	return task, true
}

//update worker task to Idle and reset workerId to ""
func(t *TaskLookupTable) AllComplete(workerId string)  bool{
	for _, tsk := range t.taskmeta {
		if tsk.state != Completed {
			return false
		}
	}
	return true
}

func (t *TaskLookupTable) CompleteTask(id string, taskInfo TaskInfo) {
	for _, tsk := range t.taskmeta {
		if tsk.TaskId == taskInfo.TaskId {
			tsk.metadata = taskInfo.metadata
			tsk.state = Completed
		}
	}
}


type RequestTaskRequest struct{
	Workermeta Workermeta
}

type RequestTaskReply struct {
	workerId string
	metadata TaskInfo
}

type CompleteTaskRequest struct{
	workerId string
	metadata TaskInfo
}

type CompleteTaskReply struct {
	workerId string
	metadata TaskInfo
}


//type DoReduceArgs struct {
//	workerId string
//	metadata TaskInfo
//}
//
//type DoReduceReply struct {
//	metadata TaskInfo
//}



//RPC call for client request task, worker now is available and can receive task
func (m *Master) DoRequestTask(req *RequestTaskRequest, reply *RequestTaskReply) error{
	reqWorkerId := req.Workermeta.WorkerId
	switch m.a.phase {
	case PhaseMap :
		if m.a.workerManager.WorkerMonitor.GetWorkerById(reqWorkerId).State != WIdle  {
			return nil
		}
		t, ok := m.a.CreateMapTask(reqWorkerId)
		if ok {
			m.a.workerManager.WorkerMonitor.GetWorkerById(workerId).State = WBusy
			reply.workerId = workerId
			reply.metadata = t
			return nil
		} else{
			reply = &RequestTaskReply{}
		}
		return nil
	case PhaseReduce :
		if m.a.workerManager.WorkerMonitor.GetWorkerById(reqWorkerId).State != WIdle  {
			return nil
		}
		t, ok := m.a.CreateReduceTask(reqWorkerId)
		if ok {
			m.a.workerManager.WorkerMonitor.GetWorkerById(workerId).State = WBusy
			reply.workerId = workerId
			reply.metadata = t
			return nil
		} else{
			reply = &RequestTaskReply{}
		}
		return nil
	}
}


// dispatch task to idle worker with rpc call until the end reduce task is finished.
func (a *AnotherJobTracker) ScheduleQueuedTask(){
	ticker := time.NewTicker(3000 * time.Millisecond)
	go func() {
		for {
			select {
			case <-ticker.C:
				switch (a.phase){
				case PhaseMap:
					for w, tasks := range a.workerManager.MapTasks {
						for _, v:= range tasks {
							if v.state == Idle && a.workerManager.WorkerMonitor.GetWorkerById(w).State == WIdle{
								v.state = InProgress
								a.workerManager.WorkerMonitor.GetWorkerById(w).State = WBusy
								//args := DoMapArgs{v}
								//reply := DoMapReply {}
								////dispatch map task to worker
								//call("Worker.StartMap", &args, &reply)
							}
						}
					}
				case PhaseReduce:
					for w, tasks := range a.workerManager.ReduceTasks {
						for _, v:= range tasks {
							if v.state == Idle && a.workerManager.WorkerMonitor.GetWorkerById(w).State == WIdle{
								v.state = InProgress
								a.workerManager.WorkerMonitor.GetWorkerById(w).State = WBusy
								args := DoMapArgs{v}
								reply := DoMapReply {}
								//dispatch map task to worker
								call("Worker.StartReduce", &args, &reply)
							}
						}
					}
				}
			}
		}
	}()
	<- a.reduceDone
}
// listent to the worker state and update worker status periodically
func (a *AnotherJobTracker) StartWorkerListener(){
	ticker := time.NewTicker(3000 * time.Millisecond)
	const MAX_THRESHOLD int64 = 10 * 60 * 1000//10 minutes
	now := time.Now().Unix()

	go func() {
		for {
			select {
			case <-ticker.C:
				for _, worker := range a.workerManager.WorkerMonitor.workers {
					if now - worker.lastUpdateTime > MAX_THRESHOLD {
						if worker.State != WError {
							//exceed max value
							a.workerManager.WorkerMonitor.setDisableWorker(worker.WorkerId)
							workerId := worker.WorkerId
							a.workerManager.workerErrorHandler(workerId, &a.workerManager) //handle error
						}
					}
				}
			}
		}
	}()
	<- a.reduceDone //?
}


type TaskInfo struct {
	JobId	string
	TaskId	string
	Type TaskType
	state TaskState
	metadata interface{}
}

// meta data for map & reduce task information transform to master
// the map task need to maintain the map file delivered by master
// in this lab for simplicity only one file
type MapMetadata struct {
	NReduce	int 	// map split intermediate file count
	Filename string // map source filename
	IntermediateFiles []string // map generated intermediate files
}

func (m *MapMetadata) setIntermediateFiles(f []string) {
	m.IntermediateFiles = f
}


type ReduceMetadata struct {
	ReduceId	string // determine aggregate those intermediate files for reduce task
	Filenames	[]string //reduce intermediate file names
	ResultFilename	string //reduce result file
}

func (a *AnotherJobTracker) getReduceFile()[]string{

}


type JobTracker struct {
	lock    sync.Mutex
	files   []string
	nMap    int
	nReduce int
	// key is workerId
	mapTasks       map[string]*TaskTracker
	reduceTasks    map[string]*TaskTracker
	runningTask    map[string]int
	reduceAssigner IntHeap
}

type InputSource []string
//only one job is available a time now
type AnotherJobTracker struct {
	mapDone    chan bool
	reduceDone    chan bool
	inputsource   InputSource
	nReduce       int
	jobId         string
	phase         JobPhase
	workerManager WorkerManager
	ReduceCounter	int32
}


func(a InputSource) Assign()(string, bool){
	if len(a) <= 0 {
		return "", false
	}
	s := a[len(a) - 1]
	a = a[:len(a) - 1]
	return s, true
}

func(a *AnotherJobTracker) Accept(src []string, nReduce int) {
	a.nReduce = nReduce
	a.inputsource = src
	a.jobId = Generate().String()

	a.preGenerateMapTasks(src, nReduce)
}

func (a *AnotherJobTracker) preGenerateMapTasks(src []string, nReduce int) {
	//pre generate  map tasks with idle state, waiting for worker to pick
	for _, s := range src {
		s := s
		t := TaskInfo{
			JobId:  a.jobId,
			TaskId: Generate().String(),
			Type:   Map,
			state:  Idle,
			metadata: MapMetadata{
				NReduce:           nReduce,
				Filename:          s,
				IntermediateFiles: []string{},
			},
		}
		a.workerManager.mapTaskTable.CreateTask(t)
	}
}

func (a *AnotherJobTracker) preGenerateReduceTasks(nReduce int) {
	//pre generate  map tasks with idle state, waiting for worker to pick
	for i:= 0; i < nReduce; i++ {
		t := TaskInfo{
			JobId:  a.jobId,
			TaskId: Generate().String(),
			Type:   Reduce,
			state:  Idle,
			metadata: ReduceMetadata{
				ReduceId:       strconv.Itoa(i),
				Filenames:      a.workerManager.collectAllIntermediateFiles(i),
				ResultFilename: "",
			},
		}
		a.workerManager.reduceTaskTable.CreateTask(t)
	}
}




//create map task for workerId
func(a *AnotherJobTracker) CreateMapTask(workerId string) (TaskInfo, bool) {

	t, ok := a.workerManager.mapTaskTable.AssignNewTask(workerId)

	if !ok {
		fmt.Println("no job to assigned.")
		return TaskInfo{}, false
	}
	a.workerManager.WorkerMonitor.setBusyWorker(workerId)
	v := *t
	return v, true
}

//create reduce task for workerId
func(a *AnotherJobTracker) CreateReduceTask(workerId string) (TaskInfo, bool) {
	if a.phase != PhaseReduce {
		return TaskInfo{}, false
	}
	reduceId := atomic.AddInt32(&a.ReduceCounter, -1)

	// how to get all intermediate specified by reduceId


	//metadata := ReduceMetadata{a.jobId, 	strconv.Itoa((int)(reduceId)), filename, ""}

	taskId := Generate().String()




	t, err := a.workerManager.CreateMapTask(a.jobId, workerId, taskId, metadata)
	a.workerManager.WorkerMonitor.setBusyWorker(workerId)
	return t, err
}


//create a map task on a machine when there are available idle machines and call rpc to run the job
//func(a *AnotherJobTracker) reAssginMapFailJob(workerId string) bool {
//	done := make(chan bool)
//	tasks := a.workerManager.MapTasks[workerId]
//	ticker := time.NewTicker(3000 * time.Millisecond)
//	go func() {
//		for {
//			select {
//			case <-done:
//				return
//			case <-ticker.C:
//				if len(tasks) == 0 {
//					fmt.Println("no map task to reschedule")
//					done <- true
//				}
//				newWorkerId, ok := a.getAnIdleWorker()
//				if ok {
//					task := tasks[len(tasks) - 1]
//					a.workerManager.CreateMapTask(task.JobId, newWorkerId, task.TaskId, task.metadata.(MapMetadata))
//					a.workerManager.WorkerMonitor.setBusyWorker(newWorkerId)
//					fmt.Println("reschedule map task %v from worker %v to worker %v.", task.TaskId, workerId, newWorkerId)
//					tasks = tasks[:len(tasks) - 1]
//					if len(tasks) == 0 {
//						done <- true
//					}
//				}
//			}
//		}
//	}()
//	return true
//}

func(a *AnotherJobTracker) init() bool {
	wm := WorkerManager{}
	wm.WorkerMonitor = WorkerMonitor{workers: []Workermeta{}}
	wm.mapTaskTable = TaskLookupTable{
		taskmeta:              []TaskInfo{},
		workerTaskLookupTable: map[string][]*TaskInfo{},
	}

	wm.reduceTaskTable = TaskLookupTable{
		taskmeta:              []TaskInfo{},
		workerTaskLookupTable: map[string][]*TaskInfo{},
	}
	//set error handler func
	wm.workerErrorHandler = func(workerId string, manager *WorkerManager) bool {
		//when the worker is error, set all the task to idle state
		manager.mapTaskTable.ReleaseWorkerTask(workerId)

		return true
	}
	a.workerManager = wm
	return true
}

//func (jt *JobTracker) createMapTask(workerId string) *TaskTracker {
//	jt.lock.Lock()
//	defer jt.lock.Unlock()
//	nReduce := strconv.Itoa(jt.nReduce)
//
//	a := jt.files
//	last, a := a[len(a)-1], a[:len(a)-1]
//	mapCtx := map[string]string{"filename": last, "nReduce": nReduce}
//	jt.mapTasks[workerId] = &TaskTracker{
//		taskType:       TaskTypeMap,
//		state:          WorkerStateMapReady,
//		workerId:       workerId,
//		taskId:         Generate().String(),
//		lastUpdateTime: time.Now().Unix(),
//		mapContext:     mapCtx,
//		reduceContext:  nil}
//	jt.runningTask[workerId] = TaskTypeMap
//	return jt.mapTasks[workerId]
//}
//
//func (jt *JobTracker) checkAllMapComplete() bool {
//	if len(jt.mapTasks) == jt.nMap {
//		for _, v := range jt.mapTasks {
//			if v.state != WorkerStateMapCompleted {
//				return false
//			}
//		}
//		return true
//	}
//	return false
//}
//
//func (jt *JobTracker) checkAllReduceComplete() bool {
//	if len(jt.reduceTasks) == jt.nReduce {
//		for _, v := range jt.reduceTasks {
//			if v.state != WorkerStateReduceCompleted {
//				return false
//			}
//		}
//		return true
//	}
//	return false
//}
//
//func (jt *JobTracker) initReduce() {
//	for i := 0; i < jt.nReduce; i++ {
//		jt.reduceAssigner.Push(i)
//	}
//}
//
//func (jt *JobTracker) popReduceId() string {
//	jt.lock.Lock()
//	defer jt.lock.Unlock()
//	v := jt.reduceAssigner.Pop()
//	return strconv.Itoa(v.(int))
//}
//
//func (jt *JobTracker) revokeReduceId(id string) {
//	jt.lock.Lock()
//	defer jt.lock.Unlock()
//	v, err := strconv.Atoi(id)
//	if err != nil {
//		fmt.Println("reduceId parse error %v", id)
//	}
//	jt.reduceAssigner.Push(v)
//}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	m.a.init()

	m.a.Accept(files, nReduce)

	// listen to the worker state
	go m.a.StartWorkerListener()

	// dispatch task to idle worker with rpc call until the end reduce task is finished.
	go m.a.ScheduleQueuedTask()

	m.server()
	return &m
}


// RPC
func (m* Master) CompleteMap(args *CompleteTaskRequest, reply *CompleteTaskReply) error {
	m.a.workerManager.mapTaskTable.CompleteTask(args.workerId, args.metadata)
	return nil
}

//RPC
func (m* Master) CompleteReduce(args *CompleteTaskRequest, reply *CompleteTaskReply) error {
	//todo implement complete reduce task
}
//args :=  DoMapArgs{metadata: info}
//reply :=  DoMapReply{}
//call("Master.CompleteMap", &args, &reply)
