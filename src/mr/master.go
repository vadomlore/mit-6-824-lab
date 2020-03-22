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

type Master struct {
	JobTracker AnotherJobTracker
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
	m.JobTracker.taskManager.WorkerMonitor.AddNewWorker(args.WorkerMeta)
	reply.b = true
	return nil
}

//RPC call update to idle if job is reduceDone
func (m *Master) ReportStatus(args *WorkerStatusArgs, reply *ReportStatusReply) error {
	m.JobTracker.taskManager.WorkerMonitor.UpdateWorker(args.WorkerMeta)
	reply.b = true
	return nil
}

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
	return m.JobTracker.phase == Result
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
	Result
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
type WorkerErrorHandler func(workerId string, manager *TaskManager) bool

//JobTracker worker can only process one task JobTracker time
// WorkerMonitor worker health status and worker state
type WorkerMonitor struct{
	lock	sync.Mutex
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
	w.lock.Lock()
	defer w.lock.Unlock()
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
	w.lock.Lock()
	defer w.lock.Unlock()
	worker := w.GetWorkerByIdForUpdate(workerMeta.WorkerId)
	if worker != nil {
		if workerMeta.lastUpdateTime < worker.lastUpdateTime { // the received update information is expired (old information may be due to network lag)
			return false
		}
		worker.State = workerMeta.State
		worker.lastUpdateTime = workerMeta.lastUpdateTime
		return true
	}
	return false
}

func (w *WorkerMonitor) GetWorkersByState(wstate WorkerState)[]Workermeta {
	w.lock.Lock()
	defer 	w.lock.Unlock()
	c := make([]Workermeta, 3)
	for _, worker := range w.workers {
		if worker.State == wstate {
			c = append(c, worker)
		}
	}
	return c // reference or copy value ?
}

func (w *WorkerMonitor) GetWorkerByIdForUpdate(workerId string ) *Workermeta {
	w.lock.Lock()
	defer w.lock.Unlock()
	for _, worker := range w.workers {
		if worker.WorkerId == workerId {
			return &worker
		}
	}
	return nil
}

//used by rpc-connection
func (w *WorkerMonitor) SetWorkerIdle(worker Workermeta){
	w.lock.Lock()
	defer w.lock.Unlock()
	for i, e := range w.workers {
		if e.WorkerId == worker.WorkerId {
			e.State = WIdle
			e.lastUpdateTime = time.Now().Unix()
			w.workers[i] = e
		}
	}
}


//used by rpc-connection
func (w *WorkerMonitor) SetWorkerError(worker Workermeta){
	w.lock.Lock()
	defer w.lock.Unlock()
	for i, e := range w.workers {
		if e.WorkerId == worker.WorkerId {
			e.State = WError
			e.lastUpdateTime = time.Now().Unix()
			w.workers[i] = e
		}
	}
}

//used by rpc-connection
func (w *WorkerMonitor) SetWorkerBusy(worker Workermeta){
	w.lock.Lock()
	defer w.lock.Unlock()
	for i, e := range w.workers {
		if e.WorkerId == worker.WorkerId {
			e.State = WBusy
			e.lastUpdateTime = time.Now().Unix()
			w.workers[i] = e
		}
	}
}

// manage worker health status and the map reduce jobs on the worker
type TaskManager struct {
	MapLock				sync.Mutex
	ReduceLock			sync.Mutex
	WorkerMonitor          WorkerMonitor
	MapTaskTable           TaskLookupTable
	ReduceTaskTable        TaskLookupTable
	WorkerErrorHandlerFunc WorkerErrorHandler
}

// all the task stored in seq and can reference to JobTracker workerId, JobTracker worker can hold multiple task
type TaskLookupTable struct {
	taskmeta []TaskInfo
	workerTaskLookupTable map[string][]*TaskInfo
}

func(w *TaskManager) getAllIntermediateFiles(reduceId int)[]string{
	w.MapLock.Lock()
	defer w.MapLock.Unlock()
	var filenames []string
	for _, t := range w.MapTaskTable.taskmeta {
		f := t.metadata.(MapMetadata).IntermediateFiles[reduceId]
		filenames = append(filenames, f)
	}
	return filenames
}

func (w *TaskManager) CompleteMapTask(metadata TaskInfo) bool {
	w.MapLock.Lock()
	defer w.MapLock.Unlock()
	return w.MapTaskTable.CompleteTask(metadata)
}

func (w *TaskManager) CompleteReduceTask(metadata TaskInfo) bool{
	w.ReduceLock.Lock()
	defer w.ReduceLock.Unlock()
	return w.ReduceTaskTable.CompleteTask(metadata)
}

//update worker task to Idle and reset workerId to ""
func(t *TaskLookupTable) CreateTask(task TaskInfo)  bool{
	t.taskmeta = append(t.taskmeta, task)
	return true
}

//update worker task to Idle and reset workerId to "" for map
func(t *TaskLookupTable) ReleaseAllTask(workerId string)  {
	if tasks, ok:= t.workerTaskLookupTable[workerId]; ok {
		for _, tsk := range tasks {
			tsk.state = Idle
		}
		//delete the worker task
		delete(t.workerTaskLookupTable, workerId)
	}
}

//update worker task to Idle if task is UnComplete and reset workerId to "" for reduce
func(t *TaskLookupTable) ReleaseUnCompleteTask(workerId string)  {
	if tasks, ok:= t.workerTaskLookupTable[workerId]; ok {
		for _, tsk := range tasks {
			if tsk.state != Completed {
				tsk.state = Idle
			}
		}
		//delete the worker task
		delete(t.workerTaskLookupTable, workerId)
	}
}

//pick an idle task and assign to JobTracker worker
func(t *TaskLookupTable) AssignNewTask(workerId string) (*TaskInfo, bool) {
	var task TaskInfo
	index := -1
	for i, tsk := range t.taskmeta {
		if tsk.state == Idle {
			tsk.state = InProgress
			task = tsk
			index = i
			break
		}
	}
	if index != -1 {
		t.taskmeta[index] = task
	} else {
		return nil, false
	}

	if _, ok := t.workerTaskLookupTable[workerId]; !ok {
		t.workerTaskLookupTable = map[string][]*TaskInfo{}
	}
	t.workerTaskLookupTable[workerId] = append(t.workerTaskLookupTable[workerId], &t.taskmeta[index])
	return &t.taskmeta[index], true
}

//update worker task to Idle and reset workerId to ""
func(t *TaskLookupTable) AllComplete()  bool{
	for _, tsk := range t.taskmeta {
		if tsk.state != Completed {
			return false
		}
	}
	return true
}

func (t *TaskLookupTable) CompleteTask(taskInfo TaskInfo) bool {
	for i, tsk := range t.taskmeta {
		if tsk.TaskId == taskInfo.TaskId {
			tsk.metadata = taskInfo.metadata
			tsk.state = Completed
			t.taskmeta[i] = tsk // update value in taskmeta
			return true
		}
	}
	return false
}

func (t *TaskLookupTable) setTaskInProgress(taskInfo TaskInfo) {
	for i, tsk := range t.taskmeta {
		if tsk.TaskId == taskInfo.TaskId {
			tsk.metadata = taskInfo.metadata
			tsk.state = InProgress
			t.taskmeta[i] = tsk // update value in taskmeta
		}
	}
}


type RequestTaskRequest struct{
	Workermeta Workermeta
}

type RequestTaskReply struct {
	Metadata   TaskInfo
	Workermeta Workermeta
}

type CompleteTaskRequest struct{
	Metadata   TaskInfo
	WorkerMeta Workermeta

}

type CompleteTaskReply struct {
	Metadata   TaskInfo
	WorkerMeta TaskInfo
}

//RPC call for client request task, worker now is available and can receive task
func (m *Master) DoRequestTask(req *RequestTaskRequest, reply *RequestTaskReply) error{
	reqWorkerId := req.Workermeta.WorkerId

	switch m.JobTracker.phase {
	case PhaseMap :
		if m.JobTracker.taskManager.WorkerMonitor.GetWorkerByIdForUpdate(reqWorkerId).State != WIdle  {
			return nil
		}
		t, ok := m.JobTracker.ScheduleAvailableMapTask(req.Workermeta)
		if ok {
			m.JobTracker.taskManager.WorkerMonitor.GetWorkerByIdForUpdate(reqWorkerId).State = WBusy
			reply.Workermeta = Workermeta{
				WorkerId:       req.Workermeta.WorkerId,
				State:          0,
				lastUpdateTime: time.Now().Unix(),
			}
			reply.Metadata = t
			return nil
		} else{
			reply = &RequestTaskReply{}
		}
		return nil
	case PhaseReduce :
		if m.JobTracker.taskManager.WorkerMonitor.GetWorkerByIdForUpdate(reqWorkerId).State != WIdle  {
			return nil
		}
		t, ok := m.JobTracker.ScheduleAvailableReduceTask(req.Workermeta)
		if ok {
			wm := m.JobTracker.taskManager.WorkerMonitor.GetWorkerByIdForUpdate(reqWorkerId)
			wm.State = WBusy
			reply.Workermeta = Workermeta{
				WorkerId:       wm.WorkerId,
				State:          wm.State,
				lastUpdateTime: wm.lastUpdateTime,
			}
			reply.Metadata = t
			return nil
		} else{
			reply = &RequestTaskReply{}
		}
		return nil
	default:
		return nil
	}
}

// listent to the worker state and update worker status periodically
func (a *AnotherJobTracker) StartWorkerListener(){
	ticker := time.NewTicker(3000 * time.Millisecond)
	const MAX_THRESHOLD int64 = 10 * 60 * 1000//10 minutes
	now := time.Now().Unix()

	go func() {
		for {
			select {
			case <-a.JobDone:
				return
			case <-ticker.C:
				for _, worker := range a.taskManager.WorkerMonitor.workers {
					if now - worker.lastUpdateTime > MAX_THRESHOLD {
						if worker.State != WError {
							//exceed max value
							a.taskManager.WorkerMonitor.SetWorkerError(worker)
							workerId := worker.WorkerId
							a.taskManager.WorkerErrorHandlerFunc(workerId, &a.taskManager) //handle error
						}
					}
				}
			}
		}
	}()
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
	NReduce	int32 	// map split intermediate file count
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

//only one job is available JobTracker time now
type AnotherJobTracker struct {
	JobDone                chan bool
	MapDone                sync.Once
	ReduceDone             sync.Once
	mapDone                chan bool
	reduceDone             chan bool
	jobId                  string
	phase                  JobPhase
	mMap                   int32 //map task total count
	numCompleteMapTasks    int32  //current completed map task
	nReduce                int32
	numCompleteReduceTasks int32
	taskManager TaskManager
}

func(a *AnotherJobTracker) CompleteOneMapTask() {
	atomic.AddInt32(&a.numCompleteMapTasks, 1)
}

func(a *AnotherJobTracker) ReleaseOneMapTask() {
	atomic.AddInt32(&a.numCompleteMapTasks, -1)
}

func(a *AnotherJobTracker) AllMapTaskDone() bool {
	return atomic.CompareAndSwapInt32(&a.numCompleteMapTasks, a.mMap, a.mMap)
}

func(a *AnotherJobTracker) CompleteOneReduceTask() {
	atomic.AddInt32(&a.numCompleteReduceTasks, 1)
}

func(a *AnotherJobTracker) ReduceTaskDone() bool {
	return atomic.CompareAndSwapInt32(&a.numCompleteReduceTasks, a.nReduce, a.nReduce)
}

func(a *AnotherJobTracker) Accept(src []string, nReduce int32) {
	a.nReduce = nReduce
	a.jobId = Generate().String()
	a.preGenerateMapTasks(src, nReduce)
}

func (a *AnotherJobTracker) preGenerateMapTasks(src []string, nReduce int32) {
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
		a.taskManager.MapTaskTable.CreateTask(t)
		a.mMap++
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
				Filenames:      a.taskManager.getAllIntermediateFiles(i),
				ResultFilename: "",
			},
		}
		a.taskManager.ReduceTaskTable.CreateTask(t)
	}
}

//select JobTracker map task and change state from idle to im-progress
func(a *AnotherJobTracker) ScheduleAvailableMapTask(workermeta Workermeta) (TaskInfo, bool) {
	if a.phase != PhaseMap {
		return TaskInfo{}, false
	}
	t, ok := a.taskManager.MapTaskTable.AssignNewTask(workermeta.WorkerId)

	if !ok {
		fmt.Println("no job to assigned.")
		return TaskInfo{}, false
	}
	a.taskManager.WorkerMonitor.SetWorkerBusy(workermeta)
	v := *t
	return v, true
}

//create reduce task for workerId
func(a *AnotherJobTracker) ScheduleAvailableReduceTask(worker Workermeta) (TaskInfo, bool) {
	if a.phase != PhaseReduce {
		return TaskInfo{}, false
	}
	t, ok := a.taskManager.ReduceTaskTable.AssignNewTask(worker.WorkerId)

	if !ok {
		fmt.Println("no job to assigned.")
		return TaskInfo{}, false
	}
	a.taskManager.WorkerMonitor.SetWorkerBusy(worker)
	v := *t
	return v, true
}

func(a *Master) init() bool {
	wm := TaskManager{}
	wm.WorkerMonitor = WorkerMonitor{workers: []Workermeta{}}
	wm.MapTaskTable = TaskLookupTable{
		taskmeta:              []TaskInfo{},
		workerTaskLookupTable: map[string][]*TaskInfo{},
	}

	wm.ReduceTaskTable = TaskLookupTable{
		taskmeta:              []TaskInfo{},
		workerTaskLookupTable: map[string][]*TaskInfo{},
	}
	//set error handler func
	wm.WorkerErrorHandlerFunc = a.JobTracker.workerErrorHandler()
	a.JobTracker.taskManager = wm
	return true
}

func (a *AnotherJobTracker) workerErrorHandler() func(workerId string, manager *TaskManager) bool {
	return func(workerId string, manager *TaskManager) bool {
		//when the worker is error, set all the task to idle state
		if a.phase == PhaseMap {
			manager.MapLock.Lock()
			defer manager.MapLock.Unlock()
			manager.MapTaskTable.ReleaseAllTask(workerId)
		}
		if a.phase == PhaseReduce {
			manager.ReduceLock.Lock()
			defer manager.ReduceLock.Unlock()
			manager.ReduceTaskTable.ReleaseUnCompleteTask(workerId)
		}
		return true
	}
}

// create JobTracker Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int32) *Master {
	m := Master{}

	m.init()

	m.JobTracker.Accept(files, nReduce)
	// listen to the worker state
	go m.JobTracker.StartWorkerListener()	//JobTracker.inputsource = src

	m.server()
	return &m
}

// RPC
func (m* Master) CompleteMap(args *CompleteTaskRequest, reply *CompleteTaskReply) error {
	m.JobTracker.taskManager.WorkerMonitor.UpdateWorker(args.WorkerMeta)
	if m.JobTracker.taskManager.CompleteMapTask(args.Metadata) {
		m.JobTracker.CompleteOneMapTask()
		if m.JobTracker.AllMapTaskDone() {
			m.JobTracker.MapDone.Do(func() { //complete map task
				m.JobTracker.phase = PhaseReduce
				m.JobTracker.mapDone <- true
			})
		}
	}
	return nil
}

//RPC
func (m* Master) CompleteReduce(args *CompleteTaskRequest, reply *CompleteTaskReply) error {
	m.JobTracker.taskManager.WorkerMonitor.UpdateWorker(args.WorkerMeta)
	if m.JobTracker.taskManager.CompleteReduceTask(args.Metadata) {
		m.JobTracker.CompleteOneReduceTask()
		if m.JobTracker.ReduceTaskDone() {
			m.JobTracker.ReduceDone.Do(func() { //complete map task
				m.JobTracker.phase = Result
				m.JobTracker.reduceDone <- true
				m.JobTracker.JobDone <- true
			})
		}
	}
	return nil
}
