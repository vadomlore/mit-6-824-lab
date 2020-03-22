package mr

import (
	"bytes"
	"encoding/gob"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"strconv"
	"strings"
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
	Status bool
}

type ReportStatusReply struct{
	Status bool
}


//RPC call join master
func (m *Master) JoinMaster(args *WorkerStatusArgs, reply *JoinMasterReply) error {
	log.Printf("start join master %v.\n", args.WorkerMeta.WorkerId)
	m.JobTracker.taskManager.WorkerMonitor.lock.Lock()
	defer m.JobTracker.taskManager.WorkerMonitor.lock.Unlock()
	m.JobTracker.taskManager.WorkerMonitor.AddNewWorker(args.WorkerMeta)
	reply.Status = true
	log.Printf("worker %v add succeed.\n", args.WorkerMeta.WorkerId)
	return nil
}

//RPC call update to idle if job is reduceDone
func (m *Master) ReportStatus(args *WorkerStatusArgs, reply *ReportStatusReply) error {
	// log.Println("Receive health status")
	m.JobTracker.taskManager.WorkerMonitor.lock.Lock()
	defer m.JobTracker.taskManager.WorkerMonitor.lock.Unlock()
	m.JobTracker.taskManager.WorkerMonitor.UpdateWorker(args.WorkerMeta)
	reply.Status = true
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
//when task is in Idle State, the task can be delivered to any workerId specified by master
//when map task crash in InProgress and Completed State (Caused by health check that the woker machine
//doesn't work anymore), reset task to Idle State, and the master task scheduler will find another
//available machine to schedule the task. when reduce task crash in InPogress State reset to idle and
//the secheduler will do the same work as map schedule, if reduce task is in Completed State, this will
//not trigger
const (
	Idle TaskState=0
	InProgress = 1
	Completed = 2
)

type WorkerState int

const (
	WIdle WorkerState=0
	WBusy = 1
	WError = 2
)

type Workermeta struct {
	WorkerId       string
	State          WorkerState
	LastUpdateTime int64
}

//on worker disabled action
type WorkerErrorHandler func(workerId string) bool

//JobTracker worker can only process one task JobTracker time
// WorkerMonitor worker health status and worker State
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
	if w.Exists(workerMeta) {
		log.Printf("worker %v already join master\n", workerMeta.WorkerId)
		return false
	}
	if workerMeta.LastUpdateTime == 0 {
		workerMeta.LastUpdateTime = time.Now().Unix()
	}
	w.workers = append(w.workers, workerMeta)
	return true
}

func (w *WorkerMonitor) UpdateWorker(workerMeta Workermeta) bool{
	worker, i := w.GetWorkerById(workerMeta.WorkerId)
	if worker.WorkerId != "" {
		if workerMeta.LastUpdateTime < worker.LastUpdateTime { // the received update information is expired (old information may be due to network lag)
			return false
		}
		if worker.State != workerMeta.State {
			log.Printf("Update worker from state %v to state %v\n", worker.State, workerMeta.State)
			worker.State = workerMeta.State
		}
		worker.LastUpdateTime = workerMeta.LastUpdateTime
		w.workers[i] = worker
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

func (w *WorkerMonitor) GetWorkerById(workerId string ) (Workermeta, int) {
	for i, worker := range w.workers {
		if worker.WorkerId == workerId {
			return worker, i
		}
	}
	return  Workermeta{}, -1
}

//used by rpc-connection
func (w *WorkerMonitor) SetWorkerIdle(worker Workermeta){
	for i, e := range w.workers {
		if e.WorkerId == worker.WorkerId {
			e.State = WIdle
			e.LastUpdateTime = time.Now().Unix()
			w.workers[i] = e
		}
	}
}


//used by rpc-connection
func (w *WorkerMonitor) SetWorkerError(worker Workermeta){
	for i, e := range w.workers {
		if e.WorkerId == worker.WorkerId {
			e.State = WError
			e.LastUpdateTime = time.Now().Unix()
			w.workers[i] = e
		}
	}
}

//used by rpc-connection
func (w *WorkerMonitor) SetWorkerBusy(worker Workermeta){
	
	for i, e := range w.workers {
		if e.WorkerId == worker.WorkerId {
			e.State = WBusy
			e.LastUpdateTime = time.Now().Unix()
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

// getAllIntermediateFilesBy intermediate files determined by reduce id, file format is mr-taskid-reduceid
// find intermediate files collected by map phase in the map task and filter filenames with reduceId suffix
func(w *TaskManager) getAllIntermediateFilesBy(reduceId int)[]string{
	var filenames []string
	for _, t := range w.MapTaskTable.taskmeta {
		mapMeta := toMapMetadata(t.Metadata)
		for _, fx := range mapMeta.IntermediateFiles {
			if strings.HasSuffix(fx, strconv.Itoa(reduceId)) {
				filenames = append(filenames, fx)
			}
		}
	}
	return filenames
}

func (w *TaskManager) CompleteMapTask(metadata TaskInfo) bool {
	return w.MapTaskTable.CompleteTask(metadata)
}

func (w *TaskManager) CompleteReduceTask(metadata TaskInfo) bool{
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
			tsk.State = Idle
		}
		//delete the worker task
		delete(t.workerTaskLookupTable, workerId)
	}
}

//update worker task to Idle if task is UnComplete and reset workerId to "" for reduce
func(t *TaskLookupTable) ReleaseUnCompleteTask(workerId string)  {
	if tasks, ok:= t.workerTaskLookupTable[workerId]; ok {
		for _, tsk := range tasks {
			if tsk.State != Completed {
				tsk.State = Idle
			}
		}
		//delete the worker task
		delete(t.workerTaskLookupTable, workerId)
	}
}

//pick an idle task and assign to JobTracker worker
func(t *TaskLookupTable) AssignNewTask(workerId string) (TaskInfo, bool) {
	var task TaskInfo
	index := -1
	for i, tsk := range t.taskmeta {
		if tsk.State == Idle {
			tsk.State = InProgress
			task = tsk
			index = i
			break
		}
	}
	if index != -1 {
		t.taskmeta[index] = task
	} else {
		return TaskInfo{}, false
	}

	if _, ok := t.workerTaskLookupTable[workerId]; !ok {
		t.workerTaskLookupTable = map[string][]*TaskInfo{}
	}
	t.workerTaskLookupTable[workerId] = append(t.workerTaskLookupTable[workerId], &t.taskmeta[index])
	return t.taskmeta[index], true
}

//update worker task to Idle and reset workerId to ""
func(t *TaskLookupTable) AllComplete()  bool{
	for _, tsk := range t.taskmeta {
		if tsk.State != Completed {
			return false
		}
	}
	return true
}

func (t *TaskLookupTable) CompleteTask(taskInfo TaskInfo) bool {
	for i, tsk := range t.taskmeta {
		if tsk.TaskId == taskInfo.TaskId {
			tsk.Metadata = taskInfo.Metadata
			tsk.State = Completed
			t.taskmeta[i] = tsk // update value in taskmeta
			return true
		}
	}
	return false
}

func (t *TaskLookupTable) setTaskInProgress(taskInfo TaskInfo) {
	for i, tsk := range t.taskmeta {
		if tsk.TaskId == taskInfo.TaskId {
			tsk.Metadata = taskInfo.Metadata
			tsk.State = InProgress
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
		log.Printf("request task from %+v in phase %+v \n", req, m.JobTracker.phase)
	if req.Workermeta.State != WIdle {
		return nil
	}
	log.Printf("accept request from idle worker %+v \n", req.Workermeta.WorkerId)
	reqWorkerId := req.Workermeta.WorkerId
	m.JobTracker.taskManager.WorkerMonitor.lock.Lock()
	defer m.JobTracker.taskManager.WorkerMonitor.lock.Unlock()
	switch m.JobTracker.phase {
	case PhaseMap :
		wk, index := m.JobTracker.taskManager.WorkerMonitor.GetWorkerById(reqWorkerId)
		if index != -1 && wk.State != WIdle  {
			return nil
		}
		m.JobTracker.taskManager.MapLock.Lock()
		t, ok := m.JobTracker.ScheduleAvailableMapTask(req.Workermeta)
		if ok {
			wm, _ := m.JobTracker.taskManager.WorkerMonitor.GetWorkerById(reqWorkerId)
			reply.Workermeta = Workermeta{
				WorkerId:        req.Workermeta.WorkerId,
				State:           wm.State,
				LastUpdateTime: time.Now().Unix(),
			}
			log.Printf("allow map for worker %+v \n", req.Workermeta.WorkerId)
			reply.Metadata = t
		} else{
			reply = &RequestTaskReply{}
		}
		m.JobTracker.taskManager.MapLock.Unlock()
		log.Printf("request task reply %+v\n", reply)
		return nil
	case PhaseReduce :
		wk, index := m.JobTracker.taskManager.WorkerMonitor.GetWorkerById(reqWorkerId)
		if  index != -1 && wk.State != WIdle  {
			return nil
		}
		m.JobTracker.taskManager.ReduceLock.Lock()
		t, ok := m.JobTracker.ScheduleAvailableReduceTask(req.Workermeta)
		if ok {
			// get new state
			wm, _ := m.JobTracker.taskManager.WorkerMonitor.GetWorkerById(reqWorkerId)
			reply.Workermeta = Workermeta{
				WorkerId:       wm.WorkerId,
				State:          wm.State,
				LastUpdateTime: wm.LastUpdateTime,
			}
			log.Printf("allow reduce for worker %+v \n", req.Workermeta.WorkerId)
			reply.Metadata = t
		} else{
			reply = &RequestTaskReply{}
		}
		m.JobTracker.taskManager.ReduceLock.Unlock()
		return nil
	default:
		return nil
	}
}

// listent to the worker State and update worker status periodically
func (a *AnotherJobTracker) StartWorkerListener(){
	ticker := time.NewTicker(3000 * time.Millisecond)
	const MAX_THRESHOLD int64 = 10 * 60 * 1000//10 minutes
	now := time.Now().Unix()

	go func() {
		for {
			select {
			case <-ticker.C:
				a.taskManager.WorkerMonitor.lock.Lock()
				for _, worker := range a.taskManager.WorkerMonitor.workers {
					if now - worker.LastUpdateTime > MAX_THRESHOLD {
						if worker.State != WError {
							//exceed max value
							a.taskManager.WorkerMonitor.SetWorkerError(worker)
							workerId := worker.WorkerId
							a.taskManager.WorkerErrorHandlerFunc(workerId) //handle error
						}
					}
				}
				a.taskManager.WorkerMonitor.lock.Unlock()
			}
		}
	}()

}

type TaskInfo struct {
	JobId    string
	TaskId   string
	Type     TaskType
	State    TaskState
	Metadata []byte
}

// meta data for map & reduce task information transform to master
// the map task need to maintain the map file delivered by master
// in this lab for simplicity only one file
type MapMetadata struct {
	NReduce	int32 	// map split intermediate file count
	Filename string // map source filename
	IntermediateFiles []string // map generated intermediate files
}



func (metadata MapMetadata) toByteBuffer() []byte{
	var converter bytes.Buffer
	encoder := gob.NewEncoder(&converter)
	err := encoder.Encode(metadata)
	if err != nil {
		log.Println("encode message error")
	}
	return converter.Bytes()
}

func (metadata ReduceMetadata) toByteBuffer() []byte{
	var converter bytes.Buffer
	encoder := gob.NewEncoder(&converter)
	err := encoder.Encode(metadata)
	if err != nil {
		log.Println("encode message error")
	}
	return converter.Bytes()
}

func toMapMetadata(meta []byte) MapMetadata{
	buffer := bytes.NewBuffer(meta)
	var m MapMetadata
	decoder := gob.NewDecoder(buffer)
	err := decoder.Decode(&m)
	if err != nil {
		log.Fatal("decode error")
	}
	return m
}

func toReduceMetadata(meta []byte) ReduceMetadata{
	buffer := bytes.NewBuffer(meta)
	var m ReduceMetadata
	decoder := gob.NewDecoder(buffer)
	err := decoder.Decode(&m)
	if err != nil {
		log.Fatal("decode error")
	}
	return m
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
	MapDoneSync            sync.Once
	ReduceDoneSync         sync.Once
	mapDone                chan bool
	reduceDone             chan bool
	jobId                  string
	phase                  JobPhase
	mMap                   int32 //map task total count
	numCompleteMapTasks    int32  //current completed map task
	nReduce                int32
	numCompleteReduceTasks int32
	taskManager            TaskManager
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

func(a *AnotherJobTracker) CompleteOneReduceTask() int32 {
	remain := atomic.AddInt32(&a.numCompleteReduceTasks, 1)
	return remain
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
	//pre generate  map tasks with idle State, waiting for worker to pick
	for _, s := range src {
		s := s
		t := TaskInfo{
			JobId:  a.jobId,
			TaskId: Generate().String(),
			Type:   Map,
			State:  Idle,
			Metadata: MapMetadata{
				NReduce:           nReduce,
				Filename:          s,
				IntermediateFiles: []string{},
			}.toByteBuffer(),
		}
		a.taskManager.MapTaskTable.CreateTask(t)
		a.mMap++
	}

	log.Printf("generated map task %+v\n", a.taskManager.MapTaskTable)
}

func (a *AnotherJobTracker) preGenerateReduceTasks(nReduce int) {
	//pre generate  map tasks with idle State, waiting for worker to pick
	for i:= 0; i < nReduce; i++ {
		t := TaskInfo{
			JobId:  a.jobId,
			TaskId: Generate().String(),
			Type:   Reduce,
			State:  Idle,
			Metadata: ReduceMetadata{
				ReduceId:       strconv.Itoa(i),
				Filenames:      a.taskManager.getAllIntermediateFilesBy(i),
				ResultFilename: "",
			}.toByteBuffer(),
		}
		a.taskManager.ReduceTaskTable.CreateTask(t)
	}
}

//select JobTracker map task and change State from idle to im-progress
func(a *AnotherJobTracker) ScheduleAvailableMapTask(workermeta Workermeta) (TaskInfo, bool) {
	if a.phase != PhaseMap {
		return TaskInfo{}, false
	}
	t, ok := a.taskManager.MapTaskTable.AssignNewTask(workermeta.WorkerId)

	if !ok {
		log.Println("no job to assigned.")
		return TaskInfo{}, false
	}
	a.taskManager.WorkerMonitor.SetWorkerBusy(workermeta)
	v := t
	return v, true
}

//create reduce task for workerId
func(a *AnotherJobTracker) ScheduleAvailableReduceTask(worker Workermeta) (TaskInfo, bool) {
	if a.phase != PhaseReduce {
		return TaskInfo{}, false
	}
	t, ok := a.taskManager.ReduceTaskTable.AssignNewTask(worker.WorkerId)

	if !ok {
		log.Println("no job to assigned.")
		return TaskInfo{}, false
	}
	a.taskManager.WorkerMonitor.SetWorkerBusy(worker)
	v := t
	return v, true
}

func(a *Master) init() bool {
	a.JobTracker = AnotherJobTracker{
		taskManager:            TaskManager{
			WorkerMonitor:WorkerMonitor{workers: []Workermeta{}},
			MapTaskTable:TaskLookupTable{
				taskmeta:              []TaskInfo{},
				workerTaskLookupTable: map[string][]*TaskInfo{},
			},
			ReduceTaskTable: TaskLookupTable{
				taskmeta:              []TaskInfo{},
				workerTaskLookupTable: map[string][]*TaskInfo{},
			},
			WorkerErrorHandlerFunc: a.JobTracker.workerErrorHandler(),
		},
	}
	return true
}

func (a *AnotherJobTracker) workerErrorHandler() func(workerId string) bool {
	return func(workerId string) bool {
		//when the worker is error, set all the task to idle State
		if a.phase == PhaseMap {
			a.taskManager.MapLock.Lock()
			defer a.taskManager.MapLock.Unlock()
			a.taskManager.MapTaskTable.ReleaseAllTask(workerId)
		}
		if a.phase == PhaseReduce {
			a.taskManager.ReduceLock.Lock()
			defer a.taskManager.ReduceLock.Unlock()
			a.taskManager.ReduceTaskTable.ReleaseUnCompleteTask(workerId)
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
	// listen to the worker State
	go m.JobTracker.StartWorkerListener()	//JobTracker.inputsource = src

	m.server()
	return &m
}

// RPC
func (m* Master) CompleteMap(args *CompleteTaskRequest, reply *CompleteTaskReply) error {
	m.JobTracker.taskManager.WorkerMonitor.lock.Lock()
	m.JobTracker.taskManager.MapLock.Lock()

	m.JobTracker.taskManager.WorkerMonitor.UpdateWorker(args.WorkerMeta)
	defer func() {
		m.JobTracker.taskManager.WorkerMonitor.lock.Unlock()
		m.JobTracker.taskManager.MapLock.Unlock()
	}()
	if m.JobTracker.taskManager.CompleteMapTask(args.Metadata) {
		m.JobTracker.CompleteOneMapTask()
		if m.JobTracker.AllMapTaskDone() {
			m.JobTracker.MapDoneSync.Do(func() { //complete map task
				m.JobTracker.phase = PhaseReduce
				log.Println("Map task done................................................")
				log.Println("start reduce task................................................")
				// do reduce work presign
				log.Println("pre-generate reduce tasks................................................")
				m.JobTracker.preGenerateReduceTasks((int)(m.JobTracker.nReduce))
			})
		}
	}
	return nil
}

//RPC
func (m* Master) CompleteReduce(args *CompleteTaskRequest, reply *CompleteTaskReply) error {
	m.JobTracker.taskManager.WorkerMonitor.lock.Lock()
	m.JobTracker.taskManager.ReduceLock.Lock()
	m.JobTracker.taskManager.WorkerMonitor.UpdateWorker(args.WorkerMeta)
	defer func() {
		m.JobTracker.taskManager.WorkerMonitor.lock.Unlock()
		m.JobTracker.taskManager.ReduceLock.Unlock()
	}()
	if m.JobTracker.taskManager.CompleteReduceTask(args.Metadata) {
		remain := m.JobTracker.CompleteOneReduceTask()
		log.Printf("remain reduce task %v \n", remain);
		if m.JobTracker.ReduceTaskDone() {
			log.Printf("reduce task completed. \n");
			m.JobTracker.ReduceDoneSync.Do(func() { //complete map task
				m.JobTracker.phase = Result
				m.JobTracker.reduceDone <- true
				m.JobTracker.JobDone <- true
			})
		}
	}
	return nil
}
