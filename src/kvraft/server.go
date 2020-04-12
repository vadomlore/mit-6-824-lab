package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpId string // Get/Put/Append
	Args []byte //GetArgs //PutAppendArgs
}

func newGetLog(args *GetArgs) Op {
	op := Op{}
	rb := new(bytes.Buffer)
	e := labgob.NewEncoder(rb)
	e.Encode(args)
	op.OpId = "Get"
	op.Args = rb.Bytes()
	return op
}

func newGetCommand(op *Op) GetArgs {
	args := GetArgs{}
	rb := bytes.NewBuffer(op.Args)
	d := labgob.NewDecoder(rb)
	d.Decode(&args)
	return args
}

func newPutAppendCommand(op *Op) PutAppendArgs {
	args := PutAppendArgs{}
	rb := bytes.NewBuffer(op.Args)
	d := labgob.NewDecoder(rb)
	err := d.Decode(&args)
	if err != nil {
		log.Fatal(err)
	}
	return args
}

func newPutAppendLog(args *PutAppendArgs) Op {
	op := Op{}
	rb := new(bytes.Buffer)
	e := labgob.NewEncoder(rb)
	e.Encode(args)
	op.OpId = args.Op
	op.Args = rb.Bytes()
	return op
}

func (server *KVServer) doGet(getArgs *GetArgs) GetReply {
	if v, ok := server.db[getArgs.Key]; ok {
		return GetReply{
			Err:   OK,
			Value: v,
		}
	} else {
		return GetReply{
			Err:   ErrNoKey,
			Value: "",
		}
	}
}

func (kv *KVServer) doPutAppend(args *PutAppendArgs) PutAppendReply {
	if args.Op == "Put" {
		kv.db[args.Key] = args.Value
		return PutAppendReply{
			Err: OK,
		}
	} else if args.Op == "Append" {
		v, ok := kv.db[args.Key]
		if !ok {
			kv.db[args.Key] = args.Value
		} else {
			kv.db[args.Key] = v + args.Value
		}
		return PutAppendReply{
			Err: OK,
		}
	}
	return PutAppendReply{}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db map[string]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[server:] Start Get")
	_, _, isLeader := kv.rf.Start(newGetLog(args))
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := <-kv.applyCh

	op := (r.Command).(Op)
	if op.OpId == "Get" {
		c := newGetCommand(&op)
		DPrintf("Committed Get args: %#v", c)
		rp := kv.doGet(&c)
		reply.Value = rp.Value
		reply.Err = rp.Err
	}

	DPrintf("[server:] Committed Get Done")
	DPrintf("[server:] Get resp channel %v", r)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, _, isLeader := kv.rf.Start(newPutAppendLog(args))

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	r := <-kv.applyCh

	op := (r.Command).(Op)
	DPrintf("Op: %v\n", op.OpId)
	if op.OpId == "Put" || op.OpId == "Append" {
		c := newPutAppendCommand(&op)
		DPrintf("Committed %v args: %#v", op.OpId, c)
		rp := kv.doPutAppend(&c)
		reply.Err = rp.Err
	}

	DPrintf("[server:] Committed PutAppend Done")
	DPrintf("[server:] get resp channel PutAppend %v", r)
	DPrintf("[server:] current store %v", kv.db)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(GetArgs{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.

	return kv
}
