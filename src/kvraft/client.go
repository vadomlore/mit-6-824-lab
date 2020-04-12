package kvraft

import (
	"../labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	cachedLeaderIndex int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	getArgs := GetArgs{
		Key: key,
	}
	for {
		reply := GetReply{}
		DPrintf("[client:]Send to server peer-%v Get %v", ck.cachedLeaderIndex, getArgs)
		done := make(chan bool, 1)
		go func() {
			ok := ck.servers[ck.cachedLeaderIndex].Call("KVServer.Get", &getArgs, &reply)
			done <- ok
		}()

		select {
		case <-time.After(200 * time.Millisecond):
			{
				ck.cachedLeaderIndex = (ck.cachedLeaderIndex + 1) % len(ck.servers)
				continue
			}
		case ok := <-done:
			{
				if ok && reply.Err != ErrWrongLeader {
					if reply.Err == OK {
						goto EXIT
					}
				}
				if reply.Err == ErrWrongLeader {
					ck.cachedLeaderIndex = (ck.cachedLeaderIndex + 1) % len(ck.servers)
					continue
				}
			}
		}
	EXIT:
		return reply.Value
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:   key,
		Value: value,
		Op:    op,
	}
	for {
		reply := PutAppendReply{}
		DPrintf("[client:]Send to server peer-%v PutAppend %v", ck.cachedLeaderIndex, args)
		done := make(chan bool, 1)
		go func() {
			ok := ck.servers[ck.cachedLeaderIndex].Call("KVServer.PutAppend", &args, &reply)
			done <- ok
		}()

		select {
		case <-time.After(200 * time.Millisecond):
			{
				ck.cachedLeaderIndex = (ck.cachedLeaderIndex + 1) % len(ck.servers)
				continue
			}
		case ok := <-done:
			{
				if ok && reply.Err != ErrWrongLeader {
					if reply.Err == OK {
						ck.cachedLeaderIndex = (ck.cachedLeaderIndex + 1) % len(ck.servers)
						goto EXIT
					}
				}
				if reply.Err == ErrWrongLeader {
					ck.cachedLeaderIndex = (ck.cachedLeaderIndex + 1) % len(ck.servers)
					continue
				}
			}
		}
	EXIT:
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
