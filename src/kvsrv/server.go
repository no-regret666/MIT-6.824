package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex
	// Your definitions here.
	kv      map[string]string
	history map[uint64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.kv[args.Key]
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, exists := kv.history[args.Seq]
	if exists {
		return
	}
	kv.kv[args.Key] = args.Value
	kv.history[args.Seq] = ""
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	result, exists := kv.history[args.Seq]
	if exists {
		reply.Value = result
		return
	} else {
		kv.history[args.Seq] = kv.kv[args.Key]
		reply.Value = kv.kv[args.Key]
		kv.kv[args.Key] += args.Value
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kv = make(map[string]string)
	kv.history = make(map[uint64]string)
	return kv
}
