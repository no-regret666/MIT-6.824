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
	history map[int64]*Result
}

type Result struct {
	lastSeq uint64
	value   string
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
	result, exists := kv.history[args.Identifier]
	if exists {
		if result.lastSeq >= args.Seq {
			return
		}
	} else {
		kv.history[args.Identifier] = new(Result)
	}
	kv.kv[args.Key] = args.Value
	kv.history[args.Identifier].lastSeq = args.Seq
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	result, exists := kv.history[args.Identifier]
	if exists {
		if result.lastSeq >= args.Seq {
			reply.Value = result.value
			return
		}
	} else {
		kv.history[args.Identifier] = new(Result)
	}
	kv.history[args.Identifier].value = kv.kv[args.Key]
	reply.Value = kv.kv[args.Key]
	kv.kv[args.Key] += args.Value
	kv.history[args.Identifier].lastSeq = args.Seq
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.kv = make(map[string]string)
	kv.history = make(map[int64]*Result)
	return kv
}
