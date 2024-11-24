package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"log"
	"sort"

	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const heartBeatTimeout time.Duration = 100 * time.Millisecond

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int   //当前任期
	votedFor    int   //当前任期投票对象
	log         []Log //日志

	electionTimeout  time.Duration //选举超时时间
	time             time.Time     //开始计算选举超时的时间
	heartBeatTimeout time.Duration //心跳超时时间
	lastHeartBeat    time.Time     //上一次心跳时间
	state            string        //当前身份
	applyCh          chan ApplyMsg //应用状态机通道
	commitIndex      int           //已提交的最高的日志条目的索引
	lastApplied      int           //已经被应用到状态机的最高的日志条目的索引
	nextIndex        []int         //对于每一台服务器，发送到该服务器的下一个日志条目的索引
	matchIndex       []int         //对于每一台服务器，已知的已经复制到该服务器的最高日志条目的索引
	applyCond        *sync.Cond
	appendCond       []*sync.Cond
	lastIncludeIndex int    //日志快照覆盖的最后一条日志索引
	lastIncludeTerm  int    //任期
	snapshot         []byte //日志快照
}

type Log struct {
	Term    int
	Index   int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (3A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.state == "Leader"
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	//e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.log) != nil {
		log.Printf("persist failed for term %d: %v", rf.currentTerm, e)
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//  rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var tmpCurrentTerm int
	var tmpVotedFor int
	var tmpLog []Log
	if d.Decode(&tmpCurrentTerm) != nil || d.Decode(&tmpVotedFor) != nil || d.Decode(&tmpLog) != nil {
		log.Printf("readPersist failed for term %d: %v", rf.currentTerm, d)
	} else {
		rf.currentTerm = tmpCurrentTerm
		rf.votedFor = tmpVotedFor
		rf.log = tmpLog
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).
	pos := index - rf.lastIncludeIndex - 1
	rf.log = append(rf.log[1:], rf.log[pos+1:]...)
	rf.snapshot = snapshot
	rf.lastIncludeIndex = index
	rf.persist()
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int //候选人任期号
	CandidateId  int //请求选票的候选人的ID
	LastLogIndex int //候选人的最后日志条目的索引值
	LastLogTerm  int //候选人最后日志条目的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int  //当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool //候选人赢得了此张选票时为真
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.state = "Follower"
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	if len(rf.log) != 0 {
		lastLog := rf.log[len(rf.log)-1]
		if args.LastLogTerm < lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex < lastLog.Index) {
			reply.Term = rf.currentTerm
			reply.VoteGranted = false
			rf.persist()
			return
		}
	}
	rf.votedFor = args.CandidateId
	rf.resetTimeout()
	reply.Term = rf.currentTerm
	reply.VoteGranted = true
	DPrintf("[%d] %d agree %d to become leader", rf.currentTerm, rf.me, rf.votedFor)
	rf.persist()
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Log          []Log
	LeaderCommit int
	LogIndex     int //leader最新日志的索引
}

type AppendEntriesReply struct {
	Term    int
	Success string
	XTerm   int //冲突Log任期号
	XIndex  int //对应任期号为XTerm的第一条Log的槽位号/下一条日志索引
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs,
	reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	reply.Term = rf.currentTerm
	reply.Success = "false"
	reply.XTerm = -1
	reply.XIndex = args.PrevLogIndex
	if args.Term < rf.currentTerm {
		return
	}
	//lab3C : 判断是否是无效请求
	if len(args.Log) > 0 {
		if len(rf.log)-1 >= args.Log[len(args.Log)-1].Index {
			reply.Success = "pass"
			return
		}
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.state = "Follower"
	rf.resetTimeout()
	if len(args.Log) > 0 {
		if args.PrevLogIndex >= len(rf.log) {
			reply.XIndex = rf.lastApplied + 1
			DPrintf("[%d] %d 's logs length %d are less than the leader's ", rf.currentTerm, len(rf.log), rf.me)
			return
		}
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.XIndex = rf.lastApplied + 1
			DPrintf("[%d] %d 's logs conflict with the leader's", rf.currentTerm, rf.me)
			return
		}
		rf.log = append(rf.log[:args.PrevLogIndex-rf.lastIncludeTerm], args.Log...)
		DPrintf("[%d] %d successfully append logs to %d from the leader %d", rf.currentTerm, rf.me, len(rf.log)-1, args.LeaderId)
	}
	prevCommitIndex := rf.commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		DPrintf("[%d] %d update commitIndex to %d according to the leader %d", rf.currentTerm, rf.me, rf.commitIndex, args.LeaderId)
	}
	if rf.commitIndex > prevCommitIndex {
		rf.applyCond.Signal()
	}
	reply.Success = "true"
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}

type InstallSnapshotReply struct {
	term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()
	if args.Term < rf.currentTerm {

	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) callRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	if _, is := rf.GetState(); is == false {
		return -1, -1, false
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	newLog := Log{
		Term:    rf.currentTerm,
		Index:   len(rf.log),
		Command: command,
	}
	rf.log = append(rf.log, newLog)
	rf.persist()
	DPrintf("[%d] %d receive a new command %v,then append at %d", rf.currentTerm, rf.me, command, newLog.Index)
	rf.startAppendEntries(false)
	return newLog.Index, newLog.Term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) resetTimeout() {
	rf.time = time.Now()
	ms := 300 + (rand.Int63() % 300)
	rf.electionTimeout = time.Duration(ms) * time.Millisecond
}

func (rf *Raft) startElection() {
	DPrintf("[%d] %d try to be Leader", rf.currentTerm, rf.me)
	voted := 1
	args := RequestVoteArgs{}
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogTerm = -1
	args.LastLogIndex = -1
	if len(rf.log) != 0 {
		args.LastLogTerm = rf.log[len(rf.log)-1].Term
		args.LastLogIndex = rf.log[len(rf.log)-1].Index
	}

	success := false
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go func(peer int) {
			reply := RequestVoteReply{}
			if rf.callRequestVote(peer, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.VoteGranted {
					if rf.currentTerm == args.Term && rf.state == "Candidate" {
						voted++
						if voted >= len(rf.peers)/2+1 {
							DPrintf("[%d] %d get more than half votes,succeed to be leader", rf.currentTerm, rf.me)
							for i := 0; i < len(rf.peers); i++ {
								rf.nextIndex[i] = len(rf.log)
							}
							success = true
							rf.state = "Leader"
							rf.startAppendEntries(true)
							return
						}
					}
				} else if reply.Term > rf.currentTerm {
					rf.state = "Follower"
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					DPrintf("[%d] %d fail to be leader", rf.currentTerm, rf.me)
					rf.persist()
					return
				}
			}
		}(peer)
		if success {
			return
		}
	}
}

func (rf *Raft) judgeElectionTimeout() bool {
	return time.Now().Sub(rf.time) > rf.electionTimeout
}

func (rf *Raft) judgeHeartBeatTimeout() bool {
	return time.Now().Sub(rf.lastHeartBeat) > rf.heartBeatTimeout
}

func (rf *Raft) updateCommitIndex() {
	prevCommitIndex := rf.commitIndex
	rf.matchIndex[rf.me] = len(rf.log) - 1
	for i, value := range rf.matchIndex {
		DPrintf("%d : %d", i, value)
	}
	nums := make([]int, len(rf.peers))
	copy(nums, rf.matchIndex)
	sort.Ints(nums)
	rf.commitIndex = nums[len(nums)/2]
	if rf.commitIndex > prevCommitIndex {
		rf.applyCond.Signal()
	}
	DPrintf("[%d] %d update commitIndex to %d", rf.currentTerm, rf.me, rf.commitIndex)
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		switch rf.state {
		case "Leader":
			if rf.judgeHeartBeatTimeout() {
				rf.lastHeartBeat = time.Now()
				rf.startAppendEntries(true)
			}
		case "Follower":
			fallthrough
		case "Candidate":
			if rf.judgeElectionTimeout() {
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.state = "Candidate"
				rf.persist()
				rf.resetTimeout()
				rf.startElection()
			}
		}
		rf.mu.Unlock()
		time.Sleep(200 * time.Millisecond)
	}
}

// 发送日志/心跳
func (rf *Raft) startAppendEntries(isHeartBeat bool) {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			DPrintf("[%d] %d send %d heartbeat", rf.currentTerm, rf.me, peer)
			go rf.sendAppendEntries(peer)
		} else {
			rf.appendCond[peer].Signal()
		}
	}
}

func (rf *Raft) sendAppendEntries(peer int) {
	rf.mu.RLock()
	if rf.state != "Leader" {
		rf.mu.RUnlock()
		return
	}
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Log:          nil,
		LeaderCommit: rf.commitIndex,
		LogIndex:     rf.lastIncludeIndex + len(rf.log),
	}
	//发送snapshot
	if rf.nextIndex[peer] <= rf.lastIncludeIndex {

	}
	args.PrevLogIndex = rf.nextIndex[peer] - 1
	for rf.nextIndex[peer] > rf.lastIncludeIndex {
		args.PrevLogIndex = rf.nextIndex[peer] - 1
		if args.PrevLogIndex >= len(rf.log)+rf.lastIncludeIndex+1 {
			rf.nextIndex[peer]--
			continue
		} //不理解为什么
		if args.PrevLogIndex == rf.lastIncludeIndex {
			args.PrevLogTerm = rf.lastIncludeTerm
		} else {
			args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.lastIncludeIndex-1].Term
		}
		break
	}
	if rf.nextIndex[peer] < len(rf.log)+rf.lastIncludeIndex+1 {
		args.Log = make([]Log, args.LogIndex+1-rf.nextIndex[peer])
		copy(args.Log, rf.log[rf.nextIndex[peer]-rf.lastIncludeIndex-1:args.LogIndex-rf.lastIncludeIndex])
		DPrintf("[%d] %d send %d %d logs from %d", rf.currentTerm, rf.me, peer, len(args.Log), rf.nextIndex[peer])
	}
	rf.mu.RUnlock()
	reply := AppendEntriesReply{}
	for {
		if rf.killed() {
			return
		}
		ok := rf.callAppendEntries(peer, &args, &reply)
		if ok {
			break
		}
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term != rf.currentTerm { //过期消息
		return
	}
	if reply.Term > rf.currentTerm {
		rf.state = "Follower"
		rf.resetTimeout()
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		DPrintf("[%d] a new leader happen,%d become follower", rf.currentTerm, rf.me)
		return
	}
	if reply.Success == "true" {
		rf.nextIndex[peer] = args.PrevLogIndex + len(args.Log) + 1
		rf.matchIndex[peer] = args.PrevLogIndex + len(args.Log)
		rf.updateCommitIndex()
		return
	} else if reply.Success == "false" {
		rf.nextIndex[peer] = max(reply.XIndex, 1)
		DPrintf("[%d] %d change %d's nextIndex to %d", rf.currentTerm, rf.me, peer, rf.nextIndex[peer])
	}
}

func (rf *Raft) callAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// leader判断follower是否
func (rf *Raft) appender(server int) {
	rf.appendCond[server].L.Lock()
	defer rf.appendCond[server].L.Unlock()
	for rf.killed() == false {
		if !rf.needAppend(server) {
			rf.appendCond[server].Wait()
		}
		rf.sendAppendEntries(server)
	}
}

func (rf *Raft) needAppend(server int) bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.state == "leader" && rf.matchIndex[server] < len(rf.log)-1
}

func (rf *Raft) applyCommited() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.log[rf.commitIndex-rf.lastIncludeIndex-1].Term != rf.currentTerm {
			rf.mu.Unlock()
			continue
		}
		rf.lastApplied++
		index := rf.lastApplied
		command := rf.log[index-rf.lastIncludeIndex-1].Command
		msg := ApplyMsg{
			CommandValid: true,
			Command:      command,
			CommandIndex: index,
		}
		DPrintf("[%d] %d apply the log %d %v", rf.currentTerm, rf.me, index, command)
		rf.applyCh <- msg
		rf.mu.Unlock()
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = "Follower"
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Log, 1)
	rf.log[0] = Log{}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.resetTimeout()
	rf.heartBeatTimeout = heartBeatTimeout
	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.appendCond = make([]*sync.Cond, len(rf.peers))
	rf.lastIncludeIndex = -1
	rf.lastIncludeTerm = 0

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
		if i != rf.me {
			rf.appendCond[i] = sync.NewCond(&sync.Mutex{})
			go rf.appender(i)
		}
	}

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.applyCommited()

	return rf
}
