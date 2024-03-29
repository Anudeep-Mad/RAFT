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

import "sync"
import "labrpc"
import "time"
import "math/rand"


import "bytes"
import "labgob"



//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
    CommandValid bool
    Command      interface{}
    CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
    mu        sync.Mutex          // Lock to protect shared access to this peer's state
    peers     []*labrpc.ClientEnd // RPC end points of all peers
    persister *Persister          // Object to hold this peer's persisted state
    me        int                 // this peer's index into peers[]

    // Your data here (2A, 2B, 2C).
    state int
    currentTerm int
    sum int
    votedFor int
    maxchan chan int
    heartbeat chan int
    recvchan chan int
    //Integer variable for all non-leader servers to timeout when hearbeat not received in a time frame.
    log []Log
    kill bool
    //Volatile states
    commitIndex int
    lastApplied int
    commitChan chan ApplyMsg
    //Leader volatile states
    nextIndex []int
    matchIndex []int
    committed bool

    // Look at the paper's Figure 2 for a description of what
    // state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

    var term int
    var isleader bool
    // Your code here (2A).
    rf.mu.Lock()
    term = rf.currentTerm
    if rf.state==2{
        isleader = true
    }else{
        isleader=false
    }
    rf.mu.Unlock()
    return term, isleader
}


//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
    // Your code here (2C).
    // Example:
    // w := new(bytes.Buffer)
    // e := labgob.NewEncoder(w)
    // e.Encode(rf.xxx)
    // e.Encode(rf.yyy)
    // data := w.Bytes()
    // rf.persister.SaveRaftState(data)
    w:= new(bytes.Buffer)
    e:= labgob.NewEncoder(w)
    //e.Encode(rf.state)
    e.Encode(rf.currentTerm)
    e.Encode(rf.log)
    e.Encode(rf.votedFor)
    data:= w.Bytes()
    rf.persister.SaveRaftState(data)

}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
    if data == nil || len(data) < 1 { // bootstrap without any state?
        return
    }
    // Your code here (2C).
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
    //   rf.yyy = yyy
    // }
    r := bytes.NewBuffer(data)
    d := labgob.NewDecoder(r)
    var currentTerm int
    var votedFor int
    var log []Log
    if d.Decode(&currentTerm) !=nil || d.Decode(&log)!=nil || d.Decode(&votedFor)!= nil{
        //error...
    } else {

        rf.currentTerm = currentTerm
        rf.log = log
        rf.votedFor = votedFor

    }


}

type Log struct{
    LogIndex int
    TermIndex int
    Message interface{}
}


type AppendEntriesArgs struct{
    //Value int
    Term int
    LeaderId int
    PrevLogTerm int
    PrevLogIndex int
    Logs []Log
    LeaderCommit int
}

type AppendEntriesReply struct{
    ConflictIndex int
    Success bool
    Term int
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
    ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
    return ok
}

func (rf* Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

    rf.mu.Lock()
    if args.Term<rf.currentTerm{
        reply.Success=false
        reply.Term=rf.currentTerm
        rf.mu.Unlock()
        return
    }
    reply.Term=args.Term

    rf.mu.Unlock()
    rf.heartbeat<-1
    rf.mu.Lock()
    if rf.currentTerm<args.Term{

        rf.state=0
        rf.votedFor=-1
        rf.currentTerm=args.Term
        rf.persist()
    }


    args_prev_index:=args.PrevLogIndex
    rf_last_index:=rf.Log_Last_Index()

    if args_prev_index>rf_last_index{
        reply.Success=false
        reply.ConflictIndex=rf.log[rf.Log_Last_Index()].LogIndex+1
        rf.mu.Unlock()
        return

    }

    if rf.Log_Last_Index() >= args_prev_index{
        if rf.log[args_prev_index].TermIndex==args.PrevLogTerm{

            reply.Success=true
            rf.log=rf.log[0:args.PrevLogIndex+1]
            if args.Logs!=nil{


                rf.log=append(rf.log,args.Logs...)
                rf.persist()
            }
            reply.ConflictIndex=rf.log[rf.Log_Last_Index()].LogIndex+1
            commitIndex:=rf.commitIndex

            if args.LeaderCommit>commitIndex{

                rf.committed=true
                if rf.Log_Last_Index() > args.LeaderCommit{
                    rf.commitIndex = args.LeaderCommit
                }else{
                    rf.commitIndex = rf.Log_Last_Index()
                }
                rf.mu.Unlock()
                go func(){
                    //Go routine to execute and apply state machine commands
                    rf.ApplyMessages()
                }()
                rf.mu.Lock()

            }


        }else{
            reply.Success=false
            ind:= rf.Log_Last_Index()
            for ind>=0{
                if rf.log[ind].TermIndex==args.PrevLogTerm{
                    reply.ConflictIndex=ind+1
                    rf.mu.Unlock()
                    return
                    break
                }else{
                    ind--
                }
            }
            reply.ConflictIndex=args.PrevLogIndex
        }

    }
    rf.mu.Unlock()


}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
    // Your data here (2A, 2B).
    Term int
    CandidateId int
    LastLogIndex int
    LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
    // Your data here (2A).
    Term int
    Success bool

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
    var flag bool
    var flag1 bool
    flag1 = false
    flag=false
    rf.mu.Lock()
    //fmt.Println(rf.me," with term ",rf.currentTerm," received vote request from candidate ",args.CandidateId," with term ",args.Term)

    if args.Term <rf.currentTerm{
        reply.Success=false
        reply.Term = rf.currentTerm
        rf.mu.Unlock()
        return
    }
    reply.Term=args.Term

    if args.Term>rf.currentTerm{
        rf.state=0
        rf.currentTerm=args.Term
        rf.votedFor=-1
        rf.persist()
    }


    if (rf.Log_Last_Term() == args.LastLogTerm && rf.Log_Last_Index()<=args.LastLogIndex) || rf.Log_Last_Term()< args.LastLogTerm{
        flag = true

    }




    if flag && (rf.votedFor==-1 || rf.votedFor==args.CandidateId){
        rf.currentTerm=args.Term
        rf.votedFor=args.CandidateId
        rf.state=0
        reply.Term=args.Term
        reply.Success=true

        rf.persist()
        flag1=true
        //fmt.Println(rf.me," with log ",rf.log," and term: ",rf.currentTerm," voted for ",args.CandidateId," and term ",args.Term)
    }
    rf.mu.Unlock()
    if flag1{
        //        go func(){
        rf.recvchan<-1
        //        }()
    }
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
    ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
    return ok
}

func (rf *Raft) Log_Last_Index() int  {
    length:= len(rf.log)
    val:= rf.log[length-1].LogIndex
    return val

}

func (rf *Raft) Log_Last_Term() int {
    length:= len(rf.log)
    val:= rf.log[length-1].TermIndex
    return val
}

func (rf *Raft) ApplyMessages(){
    rf.mu.Lock()
    a:= rf.lastApplied
    b:= rf.commitIndex
    rf.mu.Unlock()
    for a <b{
        a++
        //rf.persist()
        var apply ApplyMsg
        apply.CommandValid =true
        rf.mu.Lock()
        apply.Command=rf.log[a].Message
        rf.mu.Unlock()
        apply.CommandIndex=a
        //fmt.Println(rf.me," leader applying message ",apply.Command," with log: ",rf.log," with term ",rf.currentTerm)
        rf.commitChan<-apply
    }
    rf.mu.Lock()
    rf.lastApplied=a
    rf.mu.Unlock()
}


//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
    index := -1
    term := -1
    isLeader := true

    // Your code here (2B).
    term,isLeader= rf.GetState()
    if isLeader{
        rf.mu.Lock()
        index=rf.Log_Last_Index()+1
        //fmt.Println(rf.me," taking command from start ",command," with log ",rf.log)
        rf.log=append(rf.log,Log{LogIndex:index,TermIndex:term,Message:command})
        rf.persist()
        rf.mu.Unlock()
    }

    return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
    rf.mu.Lock()
    rf.kill=true
    rf.mu.Unlock()
    // Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
    persister *Persister, applyCh chan ApplyMsg) *Raft {
    rf := &Raft{}
    rf.peers = peers
    rf.persister = persister
    rf.me = me

    // Your initialization code here (2A, 2B, 2C).
    rf.currentTerm=0
    rf.votedFor=-1
    rf.state=0
    rf.maxchan = make(chan int,5)
    rf.heartbeat = make(chan int,500)
    rf.recvchan = make(chan int,5)
    rf.commitIndex = 0
    rf.kill=false
    rf.lastApplied = -1
    rf.log=append(rf.log,Log{LogIndex:0,TermIndex:0,Message:0})
    rf.nextIndex = make([]int, len(peers))
    rf.matchIndex = make([]int, len(peers))
    rf.committed = false
    rf.commitChan = applyCh

    go func(){
        for{
            rf.mu.Lock()
            if rf.kill{
                rf.mu.Unlock()
                return
            }
            raft_state:=rf.state
            rf.mu.Unlock()

            if raft_state==0{
                select{
                case<-rf.heartbeat:


                case<-rf.recvchan:
                    //fmt.Println("Voted ")
                case<-time.After(time.Duration(rand.Intn(150)+200)*time.Millisecond):
                    rf.mu.Lock()
                    //fmt.Println(rf.me," becoming candidate with log: ",rf.log)
                    rf.state=1
                    rf.mu.Unlock()
                }
            }else if raft_state==1{

                var args RequestVoteArgs
                rf.mu.Lock()
                rf.currentTerm+=1
                rf.votedFor=rf.me
                rf.persist()
                rf.sum=1
                args.Term=rf.currentTerm
                args.CandidateId=rf.me
                args.LastLogTerm=rf.Log_Last_Term()
                args.LastLogIndex = rf.Log_Last_Index()
                rf.mu.Unlock()

                for i:= range rf.peers {
                    if i!=rf.me {
                        go func(i int, args *RequestVoteArgs){
                            var reply RequestVoteReply
                            rf.mu.Lock()
                            r_s:=rf.state
                            rf.mu.Unlock()
                            if rf.sendRequestVote(i,args,&reply)  && r_s==1{
                                rf.mu.Lock()
                                if reply.Success && rf.state==1 && args.Term==rf.currentTerm{
                                    rf.sum++
                                    if rf.sum>len(peers)/2 && args.Term==rf.currentTerm{
                                        rf.maxchan<-1
                                    }

                                }else if reply.Term>rf.currentTerm{

                                    rf.state=0
                                    rf.votedFor=-1
                                    rf.currentTerm=reply.Term

                                    rf.persist()
                                }
                                rf.mu.Unlock()
                            }
                        }(i,&args)
                    }
                }
                select{
                case<-rf.heartbeat:
                    rf.mu.Lock()
                    rf.state=0
                    rf.votedFor=-1
                    rf.mu.Unlock()
                case<-rf.maxchan:
                    rf.mu.Lock()

                    rf.state=2
                    //fmt.Println(rf.me," becoming leader for term: ",rf.currentTerm," and log: ",rf.log," and majority: ",rf.sum)
                    for i:=range peers{
                        rf.nextIndex[i]=rf.Log_Last_Index()+1
                        rf.matchIndex[i]=0
                    }

                    //rf.persist()
                    rf.mu.Unlock()
                    // case<-rf.recvchan:
                case<-time.After(time.Duration(rand.Intn(150)+200)*time.Millisecond):
                }
            }else if raft_state==2{
                //for{
                //    rf.mu.Lock()
                //    if rf.kill{
                //        rf.mu.Unlock()
                //        return
                //    }
                //    rf.mu.Unlock()
                //var g bool

                for i:= range rf.peers{
                    var args AppendEntriesArgs
                    rf.mu.Lock()
                    args.Term=rf.currentTerm
                    args.LeaderCommit = rf.commitIndex
                    args.LeaderId=rf.me
                    args.PrevLogIndex = rf.nextIndex[i]-1
                    /*if args.PrevLogIndex<0{
                        fmt.Println("Index is being less than 0: ",args.PrevLogIndex)
                    }else if args.PrevLogIndex>=len(rf.log){
                        fmt.Println("Index is more than raft log: ",len(rf.log)," args.PrevLogIndex: ",args.PrevLogIndex)
                    }
*/
                    args.PrevLogTerm = rf.log[args.PrevLogIndex].TermIndex
                    //}
                    //len_logs:= len(rf.log)
                    args.Logs=make([]Log, len(rf.log[rf.nextIndex[i]:]))
                    //arg_len:=rf.Log_Last_Index()-rf.nextIndex[i]+1
                    //args.Logs=make([]Log, arg_len)
                    copy(args.Logs,rf.log[rf.nextIndex[i]:])
                    rf.persist()

                    raft_s:=rf.state
                    rf.mu.Unlock()
                    if i!=rf.me && raft_s==2 {
                        go func(i int,args *AppendEntriesArgs){
                            var reply AppendEntriesReply

                            if rf.sendAppendEntries(i,args,&reply)  {
                                rf.mu.Lock()
                                if reply.Success && rf.state==2{
                                    if args.Term==rf.currentTerm{
                                        //rf.nextIndex[i]=rf.log[len_logs-1].LogIndex+1
                                        rf.nextIndex[i]=reply.ConflictIndex
                                        rf.matchIndex[i]=rf.nextIndex[i]-1

                                        /*rf.matchIndex[i]=reply.ConflictIndex
                                          rf.nextIndex[i]=rf.matchIndex[i]+1*/

                                    }
                                }else if reply.Term>rf.currentTerm && rf.state==2{
                                    rf.state=0
                                    rf.votedFor=-1
                                    rf.currentTerm=reply.Term
                                    //g=true
                                    rf.persist()
                                }else if reply.Term==rf.currentTerm && rf.state==2{
                                    rf.nextIndex[i]=reply.ConflictIndex
                                }
                                rf.mu.Unlock()
                            }
                        }(i,&args)
                    }
                }
                /*if g{
                  break
                }*/
                N:=0
                rf.mu.Lock()
                count:=rf.commitIndex+1
                last_index_leader := rf.Log_Last_Index()

                for count<=last_index_leader{
                    sum_val:=1
                    for h:= range rf.peers{
                        if h!=rf.me{
                            if count<=rf.matchIndex[h] && rf.log[count].TermIndex==rf.currentTerm{
                                sum_val++
                            }
                        }
                    }
                    if sum_val>len(peers)/2{
                        N=count
                    }
                    count++
                }
                if N>rf.commitIndex && rf.state==2{
                    rf.commitIndex = N
                    //rf.persist()
                }
                rf.mu.Unlock()
                go func(){
                    rf.ApplyMessages()
                }()
                time.Sleep(100*time.Millisecond)
            }
        }
        // }
    }()
    // initialize from state persisted before a crash
    rf.mu.Lock()
    rf.readPersist(persister.ReadRaftState())
    rf.mu.Unlock()
    return rf
}
