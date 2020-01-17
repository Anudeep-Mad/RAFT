package raftkv

import (
	"labgob"
	"labrpc"
	"raft"

	"sync"
	"time"
)
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq int
	ClerkId int64
	Key string
	Value string
	Method string
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft

	executed map[int64]int
	//operations map[int]Op
	client_req map[int64]chan Op
	applyCh chan raft.ApplyMsg
	store map[string]string



	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	//fmt.Println(kv.me," Get request recieved ",args)
	//kv.r_seq++
	var op Op
	op.Seq=args.ClerkSeq
	op.ClerkId= args.ClerkId
	op.Key = args.Key
	op.Method="Get"
	//kv.operations[kv.r_seq]=op
	kv.mu.Unlock()
	_,is := kv.rf.GetState()
	reply.WrongLeader=!is
	if !is{
		return
	}

	kv.mu.Lock()
	// kv.ApplyStart()
	_,flag:= kv.client_req[args.ClerkId]
	if !flag{
		TempChannel := make(chan Op,500)
		kv.client_req[args.ClerkId]= TempChannel
	}
	kv.rf.Start(op)
	o:= kv.client_req[args.ClerkId]
	kv.mu.Unlock()
	select{
	case data1:= <-o:
		if op.Seq==data1.Seq{
			reply.Value=data1.Value
			//fmt.Println(kv.me," Get received values ",reply.Value," for key ",data1.Key," kv store is ",kv.store)
			//fmt.Println(t.Second()-time.Now().Second())
			reply.Err=OK
			return
		}else{
			reply.Err=ErrNoKey
		}
	case <-time.After(2*time.Second):
		reply.Err="TimeOut"

	}


}
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	//kv.r_seq++
	//key_comm:= strconv.Itoa(int(args.ClerkId))+""+strconv.Itoa(args.ClerkSeq)

	var op Op
	op.Seq=args.ClerkSeq
	op.ClerkId = args.ClerkId
	op.Key = args.Key
	op.Value=args.Value
	op.Method=args.Op
	//kv.operations[kv.r_seq]=op


	_,is := kv.rf.GetState()
	reply.WrongLeader=!is
	if !is{
		kv.mu.Unlock()
		return
	}

	// kv.ApplyStart()
	_,flag:= kv.client_req[args.ClerkId]
	if !flag{
		kv.client_req[args.ClerkId]= make(chan Op,500)
	}
	kv.rf.Start(op)
	client_channel:= kv.client_req[args.ClerkId]
	kv.mu.Unlock()

	select{
	case data1:= <-client_channel:
		if op.Seq==data1.Seq{
			//fmt.Println(data1.Method," received values ",data1," with sequence ",data1.Seq)
			reply.Err=OK
			return
		}else{
			reply.Err=ErrNoKey
		}
	case <-time.After(2*time.Second):
		reply.Err="TimeOut"

	}
	//fmt.Println(args.Op," request recieved from ",args.ClerkId," with args ",op)




}

/*func (kv *KVServer) ApplyStart(){
	kv.mu.Lock()

	for kv.applied<kv.r_seq{
		kv.applied++
		fmt.Println("Applying ",kv.operations[kv.applied])
		kv.rf.Start(kv.operations[kv.applied])
	}
	kv.mu.Unlock()
}*/
func (kv *KVServer) ReadStart(){
	for {
		data := <-kv.applyCh
		var tt int
		//fmt.Println(kv.me," Data recieved ",data," kv.store: ",kv.store)
		kv.mu.Lock()
		if data.Command!=0{
			tt= kv.executed[data.Command.(Op).ClerkId]
		}

		kv.mu.Unlock()
		if data.Command != 0 && data.Command.(Op).Seq >  tt{
			kv.mu.Lock()
			kv.executed[data.Command.(Op).ClerkId] = data.Command.(Op).Seq
			kv.mu.Unlock()
			if data.Command.(Op).Method == "Get" {
				/*kv.mu.Unlock()
				_,fl:=kv.client_req[data.Command.(Op).ClerkId]
				if fl{
					kv.client_req[data.Command.(Op).ClerkId]<-data.Command.(Op)
				}
				kv.mu.Lock()*/

				temp := data.Command.(Op)
				kv.mu.Lock()
				temp.Value = kv.store[temp.Key]
				_, fl := kv.client_req[data.Command.(Op).ClerkId]
				chann:= kv.client_req[data.Command.(Op).ClerkId]
				kv.mu.Unlock()
				if fl {
					go func(){
						chann <- temp
					}()
				}


			} else {
				kv.mu.Lock()
				if data.Command.(Op).Method == "Append" {

					_, f := kv.store[data.Command.(Op).Key]
					if f {
						kv.store[data.Command.(Op).Key] = kv.store[data.Command.(Op).Key] + data.Command.(Op).Value
						//fmt.Println(kv.me," Map after append ",kv.store)
					} else {
						kv.store[data.Command.(Op).Key] = data.Command.(Op).Value
					}

				} else {
					kv.store[data.Command.(Op).Key] = data.Command.(Op).Value
				}
				//fmt.Println(kv.me, " appending ", data.Command.(Op).Value, " to key ", data.Command.(Op).Key, " kv store ", kv.store)

				_, fl := kv.client_req[data.Command.(Op).ClerkId]
				uuh:= kv.client_req[data.Command.(Op).ClerkId]
				kv.mu.Unlock()
				if fl {
					go func(){
						uuh <- data.Command.(Op)
					}()
				}

			}

		} else if data.Command != 0 {
			//fmt.Println("REQUEST>>>>>>>>>>>>>>>>>........................DISCARDED")
			temp := data.Command.(Op)
			if data.Command.(Op).Method == "Get" {
				//if data.Command.(Op).Key==""{
				//kv.mu.Unlock()
				kv.mu.Lock()
				temp.Value = kv.store[temp.Key]
				kv.mu.Unlock()

			}

			kv.mu.Lock()
			_, fl := kv.client_req[data.Command.(Op).ClerkId]

			if fl {
				tempChannel := kv.client_req[data.Command.(Op).ClerkId]
				kv.mu.Unlock()
				go func(){
					//kv.mu.Unlock()

					tempChannel<- temp

				}()
			}else{kv.mu.Unlock()}


		}


	}
}


//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
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
	kv := new(KVServer)
	kv.me = me

	kv.maxraftstate = maxraftstate
	kv.store = make(map[string]string)
	kv.executed = make(map[int64]int)
	kv.client_req = make(map[int64]chan Op)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	go kv.ReadStart()
	// You may need initialization code here.
	return kv
}


