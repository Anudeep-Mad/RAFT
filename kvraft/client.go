package raftkv

import (

	"labrpc"
	"time"
)
import "crypto/rand"
import "math/big"
//import "fmt"
//import "time"
import "sync"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu          sync.Mutex
	flag bool
	clerkId     int64
	exec_seq_id int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	time.Sleep(500*time.Millisecond)
	ck := new(Clerk)
	ck.servers = servers
	ck.flag=false
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.exec_seq_id = 0
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
	var args GetArgs
	var response string
	args.Key = key
	args.ClerkId = ck.clerkId
	ck.mu.Lock()
	ck.exec_seq_id++

	args.ClerkSeq = ck.exec_seq_id
	ck.mu.Unlock()
	var wg sync.WaitGroup
	//fmt.Println("Sending Get request with args ",args,"to server \n")

	for{
		flag:=false
		for i := range ck.servers {
			//defer wg.Done()
			wg.Add(1)
			go func(i int, args *GetArgs) {
				//fmt.Println("Test1")
				var reply GetReply
				ck.mu.Lock()

				ok:=ck.servers[i].Call("KVServer.Get", args, &reply)
				ck.mu.Unlock()
				//fmt.Println(" ",reply)
				if ok{
					if !reply.WrongLeader && reply.Err==OK{
						ck.mu.Lock()
						response = reply.Value
						flag=true
						ck.mu.Unlock()
						//fmt.Println("Client ",ck.clerkId," recieved value ",response," for key ",args.Key," value ",reply.Value," from server ",i)
					}
				}
				wg.Done()
			}(i, &args)

		}
		wg.Wait()
		if flag{
			return response
		}
	}


	//time.Sleep(2000*time.Millisecond)


	// You will have to modify this function.

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
	var wg sync.WaitGroup
	var args PutAppendArgs
	ck.mu.Lock()
	ck.exec_seq_id++
	args.ClerkId = ck.clerkId
	args.ClerkSeq=ck.exec_seq_id
	ck.mu.Unlock()
	args.Key = key
	args.Value = value
	args.Op = op
	//fmt.Println("Sending ",args.Op," request with arguments ",args," to server with -cleintId: ",args.ClerkId," clientSeq: ",args.ClerkSeq)
	for{

		ck.mu.Lock()
		ck.flag= false
		ck.mu.Unlock()
		for i := range ck.servers {
			wg.Add(1)
			go func(i int, args *PutAppendArgs) {
				var reply PutAppendReply

				//ck.mu.Lock()
				ok:= ck.servers[i].Call("KVServer.PutAppend", args, &reply)
				//ck.mu.Unlock()
				if ok{
					if !reply.WrongLeader && reply.Err==OK{
						ck.mu.Lock()
						ck.flag=true
						ck.mu.Unlock()
						//fmt.Println("Converted flag to ",flag, " for clientId ",args.ClerkId," with seq: ",args.ClerkSeq)
					}
				}
				//fmt.Println(ok)
				wg.Done()
			}(i, &args)

		}
		wg.Wait()
		ck.mu.Lock()
		if ck.flag{
			ck.mu.Unlock()
			break
		}
		ck.mu.Unlock()
		//fmt.Println(" Request failed to return from client.....",args.ClerkId," Seq: ",args.ClerkSeq," Key: ",args.Key," Value ",args.Value)
	}
	//time.Sleep(2000*time.Millisecond)

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
