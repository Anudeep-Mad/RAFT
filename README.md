# Raft
 Distributed Consenus Algorithm
 
 Raft is a distributed consensus algorithm. It is widely used to achieve consensus in Distrubted servers and applications. 
 It was developed as an alternative to Paxos (https://lamport.azurewebsites.net/pubs/paxos-simple.pdf) 
 
 RAFT Paper : https://web.stanford.edu/~ouster/cgi-bin/papers/raft-atc14.
 
 This project is implemented in Go. It uses goroutines to implement parallel processing.
 Goroutines are used to implement Asynchronous and synchronous processing and thread execution.
 
 Due to infrastructure limitations, this project was implemented on a single machine, and distributed environment simulated using RPC's.
