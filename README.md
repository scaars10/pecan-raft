# Pecan-Raft
## Implementation of Raft Consensus Algorithm in Java
Raft is a consensus algorithm that is designed to be easy to understand.
For complete explanation of Raft algorithm refer to [Raft Paper](https://raft.github.io/raft.pdf).  
This is my attempt to implement a fault tolerant Key-Value store using Raft Consensus Algorithm.  
gRpc has been used to implement RPCs used in Raft and MongoDb to persist non volatile state of a node.  
