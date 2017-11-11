# p2p-distributed-file-system
Peer to Peer distributed file storage system implemented in golang

## How To Run:
Copy the repository to your $GOPATH
- "go install masterfs"
- "go install slavefs"  

Both the executables will be created in the 'bin' folder of your repository.

## Description:
All peers store their files on each other. They all run their separate FUSE filsystems. To get IPs of peers
in the system they contact a central server running on port 8080.

#### Functions implemented:
- Division of file into blocks
- Distributed writeAll function
- Distributed readAll function
- Write by offset
- Read by offset
- Seperate working peers
- Central Server to share IPs
- Fault tolerance
- Load balancing

#### Issues to solve (For now):
- nano and vim not working

