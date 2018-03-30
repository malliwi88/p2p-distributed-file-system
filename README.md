
# p2p-distributed-file-system
Peer to Peer distributed file storage system implemented in GOLANG

## Initial Setup:
Copy the repository to your $GOPATH and run the following commands:
- "go install chord"

Both the executables will be created in the 'bin' folder of your repository.
In order to avoid temporary files made by vim while editing any file, add the following lines in your .vimrc file (vim configuration file on unix based OS) present in you $HOME directory:
set nobackup
set nowritebackup
set noswapfile

## How To Run:
To run the peer: "./peer -mount=/path/to/mountpoint/ -port=some_random_port" with admin rights. Remember not to run different peers on same port or same mountpoint in case testing on the single machine.

## Description:
The peer module runs its own FUSE filesystem. When writing anything to a file, the data gets divided into block 512 bytes each. These blocks are sent to other peers by initiating a TCP connection with them. The peer joins in the network by giving the address of any random peer belonging to the chord ring. Each peer maintains its succesor list and a finger table.

#### Functions implemented:
- Division of file into blocks
- Distributed writeAll function
- Distributed readAll function
- Write by offset
- Read by offset
- Seperate working peers
- Fault tolerance
- Load balancing



## UNDER DEVELOPMENT ...