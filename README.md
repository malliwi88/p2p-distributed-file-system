# p2p-distributed-file-system
Peer to Peer distributed file storage system implemented in golang

## How To Run:
Copy the repository to your $GOPATH
- "go install masterfs"
- "go install slavefs"  

Both the executables will be created in the 'bin' folder of your repository.

## Description:
The master server runs on the default address: localhost:8000 and uses FUSE as its filesystem.
To run the master you have to pass the mountpoint to the command line. The slaves automatically
conncect to the master when executed. All the data is stored remotely in the slaves. FUSE 
will work as if all the files were stored locally.

#### Functions implemented:
- Division of file into blocks
- Distributed writeAll function
- Distributed readAll function

#### Functions to implement (For now):
- Read a particular file block stored at the slaves
- Write a particular block and update the corresponding blocks.



