
# p2p-distributed-file-system
Peer to Peer distributed file storage system implemented in GOLANG

## Initial Setup:
Copy the repository to your $GOPATH and run the following commands:
- "go install chord"
- "go install relay"
- "go install tracker"

Both the executables will be created in the 'bin' folder of your repository.
In order to avoid temporary files made by vim while editing any file, add the following lines in your .vimrc file (vim configuration file on unix based OS) present in you $HOME directory:
set nobackup
set nowritebackup
set noswapfile

## How To Run:
To run the peer launch app.py which will open up a GUI. The user will have to enter username, password, mountpoint and the tracker's IP address and start the FUSE filesystem by pressing the 'Run' button. The launched terminal will have the basic commands that the user can interact with. The commands include:
- help:		display help
- dump:		display information about the current node
- quit:		exit the filesystem gracefully
After quitting FUSE the user needs to click on 'Exit' button of the GUI to exit the entire program.

## Description:
See google docs for detailed explanation: https://docs.google.com/document/d/17GVUKNmyi63_IA0IxgAtiRKA8j5ZGQLpPwCeyxHzFdA/edit?usp=sharing

## Functions implemented:
- FUSE Filesystem
- Chord DHT
- Fault Tolerance
- Load Balancing
- NAT Traversal
- Relay Server
- Tracker Server
- GUI
