package main

import (
	"log"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"os"
	"os/signal"
	"syscall"
	"time"
	"io/ioutil"
	"encoding/json"
)

// variables
var dataBlockSize uint64 = 512
var blockIdentifier uint64 = 0
var inode uint64 = 0

// structures
type Node struct {
	Name string
	Attributes fuse.Attr
}

type Meta struct {
	Name string
	Attributes fuse.Attr
	DataNodes map[uint64][]*OneBlockInfo
	Replicas int
}

type OneBlockInfo struct {
	Name uint64
	PeerInfo *Peer
	Used bool
}


// functions
func (n *Node) InitNode() {
	
	t := time.Now()
	n.Attributes.Inode = NewInode()      
    n.Attributes.Size = 0      			
    n.Attributes.Blocks = 0      		
	n.Attributes.Atime = t
	n.Attributes.Mtime = t
	n.Attributes.Ctime = t
	n.Attributes.Crtime = t
	n.Attributes.Mode = 0644 
	n.Attributes.Nlink = 0
	n.Attributes.Uid = 0
	n.Attributes.Gid = 0
	n.Attributes.Rdev = 0
	n.Attributes.BlockSize = uint32(dataBlockSize) // block size

}

func checkError(e error){
	
	if e != nil {
		log.Println(e)
	}
}

func checkFatal(e error){
	
	if e != nil {
		log.Fatalln(e)
	}	
}

func Split(buf []byte, lim int) [][]byte {
	
	var chunk []byte
	chunks := make([][]byte, 0, len(buf)/lim+1)
	for len(buf) >= lim {
		chunk, buf = buf[:lim], buf[lim:]
		chunks = append(chunks, chunk)
	}
	if len(buf) > 0 {
		chunks = append(chunks, buf[:len(buf)])
	}
	return chunks
}

func NewInode() uint64 {
	
	inode += 1
	return inode
}


func Blocks(value uint64) uint64 { // Blocks returns the number of 512 byte blocks required
	
	if value == 0 {
		return 0
	}
	blocks := value / dataBlockSize
	if value%dataBlockSize > 0 {
		return blocks + 1
	}
	return blocks
}

func Offset2Block (value uint64) uint64 {
	
	return (value / dataBlockSize)
}

func BlockCheck(offsetBlock uint64, dataNodes *map[uint64][]*OneBlockInfo, buffer []byte, numReplicas *int) {
	
	if *numReplicas > len(connList) {
		*numReplicas = len(connList)
	}
	if offsetBlock < uint64(len(*dataNodes)) {
		if (*dataNodes)[offsetBlock][0].Used {
			// log.Println("Exists and Used")
			// log.Println(offsetBlock)
			for j := 0; j < len((*dataNodes)[offsetBlock]); j++ {
				sendBlock((*dataNodes)[offsetBlock][j].PeerInfo, buffer, (*dataNodes)[offsetBlock][j].Name)
			}
		} else {
			// log.Println("Exists but not Used")
			// log.Println(offsetBlock)
			sortPeers1("data", connList)
			name := (*dataNodes)[offsetBlock][0].Name
			(*dataNodes)[offsetBlock] = make([]*OneBlockInfo, 0, *numReplicas)
			for j := 0; j < *numReplicas; j++ {
				singleBlock := &OneBlockInfo{name, connList[j], true}
				(*dataNodes)[offsetBlock] = append((*dataNodes)[offsetBlock], singleBlock)
				sendBlock(connList[j], buffer, (*singleBlock).Name)
			}
		}
	} else {
		// log.Println("Doesn't exist")
		// 	log.Println(offsetBlock)
		sortPeers1("data", connList)
		for j := 0; j < *numReplicas; j++ {

			singleBlock := &OneBlockInfo{blockIdentifier, connList[j], true}
			if _, ok := (*dataNodes)[offsetBlock]; !ok {
				(*dataNodes)[offsetBlock] = make([]*OneBlockInfo, 0, *numReplicas)
			}
			(*dataNodes)[offsetBlock] = append((*dataNodes)[offsetBlock], singleBlock)
			
		// log.Println("Call for:", offsetBlock)

			// GO CALL CREATES PROBLEM HERE
			sendBlock(connList[j], buffer, (*singleBlock).Name)
		}
		blockIdentifier++
	}
}


func InterruptHandler(mountpoint string, FileSystem *FS) {

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	log.Println("\nShutting down fuse server!\n")
	err := fuse.Unmount(mountpoint)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}



func FUSE(mountpoint string) {

	// load meta data
	backupDir := "/mnt/backup/"
	files, err := ioutil.ReadDir(backupDir)
	if err != nil {
		log.Fatal(err)
	}
	meta := Meta{}
	fileArray := []*File{}
	for _, file := range files {
		content, err := ioutil.ReadFile(backupDir + file.Name())
		if err != nil {
			log.Fatal(err)
		}
		meta = Meta{}
    	json.Unmarshal(content, &meta)
    	filemeta := File{}
    	filemeta.Node.Name = meta.Name
    	filemeta.DataNodes = meta.DataNodes
    	filemeta.Node.Attributes = meta.Attributes
    	filemeta.Replicas = meta.Replicas
		fileArray = append(fileArray,&filemeta)
	}
	////////////////////////////////////////////////////
	
	fuse.Unmount(mountpoint)
	c, err := fuse.Mount(mountpoint)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	FileSystem := new(FS)
	FileSystem.root = new(Dir)
	FileSystem.root.InitNode()
	FileSystem.root.files = &fileArray
	go InterruptHandler(mountpoint, FileSystem)
	
	err = fs.Serve(c, FileSystem)
	if err != nil {
		log.Fatal(err)

	}
	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}


}


