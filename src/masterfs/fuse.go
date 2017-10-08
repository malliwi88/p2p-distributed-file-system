package main

import (
	"log"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"strconv"
	"os"
	"os/signal"
	"syscall"
	"time"
	"io/ioutil"
	"encoding/json"
)

// variables
var dataBlockSize uint64 = 512
var blockNum int = 0
var inode uint64 = 0

// structures
type Node struct {
	Name  string
	Attributes fuse.Attr
}

type Meta struct {
    	Name   string
    	Attributes fuse.Attr
    	DataNodes []string
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

func RangeOfBlocks (min, max uint64) []uint64{
	
	a := make([]uint64, max-min+uint64(1))
    for i := range a {
        a[i] = min + uint64(i)
    }
    return a
}

func BlockCheck(offsetBlock uint64, dataNodes *[]string, startWrite uint64, endWrite uint64, blockStart uint64, buffer *[]byte) {
	
	var startData, endData, startBuff, endBuff uint64
	if offsetBlock < uint64(len(*dataNodes)) {
		dataBlock, err := recvBlock((*dataNodes)[offsetBlock])

		checkError(err)
		if endWrite > (blockStart+dataBlockSize) {
			if startWrite >= blockStart && startWrite < (blockStart+dataBlockSize) { // 1st block
				// datablock[0:starWrite] = do nothing
				startData = Normalize(blockStart,blockStart+dataBlockSize,0,dataBlockSize,startWrite)
				endData = Normalize(blockStart,blockStart+dataBlockSize,0,dataBlockSize,blockStart+dataBlockSize)
				startBuff = 0
				endBuff = ((blockStart+dataBlockSize)-startWrite)
				copy(dataBlock[startData:endData] , (*buffer)[startBuff:endBuff])
				*buffer = append((*buffer)[:0], (*buffer)[((blockStart+dataBlockSize)-startWrite):]...)
				sendBlock((*dataNodes)[offsetBlock],dataBlock)
			} else {
				startData = Normalize(blockStart,blockStart+dataBlockSize,0,dataBlockSize,blockStart)
				endData = Normalize(blockStart,blockStart+dataBlockSize,0,dataBlockSize,blockStart+dataBlockSize)
				startBuff = 0
				endBuff = dataBlockSize
				copy(dataBlock[startData:endData] , (*buffer)[startBuff:endBuff])
				*buffer = append((*buffer)[:0], (*buffer)[dataBlockSize:]...)
				sendBlock((*dataNodes)[offsetBlock],dataBlock)
			}		
		} else {

			
			if endWrite-blockStart > uint64(len(dataBlock)) {

				// extend
				t := make([]byte, endWrite-blockStart, endWrite-blockStart)
				copy(t, dataBlock)
				dataBlock = t
				log.Println(len(dataBlock))

			}

			if startWrite >= blockStart && startWrite < (blockStart+dataBlockSize) { // 1st block
				// datablock[blockStart:starWrite] = do nothing
				startData = Normalize(blockStart,blockStart+dataBlockSize,0,dataBlockSize,startWrite)
				endData = Normalize(blockStart,blockStart+dataBlockSize,0,dataBlockSize,endWrite)
				copy(dataBlock[startData:endData] , (*buffer)[:])
				*buffer = (*buffer)[:0]
				sendBlock((*dataNodes)[offsetBlock],dataBlock)
			} else {
				startData = Normalize(blockStart,blockStart+dataBlockSize,0,dataBlockSize,blockStart)
				endData = Normalize(blockStart,blockStart+dataBlockSize,0,dataBlockSize,endWrite)
				copy(dataBlock[startData:endData] , (*buffer)[:])
				*buffer = (*buffer)[:0]
				sendBlock((*dataNodes)[offsetBlock],dataBlock)
			}
		}
		
	} else {
		chunks := Split(*buffer,int(dataBlockSize))
		peerNum := 0
		if len(connList) > 0 {
			for _, c := range chunks {
				*dataNodes = append(*dataNodes,connList[peerNum].RemoteAddr().String() + "/" + strconv.Itoa(blockNum))
				sendBlock(connList[peerNum].RemoteAddr().String() + "/" + strconv.Itoa(blockNum),c)
				blockNum += 1
				if (peerNum+1) == len(connList){
					peerNum = 0
				} else {
					peerNum += 1
				}
			}
			*buffer = (*buffer)[:0]
		}
	}
}

func Normalize(OldMin,OldMax uint64,NewMin,NewMax uint64,OldValue uint64) uint64{
	
	OldRange := (OldMax - OldMin)  
	NewRange := (NewMax - NewMin)  
	NewValue := (((OldValue - OldMin) * NewRange) / OldRange) + NewMin
	return NewValue	
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

