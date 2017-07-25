package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// variables
var dataBlockSize = 512
var blockNum = 0

// structures
type Node struct {
	inode uint64
	name  string
	dataNodes [] string
}
var inode uint64
func NewInode() uint64 {
	inode += 1
	return inode
}

// functions
func split(buf []byte, lim int) [][]byte {
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


func usage() {
	fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
	fmt.Fprintf(os.Stderr, "  %s MOUNTPOINT\n", os.Args[0])
	flag.PrintDefaults()
}

func FUSE(mountpoint string) {	
	c, err := fuse.Mount(mountpoint)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	log.Println("About to serve fs")
	err = fs.Serve(c, &FS{
							&Dir{
								Node: Node{name: "head", inode: NewInode()}, 
								files: &[]*File{
									}, 
								directories: &[]*Dir{},
								}})
	if err != nil {
		log.Fatal(err)
	}
	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}




