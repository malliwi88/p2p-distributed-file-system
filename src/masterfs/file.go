package main

import (
	"log"
	"bazil.org/fuse"
	"golang.org/x/net/context"
	"strconv"
)

// File implements both Node and Handle for the hello file.
type File struct{
	Node
	size int
}


func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = f.inode
	a.Mode = 0777
	a.Size = uint64(f.size)
	return nil
}

func (f *File) ReadAll(ctx context.Context) ([]byte, error) {
	log.Println("Reading all of file", f.name)
	var buff []byte
	for _, p := range f.dataNodes {
		b := recvBlock(p)
		buff = append(buff, b...)
		log.Println(len(buff))
	}
	return buff, nil
}

func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	log.Println("Trying to write to ", f.name, "offset", req.Offset, "dataSize:", len(req.Data))
	if len(req.Data) > 0 {
		f.size = len(req.Data)
		resp.Size = len(req.Data)
		chunks := split(req.Data,dataBlockSize)
		peerNum := 0
		if len(connList) > 0 {
			for _, c := range chunks {
				f.dataNodes = append(f.dataNodes,connList[peerNum].RemoteAddr().String() + "/" + strconv.Itoa(blockNum))
				sendBlock(connList[peerNum],blockNum,c)
				blockNum += 1
				if (peerNum+1) == len(connList){
					peerNum = 0
				} else {
					peerNum += 1
				}
			}
			log.Println("Wrote to file", f.name)

		} else {
			log.Println("No peers connected! Cannot write to file", f.name)
		}
	}
	return nil
}



