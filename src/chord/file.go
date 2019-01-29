package main

import (
	"log"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"encoding/json"
	"os"
)

type File struct{
	Node
	DataNodes map[uint64][]*OneBlockInfo
	Replicas int
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = f.Attributes.Inode
	a.Mode = f.Attributes.Mode
	a.Size = f.Attributes.Size
	a.Blocks = f.Attributes.Blocks
	a.BlockSize = f.Attributes.BlockSize
	// log.Println("Requested Attr for File", f.Name, "has data size", f.Attributes.Size, "has blocks", f.Attributes.Blocks)
	return nil
}


func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	// log.Printf("Read %d bytes from offset %d from file %s",req.Size, req.Offset, f.Name)
	limit := uint64(req.Offset) + uint64(req.Size)
	if limit > f.Attributes.Size {
		limit = f.Attributes.Size
	}
	start_block := Offset2Block(uint64(req.Offset))
	end_block := Offset2Block(uint64(limit))
	
	if limit == uint64(req.Offset){
		resp.Data = []byte{}
		return nil
	
	} else if limit % dataBlockSize == uint64(0) && limit != uint64(0) {
		end_block = end_block - uint64(1)	
	}
	range_block := end_block - start_block
	buff := make([]byte, 0, dataBlockSize*range_block)
	for i := start_block; i <= end_block; i++ {
		for len(f.DataNodes[i]) != 0 {
			b, err := recvBlock((f.DataNodes[i])[0].Name)				// always receiving first replica!
			if err != nil {
				log.Println("Peer disconnected!")
				f.DataNodes[i] = f.DataNodes[i][1:]	// delete the disconnected
			} else {
				buff = append(buff, b...)
				break
			}
		}
	}	
	resp.Data = buff[uint64(req.Offset) - start_block*dataBlockSize : limit - start_block*dataBlockSize]
	return nil
}


func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	write_length := uint64(len(req.Data)) 						// data write length
	write_offset := uint64(req.Offset)							// offset of the write
	limit := write_offset + write_length             			// The final length of the data
	start_block := Offset2Block(write_offset)
	end_block := Offset2Block(limit)
	blk_iter := start_block
	buff := make([]byte, len(req.Data))
	copy(buff[:], req.Data[:])
	numReplicas := f.Replicas
	for i := uint64(0); i < (end_block-start_block); i++ {
		BlockCheck(i+Blocks(f.Attributes.Size), &f.DataNodes, buff[i*dataBlockSize:(i+1)*dataBlockSize], &numReplicas)
		blk_iter++
	}
	if blk_iter == end_block && write_length % dataBlockSize != 0 {
		BlockCheck(blk_iter, &f.DataNodes, buff[blk_iter*dataBlockSize-write_offset:len(buff)], &numReplicas)
	}
	f.Replicas = numReplicas
	f.Attributes.Size = limit
	f.Attributes.Blocks = Blocks(f.Attributes.Size)
	resp.Size = int(write_length)	
	// log.Printf("Wrote %d bytes offset by %d to file %s", write_length, write_offset, f.Name)
		
	return nil

}


func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	// File truncation
	if req.Valid.Size() {
		numBlocksB4 := f.Attributes.Blocks
		// log.Printf("Truncate size from %d to %d on file %s", f.Attributes.Size, req.Size, f.Name)
		f.Attributes.Size = req.Size
		f.Attributes.Blocks = Blocks(f.Attributes.Size)
		// remove rest of the blocks
		if f.Attributes.Blocks < numBlocksB4  {
			for i := numBlocksB4-1; i >= f.Attributes.Blocks; i-- {
				for j := 0; j < f.Replicas; j++{
					deleteBlock(f.DataNodes[i][0].Name)
				}
				f.DataNodes[i] = f.DataNodes[i][:1]
				f.DataNodes[i][0].Used = false
				if i == f.Attributes.Blocks {
					break
				}
			}
		}
	}
	// Set the mode on the node
	if req.Valid.Mode() {
		log.Printf("Setting node %s Mode to %v from %v", f.Name, req.Mode, f.Attributes.Mode)
		f.Attributes.Mode = req.Mode
	}
	resp.Attr = f.Attributes
	return nil

}

func (f *File) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	// log.Println("fsync on file")
	return nil
}

func (f *File) SaveMetaFile() {

	meta := &Meta{Name: f.Name, Attributes: f.Attributes, DataNodes: f.DataNodes, Replicas: f.Replicas}
    j, err := json.Marshal(meta)
    if err != nil {
        log.Println("Error converting backup to json ",err)
        return
    }
	handle, err := os.Create(Root.ID+"_backup/"+f.Name+".meta")
	
	defer handle.Close()
	checkError(err)
	handle.Chmod(0777)
	_, err = handle.WriteString(string(j))
	checkError(err)
	handle.Sync()
	log.Println("Saving backup file...")
}

func (f *File) Rename(ctx context.Context, req *fuse.RenameRequest, newDir fs.Node) error{

	log.Println("RENAME FILE")
	log.Println(req.OldName,req.NewName,newDir)
	return nil

}