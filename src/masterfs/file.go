package main

import (
	"log"
	"bazil.org/fuse"
	"golang.org/x/net/context"
	"encoding/json"
	"os"
)

type File struct{
	Node
	DataNodes map[uint64][]string
	Replicas int
}


func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	
	a.Inode = f.Attributes.Inode
	a.Mode = f.Attributes.Mode
	a.Size = f.Attributes.Size
	a.Blocks = f.Attributes.Blocks
	a.BlockSize = f.Attributes.BlockSize
	log.Println("Requested Attr for File", f.Name, "has data size", f.Attributes.Size, "has blocks", f.Attributes.Blocks)
	go f.SaveMetaFile()
	return nil
}


func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	
	if len(connList) > 0 {
		log.Printf("Read %d bytes from offset %d in file %s",req.Size, req.Offset, f.Name)
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
			b, err := recvBlock((f.DataNodes[i])[0])				// always receiving first replica!
			if err != nil {
				return err
			}
			buff = append(buff, b...)
		}	
		resp.Data = buff[uint64(req.Offset) - start_block*dataBlockSize : limit - start_block*dataBlockSize]
	} else {
		log.Println("No peers connected! Cannot write to file", f.Name)
	}
	return nil
}


func (f *File) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	
	if len(connList) > 0 {
		write_length := uint64(len(req.Data)) 						// data write length
		write_offset := uint64(req.Offset)     						// offset of the write
		limit := write_offset + write_length             			// The final length of the data
		start_block := Offset2Block(write_offset)
		end_block := Offset2Block(limit)
		range_block := RangeOfBlocks(start_block,end_block)  // range of blocks to change or create
		buff := make([]byte, len(req.Data))
		copy(buff[:], req.Data[:])
		numReplicas := f.Replicas
		for _, value:= range range_block {
			if len(buff) > 0{
				BlockCheck(value,&f.DataNodes,write_offset,limit,dataBlockSize*value,&buff,numReplicas) // check if block exists overwrite else create
			}
		}
		f.Attributes.Size = limit
		f.Attributes.Blocks = Blocks(f.Attributes.Size)
		resp.Size = int(write_length)	
		log.Printf("Wrote %d bytes offset by %d to file %s", write_length, write_offset, f.Name)
		
	} else {
		log.Println("No peers connected! Cannot write to file", f.Name)

	}

	return nil

}


func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {

	// File truncation
	if req.Valid.Size() {
		numBlocksB4 := f.Attributes.Blocks
		log.Printf("Truncate size from %d to %d on file %s", f.Attributes.Size, req.Size, f.Name)
		f.Attributes.Size = req.Size
		f.Attributes.Blocks = Blocks(f.Attributes.Size)
		// remove rest of the blocks
		range_block := RangeOfBlocks(f.Attributes.Blocks,numBlocksB4-1)
		if f.Attributes.Blocks < numBlocksB4  {
			for i := len(range_block)-1; i >= 0; i-- {

				for j := 0; j < f.Replicas; j++{
					go deleteBlock(f.DataNodes[range_block[i]][j])
				}
				// f.DataNodes = append(f.DataNodes[:range_block[i]], f.DataNodes[range_block[i]+1:]...)
				delete(f.DataNodes, range_block[i])
			}
		}
	}
	// Set the mode on the node
	if req.Valid.Mode() {
		log.Printf("Setting node %s Mode to %v", f.Name, req.Mode)
		f.Attributes.Mode = req.Mode
	}
	resp.Attr = f.Attributes
	return nil

}

func (f *File) SaveMetaFile() {

	meta := &Meta{Name: f.Name, Attributes: f.Attributes, DataNodes: f.DataNodes, Replicas: f.Replicas}
    j, err := json.Marshal(meta)
    if err != nil {
        log.Println("Error converting backup to json ",err)
        return
    }
	handle, err := os.Create("/mnt/backup/"+"."+f.Name+".meta")
	defer handle.Close()
	if err != nil {
	    log.Println("Error creating backup file ",err)
	    return
	}
	handle.Chmod(0777)
	_, err = handle.WriteString(string(j))
	if err != nil {
	    log.Println("Error saving backup file ",err)
	    return
	}
	handle.Sync()
	log.Println("Saving backup file")


}
