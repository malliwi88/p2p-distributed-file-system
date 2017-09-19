package main

import (
	"log"
	"bazil.org/fuse"
	"golang.org/x/net/context"
)

// File implements both Node and Handle for the hello file.
type File struct{
	Node
	dataNodes [] string
}


func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	
	a.Inode = f.attributes.Inode
	a.Mode = f.attributes.Mode
	a.Size = f.attributes.Size
	a.Blocks = f.attributes.Blocks
	a.BlockSize = f.attributes.BlockSize
	log.Println("Requested Attr for File", f.name, "has data size", f.attributes.Size, "has blocks", f.attributes.Blocks)
	return nil
}


func (f *File) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	
	log.Printf("Read %d bytes from offset %d in file %s",req.Size, req.Offset, f.name)
	limit := uint64(req.Offset) + uint64(req.Size)
	if limit > f.attributes.Size {
		limit = f.attributes.Size
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
		b, err := recvBlock(f.dataNodes[i])
		if err != nil {
			return err
		}
		buff = append(buff, b...)
	}	
	resp.Data = buff[uint64(req.Offset) - start_block*dataBlockSize : limit - start_block*dataBlockSize]
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

		for _, value:= range range_block {
			if len(buff) > 0{
				BlockCheck(value,&f.dataNodes,write_offset,limit,dataBlockSize*value,&buff) // check if block exists overwrite else create
			}
		}

		f.attributes.Size = limit
		f.attributes.Blocks = Blocks(f.attributes.Size)
		resp.Size = int(write_length)	
		log.Printf("Wrote %d bytes offset by %d to file %s", write_length, write_offset, f.name)
		
	} else {
		log.Println("No peers connected! Cannot write to file", f.name)

	}

	return nil

}


func (f *File) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {

	// File truncation
	if req.Valid.Size() {

		numBlocksB4 := f.attributes.Blocks
		log.Printf("Truncate size from %d to %d on file %s", f.attributes.Size, req.Size, f.name)
		f.attributes.Size = req.Size
		f.attributes.Blocks = Blocks(f.attributes.Size)
		// remove rest of the blocks
		range_block := RangeOfBlocks(f.attributes.Blocks,numBlocksB4-1)
		if f.attributes.Blocks < numBlocksB4  {
			for i := len(range_block)-1; i >= 0; i-- {
				deleteBlock(f.dataNodes[range_block[i]])
				f.dataNodes = append(f.dataNodes[:range_block[i]], f.dataNodes[range_block[i]+1:]...)
			}


		}

	}
	// Set the mode on the node
	if req.Valid.Mode() {

		log.Printf("Setting node %s Mode to %v", f.name, req.Mode)
		f.attributes.Mode = req.Mode
	}

	resp.Attr = f.attributes
	return nil

}
