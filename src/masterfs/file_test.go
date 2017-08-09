package main

import (
	"testing"
	"net"
	"bazil.org/fuse"
	"golang.org/x/net/context"
	"fmt"
	"bytes"
)

func TestInit(t *testing.T) {
	
	master_port := "8000"
	interface_addr, _ := net.InterfaceAddrs()
	local_ip := interface_addr[0].String()
	master := Peer{Ip: local_ip, Port: master_port}
	fmt.Println("Master details: ", master)
	go listen(master)
	for {

		if len(connList) != 0 {
			break
		}

	}
}

func TestWriteRead(t *testing.T) {
	
	// Initialise
	f := new(File)
	f.InitNode()
	size := 4170
	data := []byte(RandStringBytes(size))
	ctx := context.TODO()

	// Write
	req := &fuse.WriteRequest{
		Offset: 0,
		Data:   data,
	}
	resp := &fuse.WriteResponse{}
	err := f.Write(ctx, req, resp)
	if err != nil {
       t.Errorf("Error occurred: %s", err)
	}
	if resp.Size != size {
       t.Errorf("Size was incorrect, got: %d, want: %d.", resp.Size, size)
    }

	//Read
 	rreq := &fuse.ReadRequest{
		Offset: 0, Size: size,
	}
	rresp := &fuse.ReadResponse{}
	err = f.Read(ctx, rreq, rresp)
	if err != nil {
       t.Errorf("Error occurred: %s", err)
	}
	if !bytes.Equal(rresp.Data, data) {
		t.Errorf("Data not equal")
	}
}	





// write by offset
// req = &fuse.WriteRequest{
// 	Offset: 510,
// 	Data:   []byte("ran across the mat until he was very tired"),
// }
// resp = &fuse.WriteResponse{}
// err = f.Write(ctx, req, resp)