
// WORKS FOR ONE FILE ONLY

package main

import (
	"net"
	"fmt"
	"os"
	"log"
	"strings"
	"strconv"
	"flag"
)


// global variables
var connList []net.Conn

// structures
type Peer struct {
	Ip string
	Port string
}

// functions
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


func selectMountpoint() string {
	flag.Parse()
	if flag.NArg() != 1 {
		log.Printf("Usage of %s:\n", os.Args[0])
		log.Printf("  %s MOUNTPOINT\n", os.Args[0])
		flag.PrintDefaults()

		os.Exit(2)
	}
	mountpoint := flag.Arg(0)
	return mountpoint
}

func main() {
	mountpoint := selectMountpoint()
	master_port := "8000"
	interface_addr, _ := net.InterfaceAddrs()
	local_ip := interface_addr[0].String()
	master := Peer{Ip: local_ip, Port: master_port}
	fmt.Println("Master details: ", master)
	go listen(master)
	FUSE(mountpoint)
}

func listen(master Peer) {
	listen, err := net.Listen("tcp", ":" + master.Port)
	defer listen.Close()
	if err != nil {
		log.Fatalf("Socket listen port %s failed,%s", master.Port, err)
		os.Exit(1)
	}
	log.Printf("Begin listen port: %s", master.Port)
	for {
			conn, err := listen.Accept()
			if err != nil {
				log.Fatalln(err)
				continue
			}	
			handler(conn)
		}
}


func handler(conn net.Conn) {	
	buff := make([]byte, 1024)
	n, err := conn.Read(buff)
	checkError(err)
	s_addr := strings.Split(conn.RemoteAddr().String(),":")
	slave := Peer{Ip: s_addr[0], Port: s_addr[1]}
	log.Printf("Got request: " + string(buff[:n]) + " from : %s",slave)
	connList = append(connList, conn)
	
}


func sendBlock(conn net.Conn, blockNum int, data []byte) {
	conn.Write([]byte("send"))
	// got ACK
	buff := make([]byte, 1024)	
	_, err := conn.Read(buff)		
	checkError(err)
	conn.Write([]byte(  strconv.Itoa(blockNum)  )  )					
	// got name
	buff = make([]byte, 1024)	
	_, err = conn.Read(buff)		
	checkError(err)
	// copy data block to connection
	conn.Write(data)		

}

func recvBlock(addr string) []byte{
	s_addr := strings.Split(addr,"/")
	var peer_addr net.Conn
	for _, conn := range connList {
		if conn.RemoteAddr().String() == s_addr[0] {
			peer_addr = conn
		}
	}
	block := s_addr[1]
	peer_addr.Write([]byte("recv"))
	// got ACK
	buff := make([]byte, 512)	
	_, err := peer_addr.Read(buff)		
	checkError(err)
	peer_addr.Write([]byte(block))
	buff = make([]byte, 512)
	_, err = peer_addr.Read(buff)
	checkError(err)
	return buff

}