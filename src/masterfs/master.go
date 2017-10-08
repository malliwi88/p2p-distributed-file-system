package main

import (
	"net"
	"fmt"
	"os"
	"log"
	"strings"
	"flag"
	"encoding/json"
	"strconv"
)


// global variables
var connList []net.Conn

// structures
type Peer struct {
	Ip string
	Port string
}

type message struct {
	Type string `json:"type"`
	Data []byte `json:"data"`
	Number int64 `json:"number"`
}

// functions
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
	
	s_addr := strings.Split(conn.RemoteAddr().String(),":")
	slave := Peer{Ip: s_addr[0], Port: s_addr[1]}
	log.Printf("Got request" + " from: %s",slave)
	connList = append(connList, conn)	
}


func sendBlock(addr string, data []byte) {
	
	conn, block := getConnBlock(addr)
	m := message{"send", data, block}	
	json.NewEncoder(conn).Encode(&m)
	
	var ack message
	decoder := json.NewDecoder(conn)
 	err := decoder.Decode(&ack)
 	checkError(err)

}

func recvBlock(addr string) ([]byte, error) {
	
	conn, block := getConnBlock(addr)
	m := message{"recv", []byte(""), block}
	json.NewEncoder(conn).Encode(&m)

	var ack message
	decoder := json.NewDecoder(conn)
 	err := decoder.Decode(&ack)
 	checkError(err)
	return ack.Data, err
}

func deleteBlock(addr string) {
	
	conn, block := getConnBlock(addr)
	m := message{"delete", []byte(""), block}
	json.NewEncoder(conn).Encode(&m)

	var ack message
	decoder := json.NewDecoder(conn)
 	err := decoder.Decode(&ack)
 	checkError(err)
}

func getConnBlock(addr string) (net.Conn, int64) {
	
	s_addr := strings.Split(addr, "/")
	var conn net.Conn
	for _, c := range connList {
		if c.RemoteAddr().String() == s_addr[0] {
			conn = c
		}
	}
	block, err := strconv.Atoi(s_addr[1])
	checkError(err)
	return conn, int64(block)
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