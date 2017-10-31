package main

import (
	"net"
	"fmt"
	"os"
	"log"
	"strings"
	"flag"
	"encoding/json"
	"time"
	"sort"
)


// global variables
var connList []*Peer

// structures
type Peer struct {
	Ip string
	Port string
	Conn net.Conn
	DataSent uint64
	ResponseTime time.Duration
}

type message struct {
	Type string `json:"type"`
	Data []byte `json:"data"`
	Number uint64 `json:"number"`
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
	slave := Peer{Ip: s_addr[0], Port: s_addr[1], Conn: conn}
	log.Printf("Got request" + " from: %v",slave)
	connList = append(connList, &slave)	
}


func sendBlock(peer *Peer, data []byte, block uint64) {
	
	encrypted_data := Encrypt(data,[]byte("123"),int64(len(data)))
	// peer, block := getConnBlock(addr)
	conn := peer.Conn
	m := message{"send", encrypted_data, block}	
	json.NewEncoder(conn).Encode(&m)
	
	start_time := time.Now()
	
	var ack message
	decoder := json.NewDecoder(conn)
	
	peer.ResponseTime = time.Since(start_time)
	peer.DataSent += uint64(len(data)) 
 	
 	err := decoder.Decode(&ack)
 	checkError(err)


}

func recvBlock(peer *Peer, block uint64) ([]byte, error) {

	conn := peer.Conn
	m := message{"recv", []byte(""), block}
	err := json.NewEncoder(conn).Encode(&m)
	start_time := time.Now()	
	var ack message
	decoder := json.NewDecoder(conn)
 	peer.ResponseTime = time.Since(start_time)
 	err = decoder.Decode(&ack)
 	decrypted_data := Decrypt(ack.Data,[]byte("123"))
 	
	return decrypted_data, err
}

func deleteBlock(peer *Peer, block uint64) {
	
	// peer, block := getConnBlock(addr)
	conn := peer.Conn
	m := message{"delete", []byte(""), block}
	json.NewEncoder(conn).Encode(&m)
	
	start_time := time.Now()

	var ack message
	decoder := json.NewDecoder(conn)

 	peer.ResponseTime = time.Since(start_time)
	
 	err := decoder.Decode(&ack)
 	checkError(err)

}

// func getConnBlock(addr *Peer) (*Peer, int64) {
	
// 	s_addr := strings.Split(addr, "/")
// 	var conn *Peer
// 	for _, p := range connList {
// 		if p.Conn.RemoteAddr().String() == s_addr[0] {
// 			conn = p
// 		}
// 	}
// 	block, err := strconv.Atoi(s_addr[1])
// 	checkError(err)
// 	return conn, int64(block)
// }

func sortPeers(loadType string, peerArray []*Peer) {

	if loadType == "data" {
		
		sort.Slice(peerArray[:], func(i, j int) bool {
    		return peerArray[i].DataSent < peerArray[j].DataSent
		})

	} else if loadType == "time" {

		sort.Slice(peerArray[:], func(i, j int) bool {
    		return peerArray[i].ResponseTime < peerArray[j].ResponseTime
		})

	}
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