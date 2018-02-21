package main

import (
	"net"
	"fmt"
	"os"
	"log"
	"strings"
	"flag"
	"io/ioutil"
	"path/filepath"
	"encoding/json"
	"io"
	"strconv"
)


// functions
func listen(me Peer) {

	listen, err := net.Listen("tcp", ":" + me.Port)
	defer listen.Close()
	if err != nil {
		log.Fatalf("Socket listen port %s failed,%s", me.Port, err)
		os.Exit(1)
	}
	log.Printf("Begin listen port: %s", me.Port)
	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Fatalln(err)
			continue
		}	
		go manageMesseges(conn,me)
		
	}
}


func connectToServer(me Peer, dst Peer) {
	
    conn, err := net.Dial(dst.Network(), dst.String())   	
   	if err != nil {
		log.Fatalln(err)

    } else {
        log.Println("Connected to central server")
	}
	fmt.Print("id: ")
	var id string
    fmt.Scanln(&id)
	myID = id
	msg := message{"login", []byte(""), 0, myID, me.IP+":"+me.Port, ""}
	json.NewEncoder(conn).Encode(&msg)
}


func manageMesseges(conn net.Conn, myInfo Peer) {
    	
	var msg message
	decoder := json.NewDecoder(conn)
	err := decoder.Decode(&msg)    	
	checkFatal(err)
	
	if err == io.EOF {
		conn.Close()
		return
	}
	if msg.Type == "send" {
		filename := strconv.Itoa(int(msg.Name))
		myDir := "/mnt/" + (msg.PeerID)
		// create folder with peer's address
	    if _, err := os.Stat(myDir); os.IsNotExist(err) {
			os.Mkdir(myDir, 0777)
		}
		f, err := os.Create(filepath.Join(getPeerDir(msg.PeerID), filename))
		checkFatal(err)
		f.Chmod(0777)
    	b, err := f.WriteString(string(msg.Data))
    	log.Printf("Wrote %d bytes to file: %s \n", b, filename)
    	f.Sync()
    	f.Close()
    	send_ack := message{"send_ack", []byte(""), msg.Name,"","",""}
		json.NewEncoder(conn).Encode(&send_ack)

	} else if msg.Type == "recv" {

		filename := strconv.Itoa(int(msg.Name))
		dat, err := ioutil.ReadFile(filepath.Join(getPeerDir(msg.PeerID), filename))
		if err != nil {
			log.Println(err)
		}
    	log.Printf("Sending file: %s \n", filename)
	    
	    recv_ack := message{"recv_ack", dat, msg.Name,"","",""}
		json.NewEncoder(conn).Encode(&recv_ack)


	} else if msg.Type == "delete" {
		
		filename := strconv.Itoa(int(msg.Name))
    	log.Printf("Removing file: %s \n", filename)
		err := os.Remove(filepath.Join(getPeerDir(msg.PeerID), filename))
		checkError(err)

		del_ack := message{"del_ack", []byte(""), msg.Name,"","",""}
		json.NewEncoder(conn).Encode(&del_ack)


	} else if msg.Type == "add" {
    	
    	fmt.Println(msg)
    	s_addr := strings.Split(msg.PeerAddr,":")
    	otherPeer := Peer{ID: msg.PeerID, IP: s_addr[0], Port: s_addr[1], NetType: "tcp"}
		myDir := "/mnt/" + (otherPeer.ID)
		otherPeer.PathToFiles = myDir
		connList = append(connList, &otherPeer)	
    	ack := message{"ack", []byte(""), 0, "", "", ""}
		json.NewEncoder(conn).Encode(&ack)

  	} else if msg.Type == "update" {
    	
    	fmt.Println(msg)
		for _, p := range connList {
			if p.ID == msg.PeerID {
				p_addr := strings.Split(msg.PeerAddr, ":")
				p.IP = p_addr[0]
				p.Port = p_addr[1]
			}
		}
    	ack := message{"ack", []byte(""), 0, "", "", ""}
		json.NewEncoder(conn).Encode(&ack)
  	}
  	conn.Close()

}

func main() {

	myport := flag.Int("port", 9000, "Port to run this node on")
	mountpoint := flag.String("mount", "/mnt/fmount", "folder to mount")
    flag.Parse()
    if _, err := os.Stat(*mountpoint); os.IsNotExist(err) {
		os.Mkdir(*mountpoint, 0777)
	}
	local_IP := "127.0.0.1"
	me := Peer{IP: local_IP, Port: strconv.Itoa(*myport)}
	fmt.Println("My details:", me)
	server := Peer{IP: central_IP, Port: strconv.Itoa(central_port), NetType: "tcp"}
	go listen(me)
	connectToServer(me, server)
	FUSE(*mountpoint)
}

