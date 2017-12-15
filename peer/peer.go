package main

import (
	"net"
	"fmt"
	"os"
	"log"
	"strings"
	"flag"
	"time"
	"path"
	"io/ioutil"
	"path/filepath"
	"encoding/json"
	"io"
	"strconv"
)


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


func listen(peer Peer) {
	
	listen, err := net.Listen("tcp", ":" + peer.Port)
	defer listen.Close()
	if err != nil {
		log.Fatalf("Socket listen port %s failed,%s", peer.Port, err)
		os.Exit(1)
	}
	log.Printf("Begin listen port: %s", peer.Port)
	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Fatalln(err)
			continue
		}	
		handler(conn, peer.Port)
	}
}

func handler(conn net.Conn, myport string) {	
	
	s_addr := strings.Split(conn.RemoteAddr().String(),":")
	otherPeer := Peer{Ip: s_addr[0], Port: s_addr[1], Conn: conn}
	log.Printf("Got request" + " from: %v",otherPeer)
	
	// Implement a table to keep the IDs and IP addresses
	connList = append(connList, &otherPeer)	

	// get current working directory
	ex, err := os.Executable()		
    if err != nil {
        panic(err)
    }
    exPath := path.Dir(ex)
	myDir := exPath + "/" + "<" + myport + ">" + otherPeer.String()

	// create folder with peer's address
    if _, err := os.Stat(myDir); os.IsNotExist(err) {
		os.Mkdir(myDir, 0777)
	}
	go manageMesseges(conn, myDir)
}

func connectToServer(dst Peer, myport int) {
	
	// fix ip and dial
	myAddr, err := net.ResolveIPAddr("ip", "127.0.0.1")
    if err != nil {
        panic(err)
    }
    localTCPAddr := net.TCPAddr{
        IP: myAddr.IP,
   	    Port: myport}
	d := &net.Dialer{LocalAddr: &localTCPAddr,Timeout: time.Duration(10)*time.Second}
    
    // use conn istead of _
    _, err = d.Dial(dst.Network(), dst.String())   	
   	if err != nil {
		log.Fatalln(err)

    } else {
        log.Println("Connected to central server")
	}
}

func manageMesseges(conn net.Conn, path string) {
	for {
    	
    	var msg message
		decoder := json.NewDecoder(conn)
    	err := decoder.Decode(&msg)    	
    	checkFatal(err)
    	if err == io.EOF {
    		conn.Close()
    		break
    	}

		if msg.Type == "send" {

			filename := strconv.Itoa(int(msg.Name))
			f, err := os.Create(filepath.Join(path, filename))
			checkFatal(err)
			f.Chmod(0777)
	    	b, err := f.WriteString(string(msg.Data))
	    	log.Printf("Wrote %d bytes to file: %s \n", b, filename)
	    	f.Sync()
	    	f.Close()
	    	
	    	send_ack := message{"send_ack", []byte(""), msg.Name}
			json.NewEncoder(conn).Encode(&send_ack)

		} else if msg.Type == "recv" {
			
			filename := strconv.Itoa(int(msg.Name))
			dat, err := ioutil.ReadFile(filepath.Join(path, filename))
			if err != nil {
				log.Println(err)
			}
	    	log.Printf("Sending file: %s \n", filename)
		    
		    recv_ack := message{"recv_ack", dat, msg.Name}
			json.NewEncoder(conn).Encode(&recv_ack)

		} else if msg.Type == "delete" {
			
			filename := strconv.Itoa(int(msg.Name))
	    	log.Printf("Removing file: %s \n", filename)
			err := os.Remove(filepath.Join(path, filename))
			checkError(err)

			del_ack := message{"del_ack", []byte(""), msg.Name}
			json.NewEncoder(conn).Encode(&del_ack)

		}
    }
}

func main() {

	myport := *flag.Int("port", 9000, "Port to run this node on")
	interface_addr, _ := net.InterfaceAddrs()
	local_ip := interface_addr[0].String()
	peer := Peer{Ip: local_ip, Port: strconv.Itoa(myport)}
	fmt.Println("Peer details:", peer)

	// start listening to incoming connections
	go listen(peer)

	// connect to central server
	server_port := "8002"
	server_ip := "127.0.0.1"
	server := Peer{Ip: server_ip, Port: server_port, NetType: "tcp"}
	go connectToServer(server, 8000)

	// mount the FUSE file system
	mountpoint := selectMountpoint()
	FUSE(mountpoint)
}

