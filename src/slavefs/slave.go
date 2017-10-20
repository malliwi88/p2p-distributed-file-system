package main

import (
	"net"
	"log"
	"time"
	"os"
	"path"
	"io/ioutil"
	"path/filepath"
	"encoding/json"
	"io"
	"strconv"
	"flag"
)

type Peer struct {
	Ip string
	Port string
	NetType string
}
func (p *Peer) String() string {
	return p.Ip + ":" + p.Port
}
func (p *Peer) Network() string {
	return p.NetType
}

type message struct {
	Type string `json:"type"`
	Data []byte `json:"data"`
	Number int64 `json:"number"`
}

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

func main() {
	myport := flag.Int("port", 9000, "Port to run this node on")
	flag.Parse()
	master_port := "8000"
	master_ip := "127.0.0.1"
	master := Peer{Ip: master_ip, Port: master_port, NetType: "tcp"}
	conn, path := connectToMaster(master,*myport)
	manageMesseges(conn, path)
}

func connectToMaster(dst Peer, myport int) (net.Conn, string) {
	
	// fix ip and dial
	slaveAddr, err := net.ResolveIPAddr("ip", "127.0.0.1")
    if err != nil {
        panic(err)
    }
    localTCPAddr := net.TCPAddr{
        IP: slaveAddr.IP,
   	    Port: myport}
	d := &net.Dialer{LocalAddr: &localTCPAddr,Timeout: time.Duration(10)*time.Second}
    
    // get current working directory
	ex, err := os.Executable()		
    if err != nil {
        panic(err)
    }
    exPath := path.Dir(ex)
	myDir := exPath + "/" + dst.String()
    conn, err := d.Dial(dst.Network(),dst.String())   	
   	if err != nil {
		log.Fatalln(err)

    } else {
        log.Println("Connected to master")
	    // create folder with master's address
	    if _, err := os.Stat(myDir); os.IsNotExist(err) {
			os.Mkdir(myDir, 0777)
		}
	}
	return conn, myDir
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

			filename := strconv.Itoa(int(msg.Number))
			f, err := os.Create(filepath.Join(path, filename))
			checkFatal(err)
			f.Chmod(0777)
	    	b, err := f.WriteString(string(msg.Data))
	    	log.Printf("Wrote %d bytes to file: %s \n", b, filename)
	    	f.Sync()
	    	f.Close()
	    	
	    	send_ack := message{"send_ack", []byte(""), msg.Number}
			json.NewEncoder(conn).Encode(&send_ack)

		} else if msg.Type == "recv" {
			
			filename := strconv.Itoa(int(msg.Number))
			dat, err := ioutil.ReadFile(filepath.Join(path, filename))
			if err != nil {
				log.Println(err)
			}
	    	log.Printf("Sending file: %s \n", filename)
		    
		    recv_ack := message{"recv_ack", dat, msg.Number}
			json.NewEncoder(conn).Encode(&recv_ack)

		} else if msg.Type == "delete" {
			
			filename := strconv.Itoa(int(msg.Number))
	    	log.Printf("Removing file: %s \n", filename)
			err := os.Remove(filepath.Join(path, filename))
			checkError(err)

			del_ack := message{"del_ack", []byte(""), msg.Number}
			json.NewEncoder(conn).Encode(&del_ack)

		}
    }
}

