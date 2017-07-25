package main

import (
	"net"
	"fmt"
	"log"
	"time"
	"os"
	"path"
	"io/ioutil"

)

type Peer struct {
	Ip string
	Port string
}

func main() {

	master_port := "8000"
	master_ip := "127.0.0.1"
	master := Peer{Ip: master_ip, Port: master_port}
	// get current working directory
	ex, err := os.Executable()		
    if err != nil {
        panic(err)
    }
    exPath := path.Dir(ex)
    path := exPath + "/" + master.Ip + ":" + master.Port
    // create folder with master's address
    if _, err := os.Stat(path); os.IsNotExist(err) {
		os.Mkdir(path, 0777)
	}

	connectToMaster(master,path)

}

func connectToMaster(dst Peer, path string) {
    conn, err := net.DialTimeout("tcp", dst.Ip + ":" + dst.Port, time.Duration(10) * time.Second)
   	if err != nil {
		log.Fatalln(err)

    } else {
        log.Println("Connected to master, Sending msg...")
		conn.Write([]byte("Available"))

		for {
			// get send/recv
			buff := make([]byte, 1024)	
			n, err := conn.Read(buff)
			if err != nil {
				log.Println(err)
			}
			conn.Write([]byte("ACK"))

			if string(buff[:4]) == "send" {
				n, err = conn.Read(buff)
				if err != nil {
					log.Println(err)
				}
				conn.Write([]byte("GOT NAME"))
				// Read file block
				log.Println("Waiting for file-block")
				dataBuff := make([]byte, 512)
				n2, err := conn.Read(dataBuff)
				if err != nil {
					log.Fatal(err)
				}
				// create file block
				f, err := os.Create(path + "/" + string(buff[:n]))
				if err != nil {
					log.Fatal(err)
				}
				f.Chmod(0777)
		    	b, err := f.WriteString(string(dataBuff[:n2]))
		    	fmt.Printf("Wrote %d bytes\n", b)
		    	f.Sync()
		    	f.Close()
			} else if string(buff[:4]) == "recv" {
				n, err := conn.Read(buff)
				if err != nil {
					log.Println(err)
				}
				filename := string(buff[:n])
				// search for file
				dat, err := ioutil.ReadFile(path + "/" + filename)
				if err != nil {
					log.Println(err)
				}
			    conn.Write(dat)
			}

	    }

	}

}


