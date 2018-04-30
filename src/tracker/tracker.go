package main

import (
	"os"
	"fmt"
	// "errors"
	"net"
	"net/rpc"
)

type Tracker struct {
	Address string
	Addresses map[string]struct{}
	RelayAddress string
}


func getLocalAddress() string {

    var localaddress string
    ifaces, err := net.Interfaces()
    if err != nil {
        panic("init: failed to find network interfaces")
    }
    // find the first non-loopback interface with an IP address
    for _, elt := range ifaces {
        if elt.Flags & net.FlagLoopback == 0 && elt.Flags & net.FlagUp != 0 {
            addrs, err := elt.Addrs()
            if err != nil {
                panic("init: failed to get addresses for network interface")
            }
            for _, addr := range addrs {
                if ipnet, ok := addr.(*net.IPNet); ok {
                    if ip4 := ipnet.IP.To4(); len(ip4) == net.IPv4len {
                        localaddress = ip4.String()
                        break
                    }
                }
            }
        }
    }
    if localaddress == "" {
        panic("init: failed to find non-loopback interface with valid address on this node")
    }
    return localaddress
}



func (t *Tracker) create() {
	rpc.Register(t)
	tcpAddr, err := net.ResolveTCPAddr("tcp",t.Address)
	checkFatalError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkFatalError(err)
	fmt.Println("Tracker serving on: ",t.Address)
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go rpc.ServeConn(conn)
	}

}

func (t *Tracker) GetRootPeer(peer_id string,reply *map[string]struct{}) error {
	*reply = t.Addresses
	t.Addresses[peer_id] = struct{}{}
	return nil
}

func (t *Tracker) GetRelayAddr(dummy bool,reply *string) error {
	*reply = t.RelayAddress
	return nil
}


func checkFatalError(err error) {
    if err != nil {
        fmt.Println("Fatal error ", err.Error())
        os.Exit(1)
    }
}


func main() {
	
	// root_peer := getLocalAddress()+":5000"
	bootMap := make(map[string]struct{})
	// root[root_peer] = struct{}{}

	IpTable := new(Tracker)
	IpTable.Address = getLocalAddress()+":1234"			// tracker serve addr
	IpTable.Addresses = bootMap 							// peers serve addr
	IpTable.RelayAddress = getLocalAddress()+":5555"	// relay serve addr
	IpTable.create()

}