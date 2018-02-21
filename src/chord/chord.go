package main

import (
	"crypto/sha1"
	"math/big"
	"fmt"
	"net"
	"flag"
	"strings"
	"net/rpc"
	"strconv"
	"os"
	"os/signal"
	"syscall"
	"bufio"
	"time"
)

var Root *Peer
var encrypt_key = "123"
const num_succ_list = 3
const num_find_req = 3
const num_bits = 161
const keySize = sha1.Size * 8
var two = big.NewInt(2)
var hashMod = new(big.Int).Exp(big.NewInt(2), big.NewInt(keySize), nil)

// 160-bit hash of a string
func hashString(elt string) *big.Int {

    hasher := sha1.New()
    hasher.Write([]byte(elt))
    return new(big.Int).SetBytes(hasher.Sum(nil))
}

// computes the address of a position across the ring that should be pointed to by the given finger table entry (using 1-based numbering)
func jump(address string, fingerentry int) *big.Int {

    n := hashString(address)
    fingerentryminus1 := big.NewInt(int64(fingerentry) - 1)
    jump := new(big.Int).Exp(two, fingerentryminus1, nil)
    sum := new(big.Int).Add(n, jump)

    return new(big.Int).Mod(sum, hashMod)
}

// returns true if elt is between start and end on the ring, accounting for the boundary where the ring loops back on itself
//If inclusive is true, elt is in (start,end], otherwise it is in (start,end).
func between(start, elt, end *big.Int, inclusive bool) bool {

    if end.Cmp(start) > 0 {
        return (start.Cmp(elt) < 0 && elt.Cmp(end) < 0) || (inclusive && elt.Cmp(end) == 0)
    } else {
        return start.Cmp(elt) < 0 || elt.Cmp(end) < 0 || (inclusive && elt.Cmp(end) == 0)
    }
}

//finds the first address that is not a loopback device, so it should be one that an outside machine can connect to
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

func checkFatalError(err error) {
    if err != nil {
        fmt.Println("Fatal error ", err.Error())
        os.Exit(1)
    }
}

func checkError(err error) {
    if err != nil {
        fmt.Println("Connection error ", err.Error())
    }
}

type Peer struct {

	Address    	string
	Finger 		[]string		// the addresses should contain an ip and a port number
	SuccList	[]string;
	Successor 	string
	Predecessor	string
	Store		map[string][]byte
	Next		int
}

// server methods
func (n *Peer) init(address string) {
	n.Address = address
	n.Finger = make([]string,num_bits)
	n.SuccList = make([]string,0,num_succ_list)
	n.Successor = ""
	n.Predecessor = ""
	n.Store = make(map[string][]byte)
	n.Next = 1
}

func (n *Peer) create() {

	fmt.Println("Server running on: ",n.Address)
	rpc.Register(n)
	tcpAddr, err := net.ResolveTCPAddr("tcp",n.Address)
	checkFatalError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkFatalError(err)
	n.Successor = n.Address
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		} 
		go rpc.ServeConn(conn)
	}
}

func (n *Peer) join(addr string) {

	n.Predecessor = ""
	id := hashString(n.Address)
	var reply string
	err := call(addr,"Peer.Find",id,&reply)
	checkError(err)
	
	if err == nil {
		n.Successor = reply
		var res map[string][]byte
		err = call(n.Successor,"Peer.GetAll",n.Address,&res)
		checkError(err)
		n.Store = res
	}
}

func (n *Peer) find_successor(id *big.Int) string {

	if (between(hashString(n.Address),id,hashString(n.Successor),true)){
		return n.Successor
	} else {
		var reply string
		n_dash := n.closest_preceding_node(id)
		err := call(n_dash,"Peer.Find",id,&reply)
		checkError(err)
		return reply
	}
}

func (n *Peer) closest_preceding_node(id *big.Int) string {

	for i := num_bits-1; i >= 1; i-- {
		if(n.Finger[i] != "" && between(hashString(n.Address),hashString(n.Finger[i]),id,false)){
			return n.Finger[i]
		}
	}
	return n.Successor
}

func (n *Peer) stabilize() {
	
	for{
		time.Sleep(1 * time.Second)
		
		// x := n.Successor.Predecessor
		var preAddr string
		err := call(n.Successor, "Peer.Prev",1,&preAddr)
		checkError(err)
		if (preAddr != "") {
			if (between(hashString(n.Address),hashString(preAddr),hashString(n.Successor),false)){
				n.Successor = preAddr
			}
		}
		var reply bool
		err = call(n.Successor, "Peer.Notify",n.Address,&reply)
		checkError(err)
		
		// y := n.Successor.SuccList
		succlist := make([]string,0,num_succ_list)
		err = call(n.Successor, "Peer.Succs", 1, &succlist)
		if err != nil {
			if len(n.SuccList) == 0 {
				n.Successor = n.Address
			} else {
				n.SuccList = append(n.SuccList[:0], n.SuccList[1:]...)
				if len(n.SuccList) == 0{
					n.Successor = n.Address
				} else {
					n.Successor = n.SuccList[0]
				}
			}
		} else {
			n.SuccList =  append([]string{n.Successor}, succlist...)
			if (len(n.SuccList) > num_succ_list){
				n.SuccList = append(n.SuccList[:num_succ_list], n.SuccList[num_succ_list+1:]...)
			}
		}
	}
}

func (n *Peer) notify(addr string) {

	if ( n.Predecessor == "" || between(hashString(n.Predecessor),hashString(addr),hashString(n.Address),false) ){
		n.Predecessor = addr
	} 
}

func (n *Peer) fix_fingers() {
	
	for {		
		n.Next = n.Next + 1
		if (n.Next > num_bits-1){
			time.Sleep(5 * time.Second)
			n.Next = 1
		}
		n.Finger[n.Next] = n.find_successor(jump(n.Address,n.Next))
	}
}

func (n *Peer) check_predecessor() {
	
	var reply int
	for {
		time.Sleep(1 * time.Second)
		if(call(n.Predecessor,"Peer.Ping",1,&reply) != nil){
			n.Predecessor = ""
		}
	}
}

func (n *Peer) put_all(bucket map[string][]byte) {

	for key, value := range bucket {
		n.Store[key] = value
	}
}

func (n *Peer) get_all(address string) map[string][]byte {

	new_map := make(map[string][]byte)
	addr_id := hashString(address)
	pre_id := hashString(n.Predecessor)

	for key, _ := range n.Store {
		if(between(pre_id,hashString(key),addr_id,true)){
			new_map[key] = n.Store[key]
		}
	}

	for key, _ := range new_map {
        delete(n.Store, key)
	}

	return new_map
}



// exported server methods
type Args struct {
    Key string
    Val []byte
}

func (n *Peer) Ping(arg int, reply *int) error {
    *reply = arg
    return nil
}

func (n *Peer) Get(key string, value *[]byte) error {
	*value = n.Store[key]
    return nil
}

func (n *Peer) GetAll(addr string, reply *map[string][]byte) error {
	bucket := n.get_all(addr)
    *reply = bucket
    return nil
}

func (n *Peer) Put(pair Args, reply *bool) error {
	n.Store[pair.Key] = pair.Val
	*reply = true
    return nil
}

func (n *Peer) PutAll(bucket map[string][]byte, reply *bool) error {
	n.put_all(bucket)
	*reply = true
    return nil
}

func (n *Peer)	Delete(key string, reply *bool) error {
	delete(n.Store,key)
	*reply = true
    return nil
}

func (n *Peer) Find(id *big.Int, addr *string) error {
	*addr = n.find_successor(id)
    return nil
}

func (n *Peer) Prev(dummy int, pre_addr *string) error{
	*pre_addr = n.Predecessor
	return nil
}

func (n *Peer) Succs(dummy int, succ_list *[]string) error {
    for _, succ := range n.SuccList {
        *succ_list = append(*succ_list,succ)
    }
	return nil
}

func (n *Peer) Notify(addr string, reply *bool) error{
	n.notify(addr)
	*reply = true
	return nil
}

// client method (has to be generic)
func call(address string, method string, req interface{}, reply interface{}) error {

	client, err := rpc.Dial("tcp", address)
	if err != nil {
		return err
	}
	defer client.Close()
	err = client.Call(method, req, reply)
	if err != nil {
		return err
	}
	return nil
}



func printHelp() {
	fmt.Println("- help:			display commands")
	// fmt.Println("- create:		create a new ring.")
	fmt.Println("- join <addr>:		join an existing ring.")
	fmt.Println("- quit:			quit the program.")
	// fmt.Println("- ping <addr>:		send ping to the address.")
	// fmt.Println("- put <key> <val>:	insert key and value into the ring.")
	// fmt.Println("- putrandom <n>:	insert randomly generated keys.")
	// fmt.Println("- get <key>:		find the given key in the ring.")
	// fmt.Println("- delete <key>:		delete key from the ring.")
	fmt.Println("- dump:			display information about the current node.")
	// fmt.Println("- dumpkey <key>:	display info about node with key.")
	// fmt.Println("- dumpaddr <addr>:	display info about node with given address.")
	// fmt.Println("- dumpall:		dump all information about every peer.\n")
}

func InterruptHandler(Root *Peer) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	var reply bool
	err := call(Root.Successor,"Peer.PutAll",Root.Store,&reply)
	checkError(err)
	fmt.Println("\nresponse: ", reply)
	os.Exit(3)
}

func main() {
	port := flag.Int("port",3410,"port <n>: set the port it should listen to")
	mountpoint := flag.String("mount", "/mnt/fmount", "folder to mount")
	flag.Parse()
	
	Root = new(Peer)
	Root.init(getLocalAddress()+":"+strconv.Itoa(*port))
	printHelp()
	go Root.create()
	go Root.stabilize()
	go Root.fix_fingers()
	go Root.check_predecessor()
	go InterruptHandler(Root)
	go FUSE(*mountpoint)

	for {
	
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		args := strings.Fields(input);
		
		if (args[0] == "join")  {

			Root.join(getLocalAddress()+":"+args[1])

		} else if (args[0] == "quit")  {

			var reply bool
			err := call(Root.Successor,"Peer.PutAll",Root.Store,&reply)
			checkError(err)
			fmt.Println("response: ", reply)
			os.Exit(3)

		} else if (args[0] == "put")  {

			// var reply bool
			// value := strings.Join(args[2:]," ")
			// req := Args{args[1],value}
			// id := hashString(args[1])
			// addr := Root.find_successor(id)
			// err := call(addr, "Peer.Put",req,&reply)
			// checkError(err)
			// fmt.Println("response: ", reply)

		} else if (args[0] == "putrandom")  {

		} else if (args[0] == "get")  {

			// var reply string
			// id := hashString(args[1])
			// addr := Root.find_successor(id)
			// err := call(addr, "Peer.Get",args[1],&reply)
			// checkError(err)
			// fmt.Println("response: ", reply)

		} else if (args[0] == "delete")  {
			
			// var reply bool
			// id := hashString(args[1])
			// addr := Root.find_successor(id)
			// err := call(addr, "Peer.Delete",args[1],&reply)
			// checkError(err)
			// fmt.Println("response: ", reply)

		} else if (args[0] == "dump")  {
			
			fmt.Println("----------------------------------------")
			fmt.Println("Predecessor: ",Root.Predecessor)
			fmt.Println("Address: ",Root.Address)
			fmt.Println("Successor: ",Root.Successor)
			fmt.Println("Successor List: ",Root.SuccList)
			keys := make([]string, 0, len(Root.Store))
			for k , _ := range Root.Store {
        		keys = append(keys, k)
    		}
			fmt.Println("Key/Val Store: ",keys)
			fmt.Println("----------------------------------------")
		
		} else if (args[0] == "dumpkey")  {

		} else if (args[0] == "dumpaddr")  {

		} else if (args[0] == "dumpall")  {
 
		} else if (args[0] == "ping")  {
			
			var reply int
			err := call(args[1], "Peer.Ping",1,&reply)
			checkError(err)
			fmt.Println("response: ", reply)
		
		} else {            
			printHelp()
		}
	}
}