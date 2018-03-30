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
	"bazil.org/fuse"
)

var Root *Peer
var encrypt_key = "123"
const num_succ_list = 3
const num_find_req = 3
const num_bits = 161
const keySize = sha1.Size * 8
var two = big.NewInt(2)
var hashMod = new(big.Int).Exp(big.NewInt(2), big.NewInt(keySize), nil)

// computes the address of a position across the ring that should be pointed to by the given finger table entry (using 1-based numbering)
func jump(address string, fingerentry int) *big.Int {

    n := hash_0(address)
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
	SuccList	[]string
	Successor 	string
	Predecessor	string
	Store		map[string]struct{}
	Next		int
}

// server methods
func (n *Peer) init(address string) {
	n.Address = address
	n.Finger = make([]string,num_bits)
	n.SuccList = make([]string,0,num_succ_list)
	n.Successor = ""
	n.Predecessor = ""
	n.Store = make(map[string]struct{})
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
	id := hash_0(n.Address)
	var reply string
	err := call(addr,"Peer.Find",id,&reply)
	checkError(err)
	
	if err == nil {
		n.Successor = reply
		var res map[string][]byte
		err = call(n.Successor,"Peer.GetAll",n.Address,&res)
		checkError(err)
		// n.Store = res
		n.put_all(res)

	}
}

func (n *Peer) find_successor(id *big.Int) string {

	if (between(hash_0(n.Address),id,hash_0(n.Successor),true)){
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
		if(n.Finger[i] != "" && between(hash_0(n.Address),hash_0(n.Finger[i]),id,false)){
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
			if (between(hash_0(n.Address),hash_0(preAddr),hash_0(n.Successor),false)){
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

	if ( n.Predecessor == "" || between(hash_0(n.Predecessor),hash_0(addr),hash_0(n.Address),false) ){
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

func (n *Peer) replicate_store() {
// my succ list should have my data all the time, replicate if not	
	for {		
		time.Sleep(60 * time.Second)

		new_map := make(map[string][]byte)
		var peerAddr string
		var blockName string
		for key := range n.Store {
			peerAddr = strings.Split(key,"|")[0]
			blockName = strings.Split(key,"|")[1]
			var err error
			new_map[key], err = readFromDisk(peerAddr,blockName)
			checkError(err)			
		}
		var reply bool	
		for _, succAddr := range n.SuccList{
			err := call(succAddr,"Peer.PutAll",new_map,&reply)
			checkError(err)
		}
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
		n.Store[key] = struct{}{}
		peerAddr := strings.Split(key,"|")[0]
		blockName := strings.Split(key,"|")[1]
		writeToDisk(peerAddr,blockName,value)
	}
	
}

func (n *Peer) get_all(address string) map[string][]byte {

	new_map := make(map[string][]byte)
	addr_id := hash_0(address)
	pre_id := hash_0(n.Predecessor)
	var peerAddr string
	var blockName string

	for key := range n.Store {
		if(between(pre_id,hash_1(key),addr_id,true)){
			peerAddr = strings.Split(key,"|")[0]
			blockName = strings.Split(key,"|")[1]
			var err error
			new_map[key], err = readFromDisk(peerAddr,blockName)
			checkError(err)
		}
		if(between(pre_id,hash_2(key),addr_id,true)){
			peerAddr = strings.Split(key,"|")[0]
			blockName = strings.Split(key,"|")[1]
			var err error
			new_map[key], err = readFromDisk(peerAddr,blockName)
			checkError(err)
		}
		if(between(pre_id,hash_3(key),addr_id,true)){
			peerAddr = strings.Split(key,"|")[0]
			blockName = strings.Split(key,"|")[1]
			var err error
			new_map[key], err = readFromDisk(peerAddr,blockName)
			checkError(err)
		}
	}

	for key, _ := range new_map {
        delete(n.Store, key)
        peerAddr = strings.Split(key,"|")[0]
		blockName = strings.Split(key,"|")[1]
		err := deleteFromDisk(peerAddr,blockName)
		checkError(err)
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
	// *value = n.Store[key]
	peerAddr := strings.Split(key,"|")[0]
	blockName := strings.Split(key,"|")[1]
	var err error
	*value, err = readFromDisk(peerAddr,blockName)
    return err
}

func (n *Peer) GetAll(addr string, reply *map[string][]byte) error {
	bucket := n.get_all(addr)
    *reply = bucket
    return nil
}

func (n *Peer) Put(pair Args, reply *bool) error {
	n.Store[pair.Key] = struct{}{}

	peerAddr := strings.Split(pair.Key,"|")[0]
	blockName := strings.Split(pair.Key,"|")[1]
	writeToDisk(peerAddr,blockName,pair.Val)	
	*reply = true
    return nil
}

func (n *Peer) Replicate(pair Args, reply *bool) error {
	// send replicas to succ list
	req := Args{pair.Key, pair.Val}
	var res bool
	for _, succAddr := range n.SuccList{
		if succAddr != n.Address{
			err := call(succAddr, "Peer.Put",req,&res)
			checkError(err)
		}
	}
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
	
	peerAddr := strings.Split(key,"|")[0]
	blockName := strings.Split(key,"|")[1]
	err := deleteFromDisk(peerAddr,blockName)
    
	*reply = true
    return err
}

func (n *Peer) Dereplicate(key string, reply *bool) error {
	// delete replicas from succ list
	var res bool
	for _, succAddr := range n.SuccList{
		if succAddr != n.Address{
			err := call(succAddr, "Peer.Delete",key,&res)
			checkError(err)
		}
	}
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

func InterruptHandler(Root *Peer, mountpoint string) {
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)
	<-sigs
	for _, file := range *(FileSystem.root.files) {
		(*file).SaveMetaFile()
	}
	err := fuse.Unmount(mountpoint)
	checkFatalError(err)			
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
	go FUSE(*mountpoint)
	go InterruptHandler(Root,*mountpoint)

	for {
	
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		args := strings.Fields(input);
		
		if (args[0] == "join")  {

			Root.join(getLocalAddress()+":"+args[1])
			go Root.replicate_store()

		} else if (args[0] == "quit")  {
			// send all files to succ
			var reply bool
			err := call(Root.Successor,"Peer.PutAll",Root.get_all(Root.Address),&reply)
			checkError(err)
			fmt.Println("response: ", reply)
			// save meta file
			for _, file := range *(FileSystem.root.files) {
				(*file).SaveMetaFile()
			}
			// unmount fuse
			err = fuse.Unmount(*mountpoint)
			checkFatalError(err)	
			os.Exit(3)

		} else if (args[0] == "put")  {

			// var reply bool
			// value := strings.Join(args[2:]," ")
			// req := Args{args[1],value}
			// id := hash_0(args[1])
			// addr := Root.find_successor(id)
			// err := call(addr, "Peer.Put",req,&reply)
			// checkError(err)
			// fmt.Println("response: ", reply)

		} else if (args[0] == "putrandom")  {

		} else if (args[0] == "get")  {

			// var reply string
			// id := hash_0(args[1])
			// addr := Root.find_successor(id)
			// err := call(addr, "Peer.Get",args[1],&reply)
			// checkError(err)
			// fmt.Println("response: ", reply)

		} else if (args[0] == "delete")  {
			
			// var reply bool
			// id := hash_0(args[1])
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
			fmt.Println("Key/Val Store: ",Root.Store)
			fmt.Println("----------------------------------------")
		
		} else if (args[0] == "dumpkey")  {

		} else if (args[0] == "dumpaddr")  {

		} else if (args[0] == "dumpall")  {
 
		} else if (args[0] == "ping")  {
			
			var reply int
			err := call(getLocalAddress()+":"+args[1], "Peer.Ping",1,&reply)
			checkError(err)
			fmt.Println("response: ", reply)
		
		} else {            
			printHelp()
		}
	}
}