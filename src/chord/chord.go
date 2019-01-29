package main

import (
	"crypto/sha1"
	"math/big"
	"fmt"
	"net"
	"flag"
	"strings"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
	"bufio"
	"time"
	"bazil.org/fuse"
    "net/http"
	"io/ioutil"
	"io"
	"encoding/gob"
	"encoding/json"
	"math/rand"
)

var relayAddr string
var trackerAddr string

var globalNat bool
var Root *Peer
var liveConnRelay net.Conn
var encrypt_key string
var myRPCAddr string
var two = big.NewInt(2)
var hashMod = new(big.Int).Exp(big.NewInt(2), big.NewInt(sha1.Size * 8), nil)
const num_succ_list = 3
const num_find_req = 3
const num_bits = 161
const letterBytes = "abcdefghijklmnopqrstuvwxyz0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"

type User struct {
    Uid string
    Pwd string
}

type Neighbour struct {
	Nid string
	Naddr string
}

type LiveMsg struct{
	Command string
	Req interface{}
}

type Args struct {
    Key string
    Val []byte
}

type Peer struct {

	ID 			string
	Address    	string
	Finger 		[]Neighbour		// the Naddresses should contain an ip and a port number
	SuccList	[]Neighbour
	Successor 	Neighbour
	Predecessor	Neighbour
	Store		map[string]struct{}
	Next		int
	Nat         bool
}

func RandStringBytes(n int) string {
    b := make([]byte, n)
    for i := range b {
        b[i] = letterBytes[rand.Intn(len(letterBytes))]
    }
    return string(b)
}




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

func getPublicAddress() string{
    resp, err := http.Get("http://myexternalip.com/raw")
    if err != nil {
    }
    defer resp.Body.Close()
    ip, _ := ioutil.ReadAll(resp.Body)
    return strings.TrimSpace(string(ip))
}

func isBehindNAT() bool {
    return getPublicAddress() != getLocalAddress()
}

func checkFatalError(err error) {
    if err != nil {
        // fmt.Println("Fatal error ", err.Error())
        os.Exit(1)
    }
}

func checkError(err error) {
    if err != nil {
        // fmt.Println("Connection error ", err.Error())
    }
}


// server methods
func (n *Peer) init(id string) {
	n.ID = id
	n.Address = ""
	n.Finger = make([]Neighbour,num_bits)
	n.SuccList = make([]Neighbour,0,num_succ_list)
	n.Successor = Neighbour{}
	n.Predecessor = Neighbour{}
	n.Store = make(map[string]struct{})
	n.Next = 1
	n.Nat = isBehindNAT()
	globalNat = n.Nat
}

func (n *Peer) create() {
	if !n.Nat {
		rpc.Register(n)
		tcpAddr, err := net.ResolveTCPAddr("tcp",getLocalAddress()+":0")
		checkFatalError(err)
		listener, err := net.ListenTCP("tcp", tcpAddr)
		checkFatalError(err)
		n.Address = listener.Addr().String()
		fmt.Println("Server running on: ",n.Address)
		n.Successor = Neighbour{n.ID,n.Address}
		go func(){
			for {
				conn, err := listener.Accept()
				if err != nil {
					continue
				} 
				go rpc.ServeConn(conn)
			}
		}()

	} else {
		// establish live connection!
		var err error
		liveConnRelay, err = net.Dial("tcp", relayAddr)
	    checkFatalError(err)
	    encoder := gob.NewEncoder(liveConnRelay)
	    decoder := gob.NewDecoder(liveConnRelay)
	    var freeaddr string
	    decoder.Decode(&freeaddr)
		myRPCAddr = freeaddr
		fmt.Println("My addr: ",myRPCAddr)
		n.Address = myRPCAddr
		n.Successor = Neighbour{n.ID,n.Address}
		go n.interceptLiveMsgs(encoder,decoder)
	}
}


func (n *Peer) interceptLiveMsgs(encoder *gob.Encoder,decoder *gob.Decoder){
	for {
		// var msg LiveMsg
		msg := &LiveMsg{}
		err := decoder.Decode(msg)
		if err == io.EOF{
			break
		}
		if msg.Command == "Ping"{
			var reply int
			err := n.Ping((msg.Req).(int),&reply)
			checkError(err)
			_ = encoder.Encode(&reply)

		}  else if msg.Command == "Get"{
			var reply []byte
			err := n.Get((msg.Req).(string),&reply)
			checkError(err)
			_ = encoder.Encode(&reply)

		} else if msg.Command == "GetAll"{
			var reply map[string][]byte
			err := n.GetAll((msg.Req).(string),&reply)
			checkError(err)
			_ = encoder.Encode(&reply)

		} else if msg.Command == "Put"{
			var reply bool
			err := n.Put((msg.Req).(Args),&reply)
			checkError(err)
			_ = encoder.Encode(&reply)
			
		} else if msg.Command == "Replicate"{
			var reply bool
			err := n.Replicate((msg.Req).(Args),&reply)
			checkError(err)
			_ = encoder.Encode(&reply)
			
		} else if msg.Command == "PutAll"{
			var reply bool
			err := n.PutAll((msg.Req).(map[string][]byte),&reply)
			checkError(err)
			_ = encoder.Encode(&reply)
			
		} else if msg.Command == "Delete"{
			var reply bool
			err := n.Delete((msg.Req).(string),&reply)
			checkError(err)
			_ = encoder.Encode(&reply)
			
		} else if msg.Command == "Dereplicate"{
			var reply bool
			err := n.Dereplicate((msg.Req).(string),&reply)
			checkError(err)
			_ = encoder.Encode(&reply)
			
		} else if msg.Command == "Find"{
			var reply Neighbour
			err := n.Find((msg.Req).(*big.Int),&reply)
			checkError(err)
			_ = encoder.Encode(&reply)

		} else if msg.Command == "Prev"{
			var reply Neighbour
			err := n.Prev((msg.Req).(int),&reply)
			checkError(err)
			_ = encoder.Encode(&reply)

		} else if msg.Command == "Succs"{
			var reply []Neighbour
			err := n.Succs((msg.Req).(int),&reply)
			checkError(err)
			_ = encoder.Encode(&reply)

		} else if msg.Command == "Notify"{
			var reply bool
			err := n.Notify((msg.Req).(Neighbour),&reply)
			checkError(err)
			_ = encoder.Encode(&reply)
		}

	}
}

func (n *Peer) join(addr string) error{

	n.Predecessor = Neighbour{}
	pid := hash_0(n.ID)
	var reply Neighbour
	err := call(addr,"Peer.Find",pid,&reply)
	checkError(err)
	if err == nil {
		n.Successor = reply
		var res map[string][]byte
		err = call(n.Successor.Naddr,"Peer.GetAll",n.ID,&res)
		checkError(err)
		n.put_all(res)
	}
	return err
}

func (n *Peer) find_successor(id *big.Int) Neighbour {

	if (between(hash_0(n.ID),id,hash_0(n.Successor.Nid),true)){
		return n.Successor
	} else {
		var reply Neighbour
		n_dash := n.closest_preceding_node(id)
		err := call(n_dash.Naddr,"Peer.Find",id,&reply)
		checkError(err)
		return reply
	}
}

func (n *Peer) closest_preceding_node(id *big.Int) Neighbour {
	for i := num_bits-1; i >= 1; i-- {
		if(n.Finger[i].Nid != "" && between(hash_0(n.ID),hash_0(n.Finger[i].Nid),id,false)){
			return n.Finger[i]
		}
	}
	return n.Successor
}

func (n *Peer) stabilize() {
	
	for{
		time.Sleep(1 * time.Second)
		
		// x := n.Successor.Predecessor
		var preNbr Neighbour
		err := call(n.Successor.Naddr, "Peer.Prev",1,&preNbr)
		checkError(err)
		if (preNbr.Nid != "") {
			if (between(hash_0(n.ID),hash_0(preNbr.Nid),hash_0(n.Successor.Nid),false)){
				n.Successor = preNbr
			}
		}
		var reply bool
		err = call(n.Successor.Naddr, "Peer.Notify",Neighbour{n.ID,n.Address},&reply)
		checkError(err)
		
		// y := n.Successor.SuccList
		succlist := make([]Neighbour,0,num_succ_list)
		err = call(n.Successor.Naddr, "Peer.Succs", 1, &succlist)
		if err != nil {
			if len(n.SuccList) == 0 {
				n.Successor = Neighbour{n.ID,n.Address}
			} else {
				n.SuccList = append(n.SuccList[:0], n.SuccList[1:]...)
				if len(n.SuccList) == 0{
					n.Successor = Neighbour{n.ID,n.Address}
				} else {
					n.Successor = n.SuccList[0]
				}
			}
		} else {
			n.SuccList =  append([]Neighbour{n.Successor}, succlist...)
			if (len(n.SuccList) > num_succ_list){
				n.SuccList = append(n.SuccList[:num_succ_list], n.SuccList[num_succ_list+1:]...)
			}
		}
	}
}

func (n *Peer) notify(nbr Neighbour) {

	if ( n.Predecessor.Nid == "" || between(hash_0(n.Predecessor.Nid),hash_0(nbr.Nid),hash_0(n.ID),false) ){
		n.Predecessor = nbr
	}
}

func (n *Peer) fix_fingers() {
	
	for {		
		n.Next = n.Next + 1
		if (n.Next > num_bits-1){
			time.Sleep(5 * time.Second)
			n.Next = 1
		}
		n.Finger[n.Next] = n.find_successor(jump(n.ID,n.Next))
	}
}


func (n *Peer) replicate_store() {
// my succ list should have my data all the time, replicate if not	
	for {		
		time.Sleep(2 * time.Second)

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
			err := call(succAddr.Naddr,"Peer.PutAll",new_map,&reply)
			checkError(err)
		}
	}
}

func (n *Peer) check_predecessor() {
	
	var reply int
	for {
		time.Sleep(1 * time.Second)
		if(call(n.Predecessor.Naddr,"Peer.Ping",1,&reply) != nil){
			n.Predecessor = Neighbour{}
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
	pre_id := hash_0(n.Predecessor.Nid)
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
	for _, succNbr := range n.SuccList{
		if succNbr.Nid != n.ID{
			err := call(succNbr.Naddr, "Peer.Put",req,&res)
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
	addrSet := make(map[string]struct{})
	for _, succNbr := range n.SuccList{
		if succNbr.Nid != n.ID{
			addrSet[succNbr.Naddr] = struct{}{}
		}
	}

	for succAddr := range addrSet {
		err := call(succAddr, "Peer.Delete",key,&res)
		checkError(err)
	}
	*reply = true
    return nil
}

func (n *Peer) Find(id *big.Int, nbr *Neighbour) error {
	*nbr = n.find_successor(id)
    return nil
}

func (n *Peer) Prev(dummy int, pre_nbr *Neighbour) error{
	*pre_nbr = n.Predecessor
	return nil
}

func (n *Peer) Succs(dummy int, succ_list *[]Neighbour) error {
    for _, succ := range n.SuccList {
        *succ_list = append(*succ_list,succ)
    }
	return nil
}

func (n *Peer) Notify(nbr Neighbour, reply *bool) error{
	n.notify(nbr)
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
	fmt.Println("- dump:			display information about the current node.")
	fmt.Println("- quit:			quit the program.")
	// fmt.Println("- join <addr>:		join an existing ring.")
	// fmt.Println("- create:		create a new ring.")
	// fmt.Println("- ping <addr>:		send ping to the address.")
	// fmt.Println("- put <key> <val>:	insert key and value into the ring.")
	// fmt.Println("- putrandom <n>:	insert randomly generated keys.")
	// fmt.Println("- get <key>:		find the given key in the ring.")
	// fmt.Println("- delete <key>:		delete key from the ring.")
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
	if globalNat{
		var reply bool
		_ = call(myRPCAddr,"Peer.Destruct",true,&reply)
	}
	err := fuse.Unmount(mountpoint)
	checkFatalError(err)			
	os.Exit(3)
}

func saveCredentials(uid string, pwd string) error {
	var user User
	// if _, err := os.Stat("user.json"); os.IsNotExist(err) {
	user = User{Uid: uid, Pwd: pwd}
	uJson, _ := json.Marshal(&user)
	err := ioutil.WriteFile("user.json", uJson, 0644)
		// fmt.Println("New User created")
		// fmt.Println("User logged in as: ",user.Uid)
	// } 
	// else {
	// 	ufile,err := os.Open(credentials)
	// 	checkFatalError(err)
	// 	defer ufile.Close()
	// 	b, _ := ioutil.ReadAll(ufile)
	// 	err = json.Unmarshal(b,&user)
	// 	checkFatalError(err)
	// 	fmt.Println("User logged in as: ",user.Uid)
	// }
	return err
}

func main() {
	rand.Seed(time.Now().UnixNano())
	gob.Register(Args{})
    gob.Register(map[string][]uint8{})
    gob.Register(big.NewInt(0))
    gob.Register(Neighbour{})

	mountpoint := flag.String("mnt", "/mnt/fmount", "folder to mount")
	uid := flag.String("uid", "default_login_uid", "user name")
	pwd := flag.String("pwd", "default_login_pwd", "user password")
	tracker := flag.String("trk", getLocalAddress()+":1234", "tracker adress")
	flag.Parse()
	
	trackerAddr = *tracker
	encrypt_key = *pwd
	err := saveCredentials(*uid,*pwd)
	checkFatalError(err)

	Root = new(Peer)
	Root.init(*uid)
	
	// get relayAddr from tracker
	err = call(trackerAddr,"Tracker.GetRelayAddr",true,&relayAddr)
	checkFatalError(err)

	printHelp()
	Root.create()
	go Root.stabilize()
	go Root.fix_fingers()
	go Root.check_predecessor()
	go FUSE(*mountpoint)
	go InterruptHandler(Root,*mountpoint)

	// get bootAddrs from tracker
	var bootAddresses map[string]struct{}
	err = call(trackerAddr,"Tracker.GetRootPeer",Root.Address,&bootAddresses)
	checkFatalError(err)
	for boot := range bootAddresses{
		if boot != Root.Address{
			err = Root.join(boot)
			if err == nil{
				break
			}
		}
	}

	go Root.replicate_store()

	for {
	
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		args := strings.Fields(input);
		
		if (len(args)> 0)  {
		if (args[0] == "quit")  {
			// send all files to succ
			var reply bool
			err := call(Root.Successor.Naddr,"Peer.PutAll",Root.get_all(Root.ID),&reply)
			checkError(err)
			// fmt.Println("response: ", reply)
			// save meta file
			err = os.RemoveAll(Root.ID+"_backup/")
			checkError(err)
			backupDir := Root.ID + "_backup/"
			if _, err := os.Stat(backupDir); os.IsNotExist(err) {
				os.Mkdir(backupDir, 0777)
			}
			for _, file := range *(FileSystem.root.files) {
				(*file).SaveMetaFile()
			}
			if globalNat{
				err = call(myRPCAddr,"Peer.Destruct",true,&reply)
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
			
			fmt.Println("________________________________________________________\n")
			fmt.Println("ID:\t\t",Root.ID)
			fmt.Println("Address:\t",Root.Address)
			fmt.Println("Predecessor:\t",Root.Predecessor.Nid)
			fmt.Println("Successor:\t",Root.Successor.Nid)
			// fmt.Println("Successor List:\t",Root.SuccList)
			fmt.Println("Key/Val Store:\t",Root.Store)
			fmt.Println("________________________________________________________")
		
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
		}}
	}
}