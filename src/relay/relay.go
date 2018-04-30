package main

import (
	"net/rpc"
	"net"
	"fmt"
	"strconv"
	"os"
	"crypto/rand"
	"math/big"
    "encoding/gob"
    // "time"
    // "io"
)

// var RelayServer *Relay
var maxClients = 2000
var Dict map[string]*DictValue

type Relay struct {
    id string
}

type Peer struct{
    id string
}

type LiveMsg struct{
    Command string
    Req interface{}
}

type Args struct {
    Key string
    Val []byte
}

type Neighbour struct {
    Nid string
    Naddr string
}

type DictValue struct {
    Conn        net.Conn
    Encoder     *gob.Encoder
    Decoder     *gob.Decoder
    Listener    net.Listener
}


func raw_input(txt string) string{
    var input string
    fmt.Print(txt)
    fmt.Scanln(&input)
    return input
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
        panic("init: failed to find non-loopback interface with valid address on r node")
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


func (n *Peer) Ping(arg int, reply *int) error {
    msg := &LiveMsg{
        Command: "Ping", Req: arg}
    encoder := Dict[n.id].Encoder
    decoder := Dict[n.id].Decoder
    err := encoder.Encode(msg)
    var res int
    err = decoder.Decode(&res)
    *reply = res
    // fmt.Println(err)
    return err
}

func (n *Peer) Get(key string, value *[]byte) error {
    msg := &LiveMsg{
        Command: "Get", Req: key}
    encoder := Dict[n.id].Encoder
    decoder := Dict[n.id].Decoder
    err := encoder.Encode(msg)
    var res []byte
    err = decoder.Decode(&res)
    *value = res
    // fmt.Println(err)
    return err
}

func (n *Peer) GetAll(addr string, reply *map[string][]byte) error {
    msg := &LiveMsg{
        Command: "GetAll", Req: addr}
    encoder := Dict[n.id].Encoder
    decoder := Dict[n.id].Decoder
    err := encoder.Encode(msg)
    var res map[string][]byte
    err = decoder.Decode(&res)
    *reply = res
    // fmt.Println(err)
    return err
}

func (n *Peer) Put(pair Args, reply *bool) error {
    msg := &LiveMsg{
        Command: "Put", Req: pair}
    encoder := Dict[n.id].Encoder
    decoder := Dict[n.id].Decoder
    err := encoder.Encode(msg)
    var res bool
    err = decoder.Decode(&res)
    *reply = res
    // fmt.Println(err)
    return err
}

func (n *Peer) Replicate(pair Args, reply *bool) error {
    msg := &LiveMsg{
        Command: "Replicate", Req: pair}
    encoder := Dict[n.id].Encoder
    decoder := Dict[n.id].Decoder
    err := encoder.Encode(msg)
    var res bool
    err = decoder.Decode(&res)
    *reply = res
    // fmt.Println(err)
    return err
}

func (n *Peer) PutAll(bucket map[string][]byte, reply *bool) error {
    msg := &LiveMsg{
        Command: "PutAll", Req: bucket}
    encoder := Dict[n.id].Encoder
    decoder := Dict[n.id].Decoder
    err := encoder.Encode(msg)
    var res bool
    err = decoder.Decode(&res)
    *reply = res
    // fmt.Println(err)
    return err
}

func (n *Peer)  Delete(key string, reply *bool) error {
    msg := &LiveMsg{
        Command: "Delete", Req: key}
    encoder := Dict[n.id].Encoder
    decoder := Dict[n.id].Decoder
    err := encoder.Encode(msg)
    var res bool
    err = decoder.Decode(&res)
    *reply = res
    // fmt.Println(err)
    return err
}

func (n *Peer) Dereplicate(key string, reply *bool) error {
    msg := &LiveMsg{
        Command: "Dereplicate", Req: key}
    encoder := Dict[n.id].Encoder
    decoder := Dict[n.id].Decoder
    err := encoder.Encode(msg)
    var res bool
    err = decoder.Decode(&res)
    *reply = res
    // fmt.Println(err)
    return err
}

func (n *Peer) Find(id *big.Int, nbr *Neighbour) error {
	msg := &LiveMsg{
       	Command: "Find", Req: id}
    encoder := Dict[n.id].Encoder
    decoder := Dict[n.id].Decoder
	err := encoder.Encode(msg)
    var res Neighbour
    err = decoder.Decode(&res)
	*nbr = res
    // fmt.Println(err)
    return err
}

func (n *Peer) Prev(dummy int, pre_nbr *Neighbour) error{
    msg := &LiveMsg{
        Command: "Prev", Req: dummy}
    encoder := Dict[n.id].Encoder
    decoder := Dict[n.id].Decoder
    err := encoder.Encode(msg)
    var res Neighbour
    err = decoder.Decode(&res)
    *pre_nbr = res
    // fmt.Println(err)
    return err
}

func (n *Peer) Succs(dummy int, succ_list *[]Neighbour) error {
    msg := &LiveMsg{
        Command: "Succs", Req: dummy}
    encoder := Dict[n.id].Encoder
    decoder := Dict[n.id].Decoder
    err := encoder.Encode(msg)
    var res []Neighbour
    err = decoder.Decode(&res)
    *succ_list = res
    // fmt.Println(err)
    return err
}

func (n *Peer) Notify(nbr Neighbour, reply *bool) error{
    msg := &LiveMsg{
        Command: "Notify", Req: nbr}
    encoder := Dict[n.id].Encoder
    decoder := Dict[n.id].Decoder
    err := encoder.Encode(msg)
    var res bool
    err = decoder.Decode(&res)
    *reply = res
    // fmt.Println(err)
    return err
}

func (n *Peer) Destruct(req bool, reply *bool) error{
    fmt.Println("Destruct called by: ",n.id)
    Dict[n.id].Conn.Close() // live conn closed
    Dict[n.id].Listener.Close() // live conn closed
    delete(Dict,n.id)
    return nil
}


func (r *Relay) run(address string) net.Listener{
	handler := rpc.NewServer()
	p := &Peer{address}
	handler.Register(p)
	ln, err := net.Listen("tcp", address)
	if err != nil {
		fmt.Printf("listen(%q): %s\n", address, err)
	}
	fmt.Printf("Relay %s listening on %s\n", r.id, ln.Addr())
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				fmt.Printf("listen(%q): %s\n", address, err)
				return
			}
			go handler.ServeConn(conn)
		}
	}()
    return ln
}


func getRandPort() int64 {
    // calculate the max we will be using
    var min int64
    var max int64
    min = 2000
    max = 60000
    bg := big.NewInt(max - min)
    n, err := rand.Int(rand.Reader, bg)
    if err != nil {
        panic(err)
    }
    return n.Int64() + min
}

// regular peer call
func call(address string, method string, req interface{}, reply *interface{}) error {
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

func main() {
    gob.Register(Args{})
    gob.Register(map[string][]uint8{})
    gob.Register(big.NewInt(0))
    gob.Register(Neighbour{})

	mainAddr := getLocalAddress()+":"+strconv.Itoa(5555)
	tcpAddr, err := net.ResolveTCPAddr("tcp", mainAddr)
    checkError(err)
    fmt.Println("Relay serving on: ",mainAddr)
    listener, err := net.ListenTCP("tcp", tcpAddr)
    checkError(err)
    Dict = make(map[string]*DictValue)
    for {
        conn, err := listener.Accept()
		fmt.Printf("Relay accepted connection to %s from %s\n", conn.LocalAddr(), conn.RemoteAddr())

        if err != nil {
            continue
        }

        go handler(conn)
    }
}

func handler(conn net.Conn){
    encoder := gob.NewEncoder(conn)
	decoder := gob.NewDecoder(conn)
	addr := getLocalAddress()+":"+strconv.FormatInt(getRandPort(),10)
    r := Relay{addr}
	ln := r.run(addr)
	encoder.Encode(addr)
    Dict[r.id] = &DictValue{conn, encoder, decoder,ln}   
}

