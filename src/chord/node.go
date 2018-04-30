package main
import(
	"bazil.org/fuse"
	"time"
	"path/filepath"
	"io/ioutil"
	"os"
	"fmt"
	"math/big"
	"math/rand"
	"strconv"
	// "golang.org/x/net/context"
	"errors"
)

var blockIdentifier uint64 = 0
var dataBlockSize uint64 = 4096
var inode uint64 = 0

func NewInode() uint64 {	
	inode += 1
	return inode
}

type OneBlockInfo struct {
	Name uint64
	Used bool
}

type Node struct {
	Name string
	Attributes fuse.Attr
}

// func (n *Node) Attr(ctx context.Context, attr *fuse.Attr) error {
// 	return nil
// }

func (n *Node) InitNode() {	
	t := time.Now()
	n.Attributes.Inode = NewInode()      
    n.Attributes.Size = 0      			
    n.Attributes.Blocks = 0      		
	n.Attributes.Atime = t
	n.Attributes.Mtime = t
	n.Attributes.Ctime = t
	n.Attributes.Crtime = t
	n.Attributes.Mode = 0644 
	n.Attributes.Nlink = 0
	n.Attributes.Uid = 0
	n.Attributes.Gid = 0
	n.Attributes.Rdev = 0
	n.Attributes.BlockSize = uint32(dataBlockSize) // block size
}

func Offset2Block (value uint64) uint64 {
	return (value / dataBlockSize)
}

func Blocks(value uint64) uint64 { // Blocks returns the number of 512 byte blocks required
	if value == 0 {
		return 0
	}
	blocks := value / dataBlockSize
	if value%dataBlockSize > 0 {
		return blocks + 1
	}
	return blocks
}

func BlockCheck(offsetBlock uint64, dataNodes *map[uint64][]*OneBlockInfo, buffer []byte, numReplicas *int) {	
	if offsetBlock < uint64(len(*dataNodes)) {
		if (*dataNodes)[offsetBlock][0].Used {
			for j := 0; j < len((*dataNodes)[offsetBlock]); j++ {
				sendBlock(buffer, (*dataNodes)[offsetBlock][j].Name)
			}
		} else {
			name := (*dataNodes)[offsetBlock][0].Name
			(*dataNodes)[offsetBlock] = make([]*OneBlockInfo, 0, *numReplicas)
			for j := 0; j < *numReplicas; j++ {
				singleBlock := &OneBlockInfo{name, true}
				(*dataNodes)[offsetBlock] = append((*dataNodes)[offsetBlock], singleBlock)
				sendBlock(buffer, (*singleBlock).Name)
			}
		}
	} else {
		for j := 0; j < *numReplicas; j++ {
			singleBlock := &OneBlockInfo{blockIdentifier, true}
			if _, ok := (*dataNodes)[offsetBlock]; !ok {
				(*dataNodes)[offsetBlock] = make([]*OneBlockInfo, 0, *numReplicas)
			}
			(*dataNodes)[offsetBlock] = append((*dataNodes)[offsetBlock], singleBlock)
			sendBlock(buffer, (*singleBlock).Name)
		}
		blockIdentifier++
	}
}

type Load struct {
    respTime time.Duration
    address string
}

func getLoad(id *big.Int, c chan Load) {
	startTime := time.Now()

	r := rand.Intn(100)
	time.Sleep(time.Duration(r) * time.Millisecond)
	
	var reply int
	addr := Root.find_successor(id)
	err := call(addr.Naddr, "Peer.Ping",1,&reply)
	checkError(err)
	elapsedTime := (time.Now()).Sub(startTime)
	resp := new(Load)
	resp.respTime = elapsedTime
	resp.address = addr.Naddr 
	c <- *resp
}


func sendBlock(data []byte, block uint64) {	
	// get load in parallel
	id1 := hash_1(Root.ID + "|" + strconv.Itoa(int(block)))
	id2 := hash_2(Root.ID + "|" + strconv.Itoa(int(block)))
	id3 := hash_3(Root.ID + "|" + strconv.Itoa(int(block)))
	c1 := make(chan Load)
	c2 := make(chan Load)
	c3 := make(chan Load)
	go getLoad(id1, c1)
	go getLoad(id2, c2)
	go getLoad(id3, c3)
	x, y, z := <-c1, <-c2, <-c3
	var minLoadPeer string
	if x.respTime <= y.respTime && x.respTime <= z.respTime {
		minLoadPeer = x.address
	} else if y.respTime <= x.respTime && y.respTime <= z.respTime {
		minLoadPeer = y.address
	}else if z.respTime <= x.respTime && z.respTime <= y.respTime {
		minLoadPeer = z.address
	}
	fmt.Println("Original Key Holder: ",minLoadPeer)
	encrypted_data := Encrypt(data, []byte(encrypt_key), int64(len(data)))
	req := Args{Root.ID + "|" + strconv.Itoa(int(block)),encrypted_data}
	var reply bool
	err := call(minLoadPeer, "Peer.Put",req,&reply)
	checkError(err)
	// send replica
	go call(minLoadPeer, "Peer.Replicate",req,&reply)

}

type ValidData struct {
    invalid error
    data []byte
    addr string
}

func getFromPeer(addr string,block uint64, c chan ValidData){
	var reply []byte
	err := call(addr, "Peer.Get",Root.ID + "|" + strconv.Itoa(int(block)),&reply)
	resp := new(ValidData)
	resp.invalid = err	
	resp.data = reply
	resp.addr = addr	
	c <- *resp
}

func recvBlock(block uint64) ([]byte, error) {
	id1 := hash_1(Root.ID + "|" + strconv.Itoa(int(block)))
	id2 := hash_2(Root.ID + "|" + strconv.Itoa(int(block)))
	id3 := hash_3(Root.ID + "|" + strconv.Itoa(int(block)))
	addr1 := Root.find_successor(id1).Naddr
	addr2 := Root.find_successor(id2).Naddr
	addr3 := Root.find_successor(id3).Naddr
	
	addrSet := make(map[string]struct{})
	addrSet[addr1] = struct{}{}
	addrSet[addr2] = struct{}{}
	addrSet[addr3] = struct{}{}
	c := make(chan ValidData)
	for key := range addrSet{
		go getFromPeer(key,block,c)
	}
	list := make([]ValidData,3)
	list[0] = ValidData{invalid:errors.New("")}
	list[1] = ValidData{invalid:errors.New("")}
	list[2] = ValidData{invalid:errors.New("")}
	for i := 0; i < len(addrSet); i++ {
		list[i] = <-c 
	}
	x, y , z := list[0], list[1], list[2]

	var encoded_data []byte
	if x.invalid == nil{
		encoded_data = x.data
	} else if y.invalid == nil{
		encoded_data = y.data
	} else {
		encoded_data = z.data
	}
	
	decrypted_data := Decrypt(encoded_data, []byte(encrypt_key))
	return decrypted_data, nil
}


func deleteBlock(block uint64) {
	id1 := hash_1(Root.ID + "|" + strconv.Itoa(int(block)))
	id2 := hash_2(Root.ID + "|" + strconv.Itoa(int(block)))
	id3 := hash_3(Root.ID + "|" + strconv.Itoa(int(block)))

	addr1 := Root.find_successor(id1).Naddr
	addr2 := Root.find_successor(id2).Naddr
	addr3 := Root.find_successor(id3).Naddr
	
	addrSet := make(map[string]struct{})
	addrSet[addr1] = struct{}{}
	addrSet[addr2] = struct{}{}
	addrSet[addr3] = struct{}{}
	c := make(chan ValidData)
	for key := range addrSet{
		go getFromPeer(key,block,c)
	}

	list := make([]ValidData,3)
	list[0] = ValidData{invalid:errors.New("")}
	list[1] = ValidData{invalid:errors.New("")}
	list[2] = ValidData{invalid:errors.New("")}
	for i := 0; i < len(addrSet); i++ {
		list[i] = <-c 
	}
	x, y, z := list[0], list[1], list[2]

	var addr string
	if x.invalid == nil{
		addr = x.addr
	} else if y.invalid == nil{
		addr = y.addr
	} else {
		addr = z.addr
	}
	var reply bool
	err := call(addr, "Peer.Delete",Root.ID + "|" + strconv.Itoa(int(block)),&reply)
	checkError(err)

	go call(addr, "Peer.Dereplicate",Root.ID + "|" + strconv.Itoa(int(block)),&reply)

}

func writeToDisk(peerAddr string, blockName string, data []byte) {
	if _, err := os.Stat(peerAddr); os.IsNotExist(err) {
		os.Mkdir(peerAddr, 0777)
	}
	f, err := os.Create(filepath.Join(peerAddr, blockName))
	checkError(err)
	f.Chmod(0777)
	_, err = f.WriteString(string(data))
	// fmt.Printf("Wrote %d bytes to file: %s \n", b, blockName)
	f.Sync()
	f.Close()
}

func deleteFromDisk(peerAddr string, blockName string) error {
	err := os.Remove(filepath.Join(peerAddr, blockName))
	// if err == nil{
	// 	fmt.Printf("Removing file: %s \n", blockName)
	// }
	return err
}

func readFromDisk(peerAddr string, blockName string) ([]byte,error) {
	dat, err := ioutil.ReadFile(filepath.Join(peerAddr, blockName))
    // if err == nil{
    // 	fmt.Printf("Sending file: %s \n", blockName)
    // }
    return dat, err
}