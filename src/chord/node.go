package main
import(
	"bazil.org/fuse"
	"time"
	"fmt"
	"strconv"
)

var blockIdentifier uint64 = 0
var dataBlockSize uint64 = 512
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

func sendBlock(data []byte, block uint64) {
	var reply bool
	encrypted_data := Encrypt(data, []byte(encrypt_key), int64(len(data)))
	req := Args{Root.Address + "|" + strconv.Itoa(int(block)),encrypted_data}
	id := hashString(req.Key)
	addr := Root.find_successor(id)
	err := call(addr, "Peer.Put",req,&reply)
	checkError(err)
	fmt.Println("response: ", reply)

}

func recvBlock(block uint64) ([]byte, error) {
	var reply []byte
	id := hashString(Root.Address + "|" + strconv.Itoa(int(block)))
	addr := Root.find_successor(id)
	err := call(addr, "Peer.Get",Root.Address + "|" + strconv.Itoa(int(block)),&reply)
	checkError(err)
	decrypted_data := Decrypt(reply, []byte(encrypt_key))
	return decrypted_data, nil
}

func deleteBlock(block uint64) {
	var reply bool
	id := hashString(Root.Address + "|" + strconv.Itoa(int(block)))
	addr := Root.find_successor(id)
	err := call(addr, "Peer.Delete",Root.Address + "|" + strconv.Itoa(int(block)),&reply)
	checkError(err)
	fmt.Println("response: ", reply)


}