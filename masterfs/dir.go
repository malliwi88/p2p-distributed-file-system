package main

import (
	"log"
	"os"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context" // need this cause bazil lib doesn't use syslib context lib
)

// Dir implements both Node and Handle for the root directory.
type Dir struct{
	Node
	files       *[]*File
	directories *[]*Dir
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Inode = d.Attributes.Inode
	a.Mode = os.ModeDir | 0555
	return nil
}

func (d *Dir) Lookup(ctx context.Context, Name string) (fs.Node, error) { //** find command **//
	log.Println("Requested lookup for", Name)
	if d.files != nil {
		for _, n := range *d.files {
			if n.Name == Name {
				return n, nil
			}
		}
	}
	if d.directories != nil {
		for _, n := range *d.directories {
			if n.Name == Name {
				return n, nil
			}
		}
	}
	return nil, fuse.ENOENT
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	log.Println("Create request for Name", req.Name)
	f := &File{Node: Node{Name: req.Name}}
	f.DataNodes = make(map[uint64][]*OneBlockInfo)
	f.Replicas = 2		// number of replicas under user's control
	f.InitNode()
	if d.files != nil {
		(*d.files) = append(*d.files, f)
	}
	return f, f, nil
}
