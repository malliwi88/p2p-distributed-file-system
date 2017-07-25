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
	a.Inode = d.inode
	a.Mode = os.ModeDir | 0555
	return nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) { //** find command **//
	log.Println("Requested lookup for ", name)
	if d.files != nil {
		for _, n := range *d.files {
			if n.name == name {
				return n, nil
			}
		}
	}
	if d.directories != nil {
		for _, n := range *d.directories {
			if n.name == name {
				return n, nil
			}
		}
	}
	return nil, fuse.ENOENT
}

func (d *Dir) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	log.Println("Create request for name", req.Name)
	f := &File{Node: Node{name: req.Name, inode: NewInode()}}
	if d.files != nil {
		(*d.files) = append(*d.files, f)
	}
	return f, f, nil
}

func (d *Dir) Mkdir(ctx context.Context, req *fuse.MkdirRequest) (fs.Node, error) {
	log.Println("Mkdir request for name", req.Name)
	dir := &Dir{Node: Node{name: req.Name, inode: NewInode()}}
	if d.directories != nil {
	 	(*d.directories) = append(*d.directories, dir)
	}
	return dir, nil
}
