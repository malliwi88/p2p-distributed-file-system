package main

import (
	"log"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)


func FUSE(mountpoint string) {
	fuse.Unmount(mountpoint)
	c, err := fuse.Mount(mountpoint)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	FileSystem := new(FS)
	FileSystem.root = new(Dir)
	FileSystem.root.directories = &[]*Dir{}
	FileSystem.root.files = &[]*File{}
	FileSystem.root.InitNode()
	err = fs.Serve(c, FileSystem)
	if err != nil {
		log.Fatal(err)
	}
	// check if the mount process has an error to report
	<-c.Ready
	if err := c.MountError; err != nil {
		log.Fatal(err)
	}
}


