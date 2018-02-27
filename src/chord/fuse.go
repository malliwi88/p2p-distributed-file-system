package main

import (
	"log"
	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"os"
	"io/ioutil"
	"encoding/json"
)

var FileSystem *FS

type Meta struct {
	Name string
	Attributes fuse.Attr
	DataNodes map[uint64][]*OneBlockInfo
	Replicas int
}


func FUSE(mountpoint string) {

	backupDir := Root.Address + "_backup/"
	// load meta data
	if _, err := os.Stat(backupDir); os.IsNotExist(err) {
		os.Mkdir(backupDir, 0777)
	}
	files, err := ioutil.ReadDir(backupDir)
	if err != nil {
		log.Fatal(err)
	}
	meta := Meta{}
	fileArray := []*File{}
	for _, file := range files {
		content, err := ioutil.ReadFile(backupDir + file.Name())
		if err != nil {
			log.Fatal(err)
		}
		meta = Meta{}
    	json.Unmarshal(content, &meta)
    	filemeta := File{}
    	filemeta.Node.Name = meta.Name
    	filemeta.DataNodes = meta.DataNodes
    	filemeta.Node.Attributes = meta.Attributes
    	filemeta.Replicas = meta.Replicas
		fileArray = append(fileArray, &filemeta)
	}

	fuse.Unmount(mountpoint)
	c, err := fuse.Mount(mountpoint)
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	FileSystem = new(FS)
	FileSystem.root = new(Dir)
	FileSystem.root.directories = &[]*Dir{}
	FileSystem.root.files = &fileArray
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


