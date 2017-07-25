package main
import "bazil.org/fuse/fs"

// FS implements the hello world file system.
type FS struct{
	root *Dir
}

func (f *FS) Root() (fs.Node, error) {
	return f.root, nil
}
