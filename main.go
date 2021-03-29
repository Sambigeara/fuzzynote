package main

import "os"

func main() {
	//ImportLines()

	var rootPath string
	if rootPath = os.Getenv("ROOT_PAGE"); rootPath == "" {
		rootPath = "pages/root"
	}

	p := List{
		RootPath: rootPath,
	}
	p.BuildList()
	p.HandleKeyPresses()
	// TODO this should be handled here gracefully (rather than in loop as current)
	//p.StoreList(RootPath)
}
