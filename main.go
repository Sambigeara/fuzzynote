package main

// TODO Will be path eventually, read from env var
const rootPath = "pages/root"

func main() {
	//ImportLines()

	p := List{
		RootPath: rootPath,
	}
	p.BuildList()
	p.HandleKeyPresses()
	// TODO this should be handled here gracefully (rather than in loop as current)
	//p.StoreList(RootPath)
}
