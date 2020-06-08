package main

// TODO Will be path eventually, read from env var
const RootPath = "pages/root"

func main() {
	p := List{}
	p.BuildList(RootPath)
	p.HandleKeyPresses()
}
