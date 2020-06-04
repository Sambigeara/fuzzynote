package main

// TODO Will be path eventually, read from env var
const RootPath = "pages/root"

func main() {
	db := DbRepo{}
	db.LoadRootPage(RootPath)

	scr := StdScr{}
	scr.HandleKeyPresses(db)
}
