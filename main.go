package main

import (
	"time"
)

// TODO Will be path eventually, read from env var
const RootPath = "rootPage"

type PageItem struct {
	Line      string
	DtCreated time.Time
}

func main() {
	db := DbRepo{}
	db.LoadRootPage(RootPath)

	scr := StdScr{}
	scr.HandleKeyPresses(db)
}
