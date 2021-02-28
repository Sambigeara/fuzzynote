package main

import (
	"log"
	"os"
	"path"
	"strings"
	"time"

	//"github.com/rogpeppe/go-internal/lockedfile"

	"fuzzy-note/pkg/client"
	"fuzzy-note/pkg/service"
	//"runtime"
)

const refreshFile = "_refresh_lock.db"

func main() {
	var rootDir string
	if rootDir = os.Getenv("FZN_ROOT_DIR"); rootDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}
		rootDir = path.Join(home, ".fzn/")
	}

	// Make sure the root directory exists
	os.Mkdir(rootDir, os.ModePerm)

	//runtime.Breakpoint()
	listRepo := service.NewDBListRepo(rootDir)

	// List instantiation
	err := listRepo.Load()
	if err != nil {
		log.Fatal(err)
		os.Exit(0)
	}

	partialRefreshTicker := time.NewTicker(time.Millisecond * 500)
	fullRefreshTicker := time.NewTicker(time.Second * 60)

	// termCycle will receive tcell pollEvents and ticker refreshes to trigger a cycle of the main event loop
	// (and thus refresh the UI)
	//termCycle := make(chan tcell.Event)
	//cursorEvents := make(chan *client.OffsetKey)

	// We only need one fullRefreshTicker running between processes (if we have multiple locals running).
	//go func() {
	//    refreshFileName := path.Join(rootDir, refreshFile)
	//    mutFile, err := lockedfile.Create(refreshFileName)
	//    if err != nil {
	//        log.Fatalf("Error creating wal refresh lock: %s\n", err)
	//    }
	//    defer mutFile.Close()
	//    go func() {
	//        for {
	//            select {
	//            case <-fullRefreshTicker.C:
	//                var listItem *service.ListItem
	//                listRepo.Refresh(listItem, nil, true)
	//                cursorEvents <- &client.OffsetKey{}
	//            }
	//        }
	//    }()
	//}()

	//listRepo.ProcessEvents()

	cursorEvents := make(chan *client.OffsetKey)

	go func() {
		for {
			select {
			case el := <-listRepo.EventQueue:
				var err error
				listRepo.AddLog(*el)
				listRepo.Root, _, err = listRepo.CallFunctionForEventLog(listRepo.Root, *el)
				if err != nil {
					return
				}
			case <-partialRefreshTicker.C:
				var listItem *service.ListItem
				listRepo.Refresh(listItem, nil, false)
				cursorEvents <- &client.OffsetKey{}
			case <-fullRefreshTicker.C:
				var listItem *service.ListItem
				listRepo.Refresh(listItem, nil, true)
				cursorEvents <- &client.OffsetKey{}
			}
		}
	}()

	// Set colourscheme
	fznColour := strings.ToLower(os.Getenv("FZN_COLOUR"))
	if fznColour == "" || (fznColour != "light" && fznColour != "dark") {
		fznColour = "light"
	}

	term := client.NewTerm(listRepo, fznColour)

	err = term.RunClient(cursorEvents)
	if err != nil {
		log.Fatal(err)
	} else {
		err := listRepo.Save()
		if err != nil {
			log.Fatal(err)
		}
		partialRefreshTicker.Stop()
		fullRefreshTicker.Stop()
		os.Exit(0)
	}

	//listRepo.Save()
	//listRepo.Load()
	//listRepo.Save()
	//os.Exit(0)
}
