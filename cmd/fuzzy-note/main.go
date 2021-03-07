package main

import (
	"log"
	"os"
	"path"
	"strings"
	"time"
	//"runtime"

	"github.com/gdamore/tcell"
	//"github.com/rogpeppe/go-internal/lockedfile"

	"fuzzy-note/pkg/client"
	"fuzzy-note/pkg/service"
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
	remoteRefreshTicker := time.NewTicker(time.Second * 5)
	fullRefreshTicker := time.NewTicker(time.Second * 60)

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

	// Set colourscheme
	fznColour := strings.ToLower(os.Getenv("FZN_COLOUR"))
	if fznColour == "" || (fznColour != "light" && fznColour != "dark") {
		fznColour = "light"
	}

	term := client.NewTerm(listRepo, fznColour)

	// We retrieve keypresses from tcell through a blocking function call which can't be used
	// in the main select below

	keyPressEvts := make(chan tcell.Event)
	go func() {
		for {
			select {
			case <-partialRefreshTicker.C:
				var listItem *service.ListItem
				err := listRepo.Refresh(listItem, false, false)
				if err != nil {
					log.Fatalf("Failed on partial sync: %v", err)
					return
				}
				term.S.PostEvent(&client.RefreshKey{})
			case <-remoteRefreshTicker.C:
				var listItem *service.ListItem
				err := listRepo.Refresh(listItem, false, true)
				if err != nil {
					log.Fatalf("Failed on partial sync: %v", err)
					return
				}
				term.S.PostEvent(&client.RefreshKey{})
			case <-fullRefreshTicker.C:
				var listItem *service.ListItem
				err := listRepo.Refresh(listItem, true, true)
				if err != nil {
					log.Fatalf("Failed on full sync: %v", err)
					return
				}
				term.S.PostEvent(&client.RefreshKey{})
			case ev := <-keyPressEvts:
				cont, err := term.HandleKeyEvent(ev)
				if err != nil {
					log.Fatal(err)
				} else if !cont {
					err := listRepo.Save()
					if err != nil {
						log.Fatal(err)
					}
					partialRefreshTicker.Stop()
					fullRefreshTicker.Stop()
					os.Exit(0)
				}
			}
		}
	}()

	for {
		keyPressEvts <- term.S.PollEvent()
	}

	//listRepo.Save()
	//listRepo.Load()
	//listRepo.Save()
	//os.Exit(0)

	//runtime.Breakpoint()
	//s3WalFile := service.NewS3FileWal()
	//fileNames, _ := s3WalFile.GetFileNamesMatchingPattern("foo")
	//for _, fileName := range fileNames {
	//    wal, _ := s3WalFile.GenerateLogFromFile(fileName)
	//    fmt.Println(wal)
	//    s3WalFile.RemoveFile(fileName)
	//    s3WalFile.Flush(fileName, &wal)
	//}
}
