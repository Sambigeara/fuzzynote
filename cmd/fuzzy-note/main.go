package main

import (
	"log"
	"os"
	"path"
	//"runtime"
	"strings"
	"time"

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

	localWalFile := service.NewLocalWalFile(rootDir)
	s3WalFile := service.NewS3FileWal(rootDir)
	walFiles := []service.WalFile{localWalFile, s3WalFile}
	//walFiles := []service.WalFile{localWalFile}

	listRepo := service.NewDBListRepo(rootDir, walFiles)

	// List instantiation
	for _, wf := range walFiles {
		err := listRepo.Load(wf)
		if err != nil {
			log.Fatal(err)
			os.Exit(0)
		}
	}

	//runtime.Breakpoint()
	//listRepo.Add("Hello", nil, 0)
	//listRepo.Refresh(localWalFile, nil, false)
	//listRepo.Refresh(s3WalFile, nil, false)

	partialRefreshTicker := time.NewTicker(time.Millisecond * 250)
	remoteRefreshTicker := time.NewTicker(time.Millisecond * 500)
	fullRefreshTicker := time.NewTicker(time.Second * 60)

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
				err := listRepo.Refresh(localWalFile, nil, false)
				if err != nil {
					log.Fatalf("Failed on partial sync: %v", err)
					return
				}
				term.S.PostEvent(&client.RefreshKey{})
			case <-remoteRefreshTicker.C:
				// TODO can remote refresh run in an entirely independent loop?
				err := listRepo.Refresh(s3WalFile, nil, false)
				if err != nil {
					log.Fatalf("Failed on remote sync: %v", err)
					return
				}
			case <-fullRefreshTicker.C:
				for _, wf := range walFiles {
					err := listRepo.Refresh(wf, nil, true)
					if err != nil {
						log.Fatalf("Failed on full sync: %v", err)
						return
					}
				}
				term.S.PostEvent(&client.RefreshKey{})
			case ev := <-keyPressEvts:
				cont, err := term.HandleKeyEvent(ev)
				if err != nil {
					log.Fatal(err)
				} else if !cont {
					for _, wf := range walFiles {
						err := listRepo.Save(wf)
						if err != nil {
							log.Fatal(err)
						}
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
