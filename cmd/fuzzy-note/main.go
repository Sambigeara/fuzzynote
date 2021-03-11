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
	// Specify root directory for local instance
	var rootDir string
	var ok bool
	if rootDir, ok = os.LookupEnv("FZN_ROOT_DIR"); !ok {
		home, err := os.UserHomeDir()
		if err != nil {
			log.Fatal(err)
		}
		rootDir = path.Join(home, ".fzn/")
	}
	// Make sure the root directory exists
	os.Mkdir(rootDir, os.ModePerm)

	// Instantiate listRepo
	listRepo := service.NewDBListRepo(rootDir)

	localWalFiles := []service.WalFile{service.NewLocalWalFile(rootDir)}
	remoteWalFiles := []service.WalFile{}

	if s3Prefix := os.Getenv("FZN_REMOTE_PREFIX"); s3Prefix != "" {
		remoteWalFiles = append(remoteWalFiles, service.NewS3FileWal(s3Prefix, rootDir))
	}
	allWalFiles := append(localWalFiles, remoteWalFiles...)

	// List instantiation
	//runtime.Breakpoint()
	err := listRepo.Load(allWalFiles)
	if err != nil {
		log.Fatal(err)
		os.Exit(0)
	}

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
				err := listRepo.Refresh(localWalFiles, nil, false)
				if err != nil {
					log.Fatalf("Failed on partial sync: %v", err)
				}
				term.S.PostEvent(&client.RefreshKey{})
			case <-remoteRefreshTicker.C:
				err := listRepo.Refresh(remoteWalFiles, nil, false)
				if err != nil {
					log.Fatalf("Failed on remote sync: %v", err)
				}
			case <-fullRefreshTicker.C:
				err := listRepo.Refresh(allWalFiles, nil, true)
				if err != nil {
					log.Fatalf("Failed on full sync: %v", err)
				}
				term.S.PostEvent(&client.RefreshKey{})
			case ev := <-keyPressEvts:
				cont, err := term.HandleKeyEvent(ev)
				if err != nil {
					log.Fatal(err)
				} else if !cont {
					err := listRepo.Save(allWalFiles)
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
}
