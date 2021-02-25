package main

import (
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/gdamore/tcell"
	"github.com/rogpeppe/go-internal/lockedfile"

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

	listRepo := service.NewDBListRepo(rootDir)

	// List instantiation
	err := listRepo.Load()
	if err != nil {
		log.Fatal(err)
		os.Exit(0)
	}

	partialRefreshTicker := time.NewTicker(time.Millisecond)
	fullRefreshTicker := time.NewTicker(time.Second * 10)

	// termCycle will receive tcell pollEvents and ticker refreshes to trigger a cycle of the main event loop
	// (and thus refresh the UI)
	termCycle := make(chan tcell.Event)

	// We only need one fullRefreshTicker running between processes (if we have multiple locals running).
	go func() {
		refreshFileName := path.Join(rootDir, refreshFile)
		mutFile, err := lockedfile.Create(refreshFileName)
		if err != nil {
			log.Fatalf("Error creating wal refresh lock: %s\n", err)
		}
		defer mutFile.Close()
		go func() {
			for {
				select {
				case <-fullRefreshTicker.C:
					var listItem *service.ListItem
					listRepo.Refresh(listItem, nil, true)
					termCycle <- &client.RefreshKey{T: time.Now()}
				}
			}
		}()
	}()

	go func() {
		for {
			select {
			case <-partialRefreshTicker.C:
				var listItem *service.ListItem
				listRepo.Refresh(listItem, nil, false)
				termCycle <- &client.RefreshKey{T: time.Now()}
			}
		}
	}()

	// Set colourscheme
	fznColour := strings.ToLower(os.Getenv("FZN_COLOUR"))
	if fznColour == "" || (fznColour != "light" && fznColour != "dark") {
		fznColour = "light"
	}

	term := client.NewTerm(listRepo, fznColour)

	err = term.RunClient(termCycle)
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
}
