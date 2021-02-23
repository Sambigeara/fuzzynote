package main

import (
	"log"
	"os"
	"path"
	"strings"
	"time"

	"github.com/gdamore/tcell"

	"fuzzy-note/pkg/client"
	"fuzzy-note/pkg/service"
)

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

	// https://golang.org/pkg/time/#NewTicker
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	refresh := make(chan bool)

	// termCycle will receive tcell pollEvents and ticker refreshes to trigger a cycle of the main event loop
	// (and thus refresh the UI)
	termCycle := make(chan tcell.Event)

	go func() {
		for {
			select {
			case <-ticker.C:
				var listItem *service.ListItem
				listRepo.Refresh(listItem, nil)
				termCycle <- &client.RefreshKey{T: time.Now()}
			case <-refresh:
				ticker.Stop()
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
		refresh <- true
		os.Exit(0)
	}
}
