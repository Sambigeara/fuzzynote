package main

import (
	"log"
	"os"
	"path"
	//"runtime"
	"strings"

	"fuzzy-note/pkg/client"
	"fuzzy-note/pkg/service"
)

const (
	rootFileName   = "primary.db"
	walFilePattern = "wal_%v.db"
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

	os.Mkdir(rootDir, os.ModePerm)

	rootPath := path.Join(rootDir, rootFileName)
	walDirPattern := path.Join(rootDir, walFilePattern)

	wal := service.NewWal(walDirPattern)
	listRepo := service.NewDBListRepo(rootPath, service.NewDbEventLogger(), wal)

	// List instantiation
	err := listRepo.Load()
	if err != nil {
		log.Fatal(err)
		os.Exit(0)
	}

	// Set colourscheme
	fznColour := strings.ToLower(os.Getenv("FZN_COLOUR"))
	if fznColour == "" || (fznColour != "light" && fznColour != "dark") {
		fznColour = "light"
	}

	term := client.NewTerm(listRepo, fznColour)

	err = term.RunClient()
	if err != nil {
		log.Fatal(err)
	} else {
		err := listRepo.Save(listRepo.Root, listRepo.PendingDeletions, listRepo.NextID)
		if err != nil {
			log.Fatal(err)
		}
		os.Exit(0)
	}
}
