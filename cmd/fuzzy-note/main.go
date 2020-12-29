package main

import (
	"log"
	"os"
	"path"
	"strings"

	"fuzzy-note/pkg/client"
	"fuzzy-note/pkg/service"
	//"github.com/Sambigeara/fuzzy-note/pkg/client"
	//"github.com/Sambigeara/fuzzy-note/pkg/service"
)

const (
	rootFileName   = "primary.db"
	walFilePattern = "wal_%d.db"
)

func main() {
	var rootDir, notesSubDir string
	if rootDir = os.Getenv("FZN_ROOT_DIR"); rootDir == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}
		rootDir = path.Join(home, ".fzn/")
	}
	// TODO remove
	if notesSubDir = os.Getenv("FZN_NOTES_SUBDIR"); notesSubDir == "" {
		notesSubDir = "notes"
	}

	rootPath := path.Join(rootDir, rootFileName)
	notesDir := path.Join(rootDir, notesSubDir)
	walDir := path.Join(rootDir, walFilePattern)

	// TODO remove
	// Create (if not exists) the notes subdirectory
	os.MkdirAll(notesDir, os.ModePerm)

	walFile := service.NewWalFile(rootPath, walDir)
	fileDS := service.NewFileDataStore(rootPath, notesDir, walFile)

	// List instantiation
	root, nextID, err := fileDS.Load()
	if err != nil {
		log.Fatal(err)
		os.Exit(0)
	}

	// Get DbEventLogger
	eventLogger := service.NewDbEventLogger()

	listRepo := service.NewDBListRepo(root, nextID, eventLogger)

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
		err := fileDS.Save(listRepo.Root, listRepo.PendingDeletions, listRepo.NextID)
		if err != nil {
			log.Fatal(err)
		}
		os.Exit(0)
	}
}
