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
	walDirPattern := path.Join(rootDir, walFilePattern)

	// TODO remove
	// Create (if not exists) the notes subdirectory
	os.MkdirAll(notesDir, os.ModePerm)

	walEventLogger := service.NewWalEventLogger()
	walFile := service.NewWalFile(rootPath, walDirPattern, walEventLogger)
	fileDS := service.NewFileDataStore(rootPath, notesDir, walFile)
	listRepo := service.NewDBListRepo(service.NewDbEventLogger(), walEventLogger)

	//runtime.Breakpoint()
	// List instantiation
	err := fileDS.Load(listRepo)
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
		err := fileDS.Save(listRepo.Root, listRepo.PendingDeletions, listRepo.NextID)
		if err != nil {
			log.Fatal(err)
		}
		os.Exit(0)
	}

	//listRepo.Add("H", nil, nil, nil)
	//listRepo.Add("e", nil, listRepo.Root, nil)
	//matches, _ := listRepo.Match([][]rune{}, nil, false)
	//runtime.Breakpoint()
	//fileDS.Save(listRepo.Root, listRepo.PendingDeletions, listRepo.NextID)

	//_, walEventLog, nextID, _ = fileDS.Load()
	//listRepo = service.NewDBListRepo(nil, nextID, service.NewDbEventLogger(), walEventLogger)
	//listRepo.ReplayWalEvents(walEventLog)

	//matches, _ = listRepo.Match([][]rune{}, nil, false)
	//listRepo.Delete(matches[len(matches)-1])
	//err = fileDS.Save(listRepo.Root, listRepo.PendingDeletions, listRepo.NextID)
}
