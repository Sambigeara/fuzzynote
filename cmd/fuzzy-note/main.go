package main

import (
	"log"
	"os"
	"path"

	"fuzzy-note/pkg/client"
	"fuzzy-note/pkg/service"
	//"github.com/Sambigeara/fuzzy-note/pkg/client"
	//"github.com/Sambigeara/fuzzy-note/pkg/service"
)

const rootFileName = "primary.db"

func main() {
	var rootDir, notesSubDir string
	if rootDir = os.Getenv("FZN_ROOT_DIR"); rootDir == "" {
		// TODO currently only works on OSs with HOME
		rootDir = path.Join(os.Getenv("HOME"), ".fzn/")
	}
	if notesSubDir = os.Getenv("FZN_NOTES_SUBDIR"); notesSubDir == "" {
		notesSubDir = "notes"
	}

	rootPath := path.Join(rootDir, rootFileName)
	notesDir := path.Join(rootDir, notesSubDir)

	// Create (if not exists) the notes subdirectory
	os.MkdirAll(notesDir, os.ModePerm)

	listRepo := service.NewDBListRepo(rootPath, notesDir)

	term := client.NewTerm(listRepo)

	err := term.RunClient()
	if err != nil {
		log.Fatal(err)
	}
}
