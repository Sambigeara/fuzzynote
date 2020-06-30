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

func main() {
	var rootDir string
	if rootDir = os.Getenv("FZN_ROOT_DIR"); rootDir == "" {
		// TODO currently only works on OSs with HOME
		rootDir = path.Join(os.Getenv("HOME"), ".fzn/")
	}

	// Create app directory if not present
	// TODO atm only valid for macs in `/Applications/`
	dirsToCreate := []string{
		"notes",
	}
	for _, d := range dirsToCreate {
		os.MkdirAll(path.Join(rootDir, d), os.ModePerm)
	}

	listRepo := service.NewDBListRepo(rootDir)

	term := client.NewTerm(listRepo)

	err := term.RunClient()
	if err != nil {
		log.Fatal(err)
	}
}
