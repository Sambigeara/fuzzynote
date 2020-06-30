package main

import (
	"log"
	"os"

	"fuzzy-note/pkg/client"
	"fuzzy-note/pkg/service"
	//"github.com/Sambigeara/fuzzy-note/pkg/client"
	//"github.com/Sambigeara/fuzzy-note/pkg/service"
)

func main() {
	var rootDir string
	if rootDir = os.Getenv("FZN_ROOT_DIR"); rootDir == "" {
		rootDir = "/Applications/fzn/"
	}

	// Create app directory if not present
	// TODO atm only valid for macs in `/Applications/`
	os.MkdirAll(rootDir, os.ModePerm)

	listRepo := service.NewDBListRepo(rootDir)

	term := client.NewTerm(listRepo)

	err := term.RunClient()
	if err != nil {
		log.Fatal(err)
	}
}
