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
		err := listRepo.Save(listRepo.Root, listRepo.NextID)
		if err != nil {
			log.Fatal(err)
		}
		os.Exit(0)
	}
}
