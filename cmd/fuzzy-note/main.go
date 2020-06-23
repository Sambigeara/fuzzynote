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
	var rootPath string
	if rootPath = os.Getenv("FZN_ROOT_PAGE"); rootPath == "" {
		rootPath = "pages/root"
	}

	listRepo := service.NewDBListRepo(rootPath)

	term := client.NewTerm(listRepo)

	err := term.RunClient()
	if err != nil {
		log.Fatal(err)
	}
}
