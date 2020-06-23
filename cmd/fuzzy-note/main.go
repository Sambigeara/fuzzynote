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
	//ImportLines()

	var rootPath string
	if rootPath = os.Getenv("FZN_ROOT_PAGE"); rootPath == "" {
		rootPath = "pages/root"
	}

	listRepo := service.NewDBListRepo(rootPath)

	//list, err := db.Get()
	//if err != nil {
	//    log.Error(err)
	//}

	term := client.NewTerm(listRepo)

	err := term.RunClient()
	// TODO
	if err != nil {
		log.Fatal(err)
	}

	//list, err = p.HandleKeyPresses()
	//if err != nil {
	//    log.Error(err)
	//} else {
	//    db.Store()
	//}

	// TODO this should be handled here gracefully (rather than in loop as current)
	//p.StoreList(RootPath)
}
