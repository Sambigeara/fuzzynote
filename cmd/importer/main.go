package main

import (
	"encoding/xml"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strings"

	"fuzzy-note/pkg/service"
)

const (
	fileName       = "workflowy_source.xml"
	nodeTitle      = "outline"
	rootFileName   = "primary.db"
	walFilePattern = "wal_%d.db"
)

type Time struct {
	XMLName xml.Name `xml:"time"`
	Year    xml.Attr `xml:"startYear,attr"`
	Month   xml.Attr `xml:"startMonth,attr"`
	Day     xml.Attr `xml:"startDay,attr"`
	Content []byte   `xml:",innerxml"`
}

type Node struct {
	XMLName xml.Name
	Text    xml.Attr   `xml:"text,attr"`
	Note    xml.Attr   `xml:"_note,attr"`
	Nodes   []Node     `xml:",any"`
	Attrs   []xml.Attr `xml:"-"`
	//Time  Time
}

func walk(db service.ListRepo, oldItem *service.ListItem, nodes []Node, chain []string, f func(Node) bool) {
	// Iterate over in reverse order to mimic real life entry
	for i := len(nodes) - 1; i >= 0; i-- {
		n := nodes[i]
		newChain := chain
		if f(n) {
			newChain = append(newChain, n.Text.Value)
		}
		walk(db, oldItem, n.Nodes, newChain, f)
		fullString := strings.Join(newChain, " >> ")
		byteNote := []byte(n.Note.Value)
		err := db.Add(fullString, &byteNote, oldItem, nil)
		if err != nil {
			log.Fatal(err)
			os.Exit(1)
		}
		fmt.Printf("TEXT: %v\n", fullString)
		fmt.Printf("NOTE: %s\n", n.Note.Value)
	}
}

func importLines(db service.ListRepo) error {
	dat, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatal(err)
		return err
	}

	n := Node{}
	err = xml.Unmarshal(dat, &n)
	if err != nil {
		log.Fatal(err)
		return err
	}

	// Retrieve oldest item pre-import
	matches, err := db.Match([][]rune{}, nil, true)
	if err != nil {
		log.Fatal(err)
		return err
	}

	oldItem := matches[len(matches)-1]
	walk(db, oldItem, []Node{n}, []string{}, func(n Node) bool {
		if n.XMLName.Local == nodeTitle {
			return true
		}
		return false
	})

	return nil
}

func main() {
	var rootDir, notesSubDir string
	if rootDir = os.Getenv("FZN_IMPORT_ROOT_DIR"); rootDir == "" {
		// TODO currently only works on OSs with HOME
		rootDir = path.Join(os.Getenv("HOME"), ".fzn/import/")
	}
	if notesSubDir = os.Getenv("FZN_NOTES_SUBDIR"); notesSubDir == "" {
		notesSubDir = "notes"
	}

	rootPath := path.Join(rootDir, rootFileName)
	notesDir := path.Join(rootDir, notesSubDir)
	walDir := path.Join(rootDir, walFilePattern)

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

	err = importLines(listRepo)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}

	err = fileDS.Save(listRepo.Root, listRepo.PendingDeletions, listRepo.NextID)
	if err != nil {
		log.Fatal(err)
		os.Exit(1)
	}
}
