package main

import (
	"bytes"
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
	fileName     = "cmd/importer/workflowy_test.xml"
	nodeTitle    = "outline"
	rootFileName = "primary.db"
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
	//Content []byte     `xml:",innerxml"`
}

func (n *Node) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	n.Attrs = start.Attr
	type node Node

	return d.DecodeElement((*node)(n), &start)
}

func walk(db service.ListRepo, nodes []Node, chain []string, f func(Node) bool) {
	for _, n := range nodes {
		newChain := chain
		if f(n) {
			newChain = append(newChain, n.Text.Value)
		}
		walk(db, n.Nodes, newChain, f)
		fullString := strings.Join(newChain, " >> ")
		fmt.Printf("%v\n", fullString)
		fmt.Printf("NOOOOOOOTE %s\n", n.Note.Value)
		//if note := n.Note.Value; note != "" {
		//    fmt.Printf("NOOOOOOOTE %s\n", note)
		//} else {
		//    note = ""
		//}
		//db.Add(fullString, []byte(note), nil)
	}
}

func importLines(db service.ListRepo) error {
	db.Load() // Instantiate db

	dat, err := ioutil.ReadFile(fileName)
	if err != nil {
		log.Fatal(err)
		return err
	}

	buf := bytes.NewBuffer(dat)
	dec := xml.NewDecoder(buf)

	var n Node
	err = dec.Decode(&n)
	if err != nil {
		panic(err)
	}

	walk(db, []Node{n}, []string{}, func(n Node) bool {
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
		rootDir = path.Join(os.Getenv("HOME"), ".fzn/imported/")
	}
	if notesSubDir = os.Getenv("FZN_NOTES_SUBDIR"); notesSubDir == "" {
		notesSubDir = "notes"
	}

	rootPath := path.Join(rootDir, rootFileName)
	notesDir := path.Join(rootDir, notesSubDir)

	// Create (if not exists) the notes subdirectory
	os.MkdirAll(notesDir, os.ModePerm)

	listRepo := service.NewDBListRepo(rootPath, notesDir)

	importLines(listRepo)
}
