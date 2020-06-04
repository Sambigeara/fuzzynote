package main

import (
	"bufio"
	"log"
	"os"
	"time"
)

type SearchString struct {
	Key []rune
}

type Page struct {
	Search    SearchString
	PageItems []PageItem
	CurPos    int
}

type PageItem struct {
	Line      string
	DtCreated time.Time
}

func (p *Page) Load(rootPath string) error {
	file, err := os.Open(rootPath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		t := scanner.Text()
		pageItem := PageItem{t, time.Now()} // TODO need a way to persist datetime in files
		p.PageItems = append(p.PageItems, pageItem)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return nil
}

func (p *Page) FetchMatches(s []rune) ([]PageItem, error) {
	res := []PageItem{}
	for _, p := range p.PageItems {
		if IsFuzzyMatch(s, p.Line) {
			res = append(res, p)
		}
	}
	return res, nil // TODO
}

func (p *Page) AddPageItem(pi PageItem) error {
	return nil
}
