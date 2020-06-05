package main

import (
	"bufio"
	"log"
	"os"
	"time"
)

type SearchString struct {
	Keys [][]rune
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

func (p *Page) FetchMatches(searchGroups [][]rune) ([]PageItem, error) {
	/*For each line, iterate through each searchGroup. We should be left with lines with fulfil all groups. */
	res := []PageItem{}
	for _, p := range p.PageItems {
		isMatch := true
		for _, group := range searchGroups {
			if !IsFuzzyMatch(group, p.Line) {
				isMatch = false
				break
			}
		}
		if isMatch {
			res = append(res, p)
		}
	}
	return res, nil // TODO
}

func (p *Page) AddPageItem(pi PageItem) error {
	return nil
}
