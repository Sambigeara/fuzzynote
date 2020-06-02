package main

import (
	"bufio"
	"log"
	"os"
	"time"
)

type PageRepo interface {
	LoadRootPage() []PageItem
	FetchMatches(searchString string) ([]PageItem, error)
	AddPageItem(item PageItem) error
}

type DbRepo struct {
	Page []PageItem
}

func (db *DbRepo) LoadRootPage(rootPath string) error {
	file, err := os.Open(rootPath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		t := scanner.Text()
		pageItem := PageItem{t, time.Now()} // TODO need a way to persist datetime in files
		db.Page = append(db.Page, pageItem)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	return nil
}

func (db *DbRepo) FetchMatches(s []rune) ([]PageItem, error) {
	res := []PageItem{}
	for _, p := range db.Page {
		if IsFuzzyMatch(string(s), p.Line) {
			res = append(res, p)
		}
	}
	return res, nil // TODO
}

func (db *DbRepo) AddPageItem(p PageItem) error {
	return nil
}
