package main

import (
	"bufio"
	"log"
	"os"
	"time"
)

const (
	originPath = "pages/root_plain"
	destPath   = "pages/root"
)

func ImportLines() {
	file, err := os.Open(originPath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		t := scanner.Text()
		listItem := ListItem{t, time.Now()}
		PutListItem(listItem, destPath)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
}
