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

	arr := []ListItem{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		t := scanner.Text()
		listItem := ListItem{t, time.Now()}
		arr = PrependListArray(arr, listItem)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	destFile, err := os.OpenFile(destPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer destFile.Close()

	for _, l := range arr {
		PutListItem(l, destFile)
	}
}
