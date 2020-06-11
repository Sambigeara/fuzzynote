package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"time"
)

type PageHeader struct {
	Id         uint32
	FileId     uint64
	DataLength uint64
}

// TODO untangle boundaries between data store and local data model
// TODO should not require string - should be a method attached to a datastore with config baked in
func PutListItem(l ListItem, path string, overwrite bool) error {
	var flag int
	if overwrite {
		flag = os.O_CREATE | os.O_WRONLY
	} else {
		flag = os.O_APPEND | os.O_CREATE | os.O_WRONLY
	}
	file, err := os.OpenFile(path, flag, 0644)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var header = PageHeader{
		Id:         1, // TODO id generator
		FileId:     1, // TODO
		DataLength: uint64(len(l.Line)),
	}
	err = binary.Write(file, binary.LittleEndian, &header)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
		return err
	}

	data := []byte(l.Line)
	err = binary.Write(file, binary.LittleEndian, &data)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
		return err
	}
	return nil
}

func PrependListArray(p *[]ListItem, l ListItem) []ListItem {
	arr := *p
	arr = append(arr, l)
	copy(arr[1:], arr)
	arr[0] = l
	return arr
}

func (p *List) StoreList(rootPath string) error {
	// TODO save to temp file and then transfer over
	for _, l := range p.ListItems {
		PutListItem(l, destPath, true)
	}
	return nil
}

func (p *List) BuildList(rootPath string) error {
	file, err := os.Open(rootPath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	for {
		header := PageHeader{}
		err := binary.Read(file, binary.LittleEndian, &header)
		if err != nil {
			switch err {
			case io.EOF:
				return nil
			case io.ErrUnexpectedEOF:
				fmt.Println("binary.Read failed on page header:", err)
				return err
			}
		}

		data := make([]byte, header.DataLength)
		err = binary.Read(file, binary.LittleEndian, &data)
		if err != nil {
			switch err {
			case io.EOF:
				return nil
			case io.ErrUnexpectedEOF:
				fmt.Println("binary.Read failed on page data:", err)
				return err
			}
		}
		p.ListItems = PrependListArray(&p.ListItems, ListItem{string(data), time.Now()})
	}
}
