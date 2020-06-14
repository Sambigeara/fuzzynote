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
func PutListItem(l ListItem, file *os.File) error {
	var header = PageHeader{
		Id:         1, // TODO id generator
		FileId:     1, // TODO
		DataLength: uint64(len(l.Line)),
	}
	err := binary.Write(file, binary.LittleEndian, &header)
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

func PrependListArray(arr []ListItem, l ListItem) []ListItem {
	arr = append(arr, l)
	copy(arr[1:], arr)
	arr[0] = l
	return arr
}

func (p *List) StoreList() error {
	// TODO save to temp file and then transfer over

	// TODO when appending individual item rather than overwriting
	//file, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	file, err := os.Create(p.RootPath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	// Add files in reverse for file order consistency
	for i := len(p.ListItems) - 1; i >= 0; i-- {
		PutListItem(p.ListItems[i], file)
	}
	return nil
}

func (p *List) BuildList() error {
	file, err := os.OpenFile(p.RootPath, os.O_CREATE, 0644)
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
		p.ListItems = PrependListArray(p.ListItems, ListItem{string(data), time.Now()})
	}
}
