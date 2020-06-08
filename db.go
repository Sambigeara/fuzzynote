package main

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
)

type PageHeader struct {
	Id         uint32
	FileId     uint64
	DataLength uint64
}

func (p *List) BuildList(rootPath string) error {
	file, err := os.Open(rootPath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	var header PageHeader

	if err := binary.Read(file, binary.LittleEndian, &header); err != nil {
		fmt.Println("binary.Read failed:", err)
	}

	fmt.Println(header.Id)
	fmt.Println(header.FileId)
	fmt.Println(header.DataLength)

	data := make([]byte, header.DataLength)

	if err := binary.Read(file, binary.LittleEndian, &data); err != nil {
		fmt.Println("binary.Read failed:", err)
	}

	fmt.Println(string(data))

	//scanner := bufio.NewScanner(file)
	//for scanner.Scan() {
	//    t := scanner.Text()
	//    pageItem := ListItem{t, time.Now()}
	//    p.ListItems = append(p.ListItems, pageItem)
	//}
	//if err := scanner.Err(); err != nil {
	//    log.Fatal(err)
	//}
	return nil
}
