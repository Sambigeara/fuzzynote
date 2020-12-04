package service

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
)

// LatestFileSchemaID will be the id used when saving files
//const LatestFileSchemaID = 1
const LatestFileSchemaID uint16 = 0

type fileHeader struct {
	FileSchemaID uint16
}

//type listItemSchema interface {
//    readItem() error
//}

type listItemSchema0 struct {
	PageID     uint32
	Metadata   bits
	FileID     uint32
	LineLength uint64
}
type listItemSchema1 struct {
	PageID     uint32
	Metadata   bits
	LineLength uint64
	NoteLength uint64
}

//func (l *listItemSchema0) readItem() error {
//    return nil
//}

//func (l *listItemSchema1) readItem() error {
//    return nil
//}

var listItemSchemaMap = map[uint16]interface{}{
	0: listItemSchema0{},
	1: listItemSchema1{},
}

func (r *DBListRepo) readListItemFromFile(f io.Reader, fileSchemaID uint16, newItem *ListItem) (bool, error) {
	i, ok := listItemSchemaMap[fileSchemaID]
	if !ok {
		i, _ = listItemSchemaMap[0]
	}

	var err error
	switch s := i.(type) {
	case listItemSchema0:
		err = binary.Read(f, binary.LittleEndian, &s)
		if err != nil {
			switch err {
			case io.EOF:
				return false, nil
			case io.ErrUnexpectedEOF:
				fmt.Println("binary.Read failed on page header:", err)
				return false, err
			}
		}

		line := make([]byte, s.LineLength)
		err = binary.Read(f, binary.LittleEndian, &line)
		if err != nil {
			fmt.Println("binary.Read failed on page header:", err)
			return false, err
		}

		note, err := r.loadPage(s.PageID)
		if err != nil {
			return false, err
		}

		newItem.Line = string(line)
		//newItem.child = cur
		newItem.id = s.PageID
		newItem.Note = note
		newItem.IsHidden = has(s.Metadata, hidden)
		return true, nil
	case listItemSchema1:
		//TODO
		//err = binary.Read(f, binary.LittleEndian, &s)
		return false, nil
	}
	return false, err
}

func (r *DBListRepo) writeFileFromListItem(f io.Writer, listItem *ListItem) error {
	i, ok := listItemSchemaMap[LatestFileSchemaID]
	if !ok {
		i, _ = listItemSchemaMap[0]
	}

	var err error
	switch s := i.(type) {
	case listItemSchema0:
		var metadata bits = 0
		if listItem.IsHidden {
			metadata = set(metadata, hidden)
		}

		s.PageID = listItem.id
		s.Metadata = metadata
		s.FileID = listItem.id // TODO
		s.LineLength = uint64(len(listItem.Line))

		byteLine := []byte(listItem.Line)

		data := []interface{}{&s, &byteLine}

		// TODO the below writes need to be atomic
		for _, v := range data {
			err = binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				fmt.Println("binary.Write failed:", err)
				log.Fatal(err)
				return err
			}
		}

		r.savePage(listItem.id, listItem.Note)
	case listItemSchema1:
		//TODO
		//err = binary.Read(f, binary.LittleEndian, &s)
		return nil
	}
	return err
}
