package service

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
)

// LatestFileSchemaID will be the id used when saving files
const LatestFileSchemaID uint16 = 1

type fileHeader struct {
	FileSchemaID uint16
}

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
				fmt.Println("binary.Read failed on listItem header:", err)
				return false, err
			}
		}

		line := make([]byte, s.LineLength)
		err = binary.Read(f, binary.LittleEndian, &line)
		if err != nil {
			fmt.Println("binary.Read failed on listItem line:", err)
			return false, err
		}

		note, err := r.loadPage(s.PageID)
		if err != nil {
			return false, err
		}

		newItem.Line = string(line)
		newItem.id = s.PageID
		newItem.Note = note
		newItem.IsHidden = has(s.Metadata, hidden)
		return true, nil
	case listItemSchema1:
		err = binary.Read(f, binary.LittleEndian, &s)
		if err != nil {
			switch err {
			case io.EOF:
				return false, nil
			case io.ErrUnexpectedEOF:
				fmt.Println("binary.Read failed on listItem header:", err)
				return false, err
			}
		}

		line := make([]byte, s.LineLength)
		err = binary.Read(f, binary.LittleEndian, &line)
		if err != nil {
			fmt.Println("binary.Read failed on listItem line:", err)
			return false, err
		}

		note := make([]byte, s.NoteLength)
		err = binary.Read(f, binary.LittleEndian, &note)
		if err != nil {
			fmt.Println("binary.Read failed on listItem note:", err)
			return false, err
		}

		newItem.Line = string(line)
		newItem.id = s.PageID
		newItem.Note = &note
		newItem.IsHidden = has(s.Metadata, hidden)
		return true, nil
	}
	return false, err
}

func (r *DBListRepo) writeFileFromListItem(f io.Writer, listItem *ListItem) error {
	i, ok := listItemSchemaMap[r.latestFileSchemaID]
	if !ok {
		fmt.Printf("KAPOW   %v\n", i)
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
		s.FileID = listItem.id
		s.LineLength = uint64(len([]byte(listItem.Line)))

		byteLine := []byte(listItem.Line)

		data := []interface{}{&s, &byteLine}

		// TODO the below writes need to be atomic
		for _, v := range data {
			err = binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				fmt.Printf("binary.Write failed when writing %v: %s\n", v, err)
				log.Fatal(err)
				return err
			}
		}

		r.savePage(listItem.id, listItem.Note)
	case listItemSchema1:
		var metadata bits = 0
		if listItem.IsHidden {
			metadata = set(metadata, hidden)
		}
		byteLine := []byte(listItem.Line)

		s.PageID = listItem.id
		s.Metadata = metadata
		s.LineLength = uint64(len([]byte(listItem.Line)))
		s.NoteLength = 0

		data := []interface{}{&s, &byteLine}
		if listItem.Note != nil {
			s.NoteLength = uint64(len(*listItem.Note))
			data = append(data, listItem.Note)
		}

		// TODO the below writes need to be atomic
		for _, v := range data {
			err = binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				fmt.Printf("binary.Write failed when writing %v: %s\n", v, err)
				log.Fatal(err)
				return err
			}
		}
	}
	return err
}
