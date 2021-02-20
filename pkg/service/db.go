package service

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	//"math/rand"
	"os"
	//"runtime"
	//"time"
)

//func init() {
//    rand.Seed(time.Now().UnixNano())
//}

type fileHeader struct {
	schemaID       fileSchemaID
	uuid           uuid
	nextListItemID uint64
}

type listItemSchema1 struct {
	PageID     uint32
	Metadata   bits
	LineLength uint64
	NoteLength uint64
}

var listItemSchemaMap = map[fileSchemaID]interface{}{
	1: listItemSchema1{},
	// 2 maps to 1 to allow for addition of UUID to file header, which is handled as a special case
	2: listItemSchema1{},
	3: listItemSchema1{},
}

// Load is called on initial startup. It instantiates the app, and deserialises and displays
// default LineItems
func (r *DBListRepo) Load() error {
	f, err := os.OpenFile(r.rootPath, os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer f.Close()

	fileHeader := fileHeader{}
	err = binary.Read(f, binary.LittleEndian, &fileHeader.schemaID)
	if err != nil {
		if err == io.EOF {
			return nil
		}
		log.Fatal(err)
		return err
	}

	// NOTE Special case handling for introduction of UUID in fileSchema 2
	if fileHeader.schemaID >= 2 {
		err = binary.Read(f, binary.LittleEndian, &fileHeader.uuid)
		if err != nil {
			if err == io.EOF {
				return nil
			}
			log.Fatal(err)
			return err
		}
		// TODO get rid of this
		// We can now override uuid as it's been read from the file
		if fileHeader.uuid != 0 {
			r.wal.uuid = fileHeader.uuid
		}

		// NOTE special case handling for introduction of persisted nextId in file
		if fileHeader.schemaID >= 3 {
			err = binary.Read(f, binary.LittleEndian, &r.NextID)
			if err != nil {
				if err == io.EOF {
					return nil
				}
				log.Fatal(err)
				return err
			}
		}
	}

	var cur *ListItem
	// primaryRoot is the root built from primary.db rather than from the WAL
	var primaryRoot *ListItem

	// Retrieve first line from the file, which will be the youngest (and therefore top) entry
	for {
		nextItem := ListItem{}
		cont, err := r.readListItemFromFile(f, fileHeader.schemaID, &nextItem)
		if err != nil {
			//log.Fatal(err)
			return err
		}

		nextItem.child = cur
		if !cont {
			// Load the WAL into memory
			if err := r.wal.load(); err != nil {
				return err
			}
			//runtime.Breakpoint()
			r.wal.replay(r, primaryRoot)
			return nil
		}
		if cur == nil {
			// TODO
			primaryRoot = &nextItem
		} else {
			cur.parent = &nextItem
		}
		cur = &nextItem

		// TODO REMOVE THIS ONCE ALL REMOTES ARE USING v3 or higher
		// This is only relevant on the first instance of moving from v < 3 to v >= 3
		// We need to find the next available index for the entire dataset
		if nextItem.id >= r.NextID {
			r.NextID = nextItem.id + 1
		}
	}
}

// Save is called on app shutdown. It flushes all state changes in memory to disk
func (r *DBListRepo) Save() error {
	f, err := os.Create(r.rootPath)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer f.Close()

	// Write the file header to the start of the file
	fileHeader := fileHeader{
		schemaID:       r.latestFileSchemaID,
		uuid:           r.wal.uuid,
		nextListItemID: r.NextID,
	}
	err = binary.Write(f, binary.LittleEndian, &fileHeader)
	if err != nil {
		fmt.Println("binary.Write failed when writing fileHeader:", err)
		log.Fatal(err)
		return err
	}

	r.wal.save()

	// We don't care about the primary.db for now, so return nil here
	//return nil

	// Return if no files to write. os.Create truncates by default so the file will
	// be empty, with just the file header (including verion id and UUID)
	if r.Root == nil {
		return nil
	}

	cur := r.Root

	for {
		err := r.writeFileFromListItem(f, cur)
		if err != nil {
			//log.Fatal(err)
			return err
		}

		if cur.parent == nil {
			break
		}
		cur = cur.parent
	}
	return nil
}

func (r *DBListRepo) readListItemFromFile(f io.Reader, schemaID fileSchemaID, newItem *ListItem) (bool, error) {
	i, ok := listItemSchemaMap[schemaID]
	if !ok {
		i, _ = listItemSchemaMap[0]
	}

	var err error
	switch s := i.(type) {
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
		newItem.id = uint64(s.PageID)
		newItem.Note = &note
		newItem.IsHidden = has(s.Metadata, hidden)
		return true, nil
	}
	return false, err
}

func (r *DBListRepo) writeFileFromListItem(f io.Writer, listItem *ListItem) error {
	// TODO this doesn't need to be a map now
	i, ok := listItemSchemaMap[r.latestFileSchemaID]
	if !ok {
		i, _ = listItemSchemaMap[0]
	}

	var err error
	switch s := i.(type) {
	case listItemSchema1:
		var metadata bits = 0
		if listItem.IsHidden {
			metadata = set(metadata, hidden)
		}
		byteLine := []byte(listItem.Line)

		s.PageID = uint32(listItem.id)
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
