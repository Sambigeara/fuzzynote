package service

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	//"runtime"
	"time"
)

type (
	fileSchemaID uint16
	uuid         uint32
)

// DataStore represents the interface between the ListRepo and physical storage
type DataStore interface {
	generateUUID() uuid
	Load() error
	Save() error
}

// FileDataStore is an implementation of the DataStore interface
type FileDataStore struct {
	rootPath           string
	notesPath          string
	latestFileSchemaID fileSchemaID
	uuid               uuid
	walFile            *WalFile
	nextListItemID     uint64
}

const latestFileSchemaID fileSchemaID = 3

func (d *FileDataStore) generateUUID() uuid {
	return uuid(rand.Uint32())
}

func init() {
	rand.Seed(time.Now().UnixNano())
}

// NewFileDataStore instantiates a new FileDataStore
func NewFileDataStore(rootPath string, notesPath string, walFile *WalFile) *FileDataStore {
	return &FileDataStore{
		rootPath:           rootPath,
		notesPath:          notesPath,
		latestFileSchemaID: latestFileSchemaID,
		walFile:            walFile,
	}
}

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
func (w *Wal) Load(r *DBListRepo) error {
	f, err := os.OpenFile(w.rootPath, os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer f.Close()

	fileHeader := fileHeader{}
	//fileHeader.schemaID = fileSchemaID(firstTwo)
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
			w.uuid = fileHeader.uuid
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
		cont, err := w.readListItemFromFile(f, fileHeader.schemaID, &nextItem)
		if err != nil {
			//log.Fatal(err)
			return err
		}

		nextItem.child = cur
		if !cont {
			// Load the WAL into memory
			if err := w.loadWal(); err != nil {
				return err
			}
			//runtime.Breakpoint()
			w.replayWalEvents(r, primaryRoot)
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
func (w *Wal) Save(root *ListItem, pendingDeletions []*ListItem, nextListItemID uint64) error {
	f, err := os.Create(w.rootPath)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer f.Close()

	// TODO use fileHeader once earlier versions are gone
	// Write the file header to the start of the file
	fileHeader := fileHeader{
		schemaID:       latestFileSchemaID,
		uuid:           w.uuid,
		nextListItemID: nextListItemID,
	}
	err = binary.Write(f, binary.LittleEndian, &fileHeader)
	if err != nil {
		fmt.Println("binary.Write failed when writing fileHeader:", err)
		log.Fatal(err)
		return err
	}

	// Load the WAL into memory
	w.saveWal()

	// Return if no files to write. os.Create truncates by default so the file will
	// be empty, with just the file header (including verion id and UUID)
	if root == nil {
		return nil
	}

	cur := root

	for {
		err := w.writeFileFromListItem(f, cur)
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

func (w *Wal) readListItemFromFile(f io.Reader, schemaID fileSchemaID, newItem *ListItem) (bool, error) {
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

func (w *Wal) writeFileFromListItem(f io.Writer, listItem *ListItem) error {
	// TODO this doesn't need to be a map now
	i, ok := listItemSchemaMap[latestFileSchemaID]
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
