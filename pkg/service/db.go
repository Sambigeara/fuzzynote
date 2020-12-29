package service

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path"
	"time"
)

const firstListItemID uint32 = 1

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
}

const latestFileSchemaID fileSchemaID = 2

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
	schemaID fileSchemaID
	uuid     uuid
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

var listItemSchemaMap = map[fileSchemaID]interface{}{
	0: listItemSchema0{},
	1: listItemSchema1{},
	// 2 maps to 1 to allow for addition of UUID to file header, which is handled as a special case
	2: listItemSchema1{},
}

// Load is called on initial startup. It instantiates the app, and deserialises and displays
// default LineItems
func (d *FileDataStore) Load() (*ListItem, uint32, error) {
	nextID := firstListItemID
	f, err := os.OpenFile(d.rootPath, os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
		return nil, nextID, err
	}
	defer f.Close()

	// TODO remove this temp measure once all users are on file schema >= 1
	// The first version did not have a file schema. To detemine if a file is of the original schema,
	// we rely on the likely fact that no-one has generated enough listItems (>65535) to use the right two
	// bytes of the initial uint32 assigned for the first listItemID. E.g. if the second uint16 (bytes 3/4)
	// are 0, then it's likely the original schema.
	// With the above in mind, we can infer the correct file schema and file offset as follows:
	//
	// if first uint16 == 0:
	//   fileSchemaId = 0
	//   fileOffset = 2
	// else if second uint16 == 0:
	//   fileSchemaId = 0
	//   fileOffset = 0
	// else:
	//   fileSchemaId = first uint16
	//   fileOffset = 2
	//
	var firstTwo, secondTwo uint16
	err = binary.Read(f, binary.LittleEndian, &firstTwo)
	err = binary.Read(f, binary.LittleEndian, &secondTwo)
	if err != nil {
		if err == io.EOF {
			return nil, nextID, nil
		}
		log.Fatal(err)
		return nil, nextID, err
	}
	// NOTE: File offset now at 4

	fileHeader := fileHeader{}
	wasUnversionedFile := false
	if firstTwo == 0 {
		// File schema is explicitly set to 0
		fileHeader.schemaID = 0
		f.Seek(2, io.SeekStart)
	} else if secondTwo == 0 {
		wasUnversionedFile = true
		// Fileschema not set (assuming no lineItem with ID > 65535)
		fileHeader.schemaID = 0
		f.Seek(0, io.SeekStart)
	} else {
		fileHeader.schemaID = fileSchemaID(firstTwo)
		f.Seek(2, io.SeekStart)
		// NOTE Special case handling for introduction of UUID in fileSchema 2
		if fileHeader.schemaID >= 2 {
			err = binary.Read(f, binary.LittleEndian, &fileHeader.uuid)
			if err != nil {
				if err == io.EOF {
					return nil, nextID, nil
				}
				log.Fatal(err)
				return nil, nextID, err
			}
			d.uuid = fileHeader.uuid

			// Load the WAL into memory
			d.walFile.Load(d.uuid)
		}
	}

	// Retrieve first line from the file, which will be the youngest (and therefore top) entry
	var root, cur *ListItem

	for {
		nextItem := ListItem{}
		cont, err := d.readListItemFromFile(f, fileHeader.schemaID, &nextItem)
		if err != nil {
			//log.Fatal(err)
			return nil, nextID, err
		}
		// TODO: 2020-12-05 remove once everyone using file schema >= 1
		// Under normal circumstances, the first item read from the file will be youngest.
		// However, due to annoying historical "reasons", the first non-versioned file schema
		// wrote the listItems in reverse order, so we need to add an awful TEMP hack in here
		// to join the doubly linked list in the reverse order
		if wasUnversionedFile {
			// This bit will be removed
			nextItem.parent = cur
			if !cont {
				return cur, nextID, nil
			}
			if cur != nil {
				cur.child = &nextItem
			}
			cur = &nextItem
		} else {
			// This bit will stay
			nextItem.child = cur
			if !cont {
				return root, nextID, nil
			}
			if cur == nil {
				root = &nextItem
			} else {
				cur.parent = &nextItem
			}
			cur = &nextItem
		}

		// TODO
		// We need to find the next available index for the entire dataset
		if nextItem.id >= nextID {
			nextID = nextItem.id + 1
		}
	}
}

// Save is called on app shutdown. It flushes all state changes in memory to disk
func (d *FileDataStore) Save(root *ListItem, pendingDeletions []*ListItem) error {
	// TODO remove all files starting with `bak_*`, these are no longer needed

	// Delete any files that need clearing up
	for _, item := range pendingDeletions {
		strID := fmt.Sprint(item.id)
		oldPath := path.Join(d.notesPath, strID)
		err := os.Remove(oldPath)
		if err != nil {
			// TODO is this required?
			if !os.IsNotExist(err) {
				log.Fatal(err)
				return err
			}
		}
	}

	f, err := os.Create(d.rootPath)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer f.Close()

	// If UUID has not been set (e.g. file schema migration 1 -> 2, generate and set on Save
	if d.uuid == 0 {
		d.uuid = d.generateUUID()
	}
	// Write the file header to the start of the file
	fileHeader := fileHeader{
		schemaID: d.latestFileSchemaID,
		uuid:     d.uuid,
	}
	err = binary.Write(f, binary.LittleEndian, fileHeader)
	if err != nil {
		fmt.Println("binary.Write failed when writing fileSchemaID:", err)
		log.Fatal(err)
		return err
	}

	// Load the WAL into memory
	d.walFile.Save(d.uuid)

	// Return if no files to write. os.Create truncates by default so the file will
	// be empty, with just the file header (including verion id and UUID)
	if root == nil {
		return nil
	}

	cur := root

	for {
		err := d.writeFileFromListItem(f, cur)
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

func (d *FileDataStore) readListItemFromFile(f io.Reader, schemaID fileSchemaID, newItem *ListItem) (bool, error) {
	i, ok := listItemSchemaMap[schemaID]
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

		note, err := d.loadPage(s.PageID)
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

func (d *FileDataStore) writeFileFromListItem(f io.Writer, listItem *ListItem) error {
	i, ok := listItemSchemaMap[d.latestFileSchemaID]
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

		d.savePage(listItem.id, listItem.Note)
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

func (d *FileDataStore) loadPage(id uint32) (*[]byte, error) {
	strID := fmt.Sprint(id)
	filePath := path.Join(d.notesPath, strID)

	dat := make([]byte, 0)
	// If file does not exist, return nil
	if _, err := os.Stat(filePath); err != nil {
		if os.IsNotExist(err) {
			return &dat, nil
		} else {
			return nil, err
		}
	}

	// Read whole file
	dat, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	return &dat, nil
}

func (d *FileDataStore) savePage(id uint32, data *[]byte) error {
	strID := fmt.Sprint(id)
	filePath := path.Join(d.notesPath, strID)

	// If data has been removed or is empty, delete the file and return
	if data == nil || len(*data) == 0 {
		_ = os.Remove(filePath)
		// TODO handle failure more gracefully? AFAIK os.Remove just returns a *PathError on failure
		// which is mostly indicative of a noneexistent file, so good enough for now...
		return nil
	}

	// Open or create a file in the `/notes/` subdir using the listItem id as the file name
	// This needs to be before the ReadFile below to ensure the file exists
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	defer f.Close()

	_, err = f.Write(*data)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
		return err
	}
	return nil
}
