package service

import (
	"encoding/binary"
	//"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"path"
	"regexp"
	"strings"
	"time"
	"unicode"
)

type ListRepo interface {
	Load() error
	Save() error
	Add(line string, item *ListItem) error
	Update(line string, listItem *ListItem) error
	Delete(listItem *ListItem) error
	Match(keys [][]rune, active *ListItem) ([]*ListItem, error)
	EditPage(id uint32) (*[]byte, func(*[]byte) error, error)
	HasPendingChanges() bool
}

type DBListRepo struct {
	rootPath          string
	notesPath         string
	root              *ListItem
	nextID            uint32
	hasPendingChanges bool
}

type ListItem struct {
	Line   string
	Parent *ListItem
	Child  *ListItem
	ID     uint32
}

// FileHeader will store the schema id, so we know which pageheader to use
type FileHeader struct {
	SchemaID uint32
}

type ItemHeader struct {
	PageID     uint32
	Metadata   uint32
	FileID     uint32
	DataLength uint64
}

func NewDBListRepo(rootPath string, notesPath string) *DBListRepo {
	return &DBListRepo{
		rootPath:          rootPath,
		notesPath:         notesPath,
		hasPendingChanges: false,
	}
}

func (r *DBListRepo) Load() error {
	f, err := os.OpenFile(r.rootPath, os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer f.Close()

	// Retrieve first line from the file, which will be the oldest (and therefore bottom) entry
	var cur, oldest *ListItem

	r.nextID = 1

OuterLoop:
	for {

		header := ItemHeader{}
		err := binary.Read(f, binary.LittleEndian, &header)

		if err != nil {
			switch err {
			case io.EOF:
				break OuterLoop
			case io.ErrUnexpectedEOF:
				fmt.Println("binary.Read failed on page header:", err)
				return err
			}
		}

		// Initially we need to find the next available index for the ENTIRE dataset
		if header.PageID > r.nextID {
			r.nextID = header.PageID + 1
		}

		data := make([]byte, header.DataLength)
		err = binary.Read(f, binary.LittleEndian, &data)

		if err != nil {
			switch err {
			case io.EOF:
				break OuterLoop
			case io.ErrUnexpectedEOF:
				fmt.Println("binary.Read failed on page header:", err)
				return err
			}
		}

		nextItem := ListItem{
			Line:   string(data),
			Parent: cur,
			ID:     header.PageID,
		}
		if cur == nil {
			// `cur` will only be nil on the first iteration, therefore we can assign the oldest node here for idx assignment below
			oldest = &nextItem
		}
		cur = &nextItem
	}

	// Handle empty file
	if cur == nil {
		return nil
	}

	// Now we have know the global nextID (to account for unordered IDs), iterate through (from oldest to youngest) and assign any indexes where required.
	for {
		if oldest.Child == nil {
			break
		}
		if oldest.ID == 0 {
			oldest.ID = r.nextID
			r.nextID++
		}
		oldest = oldest.Child
	}

	r.root = cur

	// `cur` is now a ptr to the most recent ListItem
	for {
		if cur.Parent == nil {
			break
		}
		cur.Parent.Child = cur
		cur = cur.Parent
	}

	return nil
}

// TODO untangle boundaries between data store and local data model
// TODO should not require string - should be a method attached to a datastore with config baked in
func writeListItemToFile(l *ListItem, f *os.File) error {
	header := ItemHeader{
		PageID:     0, // TODO id generator
		Metadata:   0, // TODO
		FileID:     0, // TODO
		DataLength: uint64(len(l.Line)),
	}
	err := binary.Write(f, binary.LittleEndian, &header)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
		return err
	}

	data := []byte(l.Line)
	err = binary.Write(f, binary.LittleEndian, &data)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
		return err
	}
	return nil
}

func (r *DBListRepo) Save() error {
	// Account for edge condition where Load hasn't been run, and the ID is incorrectly set to 0
	if r.nextID == 0 {
		r.nextID = 1
	}

	// TODO when appending individual item rather than overwriting
	//f, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	f, err := os.Create(r.rootPath)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer f.Close()

	listItem := r.root

	// Write empty file if no listItems exist
	if listItem == nil {
		err := binary.Write(f, binary.LittleEndian, []byte{})
		if err != nil {
			fmt.Println("binary.Write failed:", err)
			log.Fatal(err)
			return err
		}
		return nil
	}

	root := listItem
	r.root = root

	// TODO store oldest item on Load
	// Get oldest listItem
	for {
		if listItem.Parent == nil {
			break
		}
		listItem = listItem.Parent
	}

	for {
		if listItem.ID == 0 {
			listItem.ID = r.nextID
			r.nextID++
		}
		header := ItemHeader{
			PageID:     listItem.ID,
			Metadata:   0,           // TODO
			FileID:     listItem.ID, // TODO
			DataLength: uint64(len(listItem.Line)),
		}

		// TODO the below writes need to be atomic
		err := binary.Write(f, binary.LittleEndian, &header)
		if err != nil {
			fmt.Println("binary.Write failed:", err)
			log.Fatal(err)
			return err
		}
		data := []byte(listItem.Line)
		err = binary.Write(f, binary.LittleEndian, &data)
		if err != nil {
			fmt.Println("binary.Write failed:", err)
			log.Fatal(err)
			return err
		}

		if listItem.Child == nil {
			break
		}
		listItem = listItem.Child
	}

	r.hasPendingChanges = false

	return nil
}

func (r *DBListRepo) Add(line string, child *ListItem) error {
	r.hasPendingChanges = true

	newItem := ListItem{
		Line:  line,
		ID:    r.nextID,
		Child: child,
	}
	r.nextID++

	// If `child` is nil, it's the first item in the list so set as root and return
	if child == nil {
		oldRoot := r.root
		r.root = &newItem
		if oldRoot != nil {
			newItem.Parent = oldRoot
			oldRoot.Child = &newItem
		}
		return nil
	}

	if child.Parent != nil {
		child.Parent.Child = &newItem
		newItem.Parent = child.Parent
	}
	child.Parent = &newItem

	return nil
}

func (r *DBListRepo) Update(line string, listItem *ListItem) error {
	listItem.Line = line
	r.hasPendingChanges = true
	return nil
}

func (r *DBListRepo) Delete(item *ListItem) error {
	if item.Child != nil {
		item.Child.Parent = item.Parent
	} else {
		// If the item has no child, it is at the top of the list and therefore we need to update the root
		r.root = item.Parent
	}

	if item.Parent != nil {
		item.Parent.Child = item.Child
	}

	// Because I don't yet trust the app, rather than deleting notes (which could be unintentionally deleted with lots of data),
	// append them with `_bak_{line}_{timestamp}`, so we know the context of the line, and the timestamp at which it was deleted.
	// We need to remove the originally named notes file to prevent orphaned files being used with future notes (due to current idx logic)
	strID := fmt.Sprint(item.ID)
	oldPath := path.Join(r.notesPath, strID)

	reg, err := regexp.Compile("[^a-zA-Z0-9]+")
	if err != nil {
		log.Fatal(err)
	}
	alphanumline := reg.ReplaceAllString(item.Line, "")

	newPath := path.Join(r.notesPath, fmt.Sprintf("bak_%d_%s_%s", item.ID, alphanumline, fmt.Sprint(time.Now().Unix())))
	err = os.Rename(oldPath, newPath)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Fatal(err)
			return err
		}
	}

	r.hasPendingChanges = true

	return nil
}

// Search functionality

func isSubString(sub string, full string) bool {
	if strings.Contains(strings.ToLower(full), strings.ToLower(sub)) {
		return true
	}
	return false
}

// Iterate through the full string, when you match the "head" of the sub rune slice,
// pop it and continue through. If you clear sub, return true. Searches in O(n)
func isFuzzyMatch(sub []rune, full string) bool {
	for _, c := range full {
		if unicode.ToLower(c) == unicode.ToLower(sub[0]) {
			_, sub = sub[0], sub[1:]
		}
		if len(sub) == 0 {
			return true
		}
	}
	return false
}

// If a matching group starts with `#` do a substring match, otherwise do a fuzzy search
func isMatch(sub []rune, full string) bool {
	if len(sub) == 0 {
		return true
	}
	if sub[0] == '#' {
		return isSubString(string(sub[1:]), full)
	} else {
		return isFuzzyMatch(sub, full)
	}
}

func (r *DBListRepo) Match(keys [][]rune, active *ListItem) ([]*ListItem, error) {
	/*For each line, iterate through each searchGroup. We should be left with lines with fulfil all groups. */
	cur := r.root

	res := make([]*ListItem, 0)

	if cur == nil {
		return res, nil
	}

	for {
		matched := true
		for _, group := range keys {
			// Match any items with empty Lines (this accounts for lines added when search is active)
			// "active" listItems pass automatically to allow mid-search item editing
			if len(cur.Line) == 0 || cur == active {
				break
			}
			if !isMatch(group, cur.Line) {
				matched = false
				break
			}
		}
		if matched {
			res = append(res, cur)
		}

		if cur.Parent == nil {
			return res, nil
		}
		cur = cur.Parent
	}
}

func (r *DBListRepo) EditPage(id uint32) (*[]byte, func(*[]byte) error, error) {
	strID := fmt.Sprint(id)
	filePath := path.Join(r.notesPath, strID)

	// Open or create a file in the `/notes/` subdir using the listItem ID as the file name
	// This needs to be before the ReadFile below to ensure the file exists
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0755)

	// Read whole file
	dat, err := ioutil.ReadFile(filePath)
	if err != nil {
		log.Fatal(err)
		return nil, nil, err
	}

	writeFn := func(newDat *[]byte) error {
		defer f.Close()

		_, err = f.Write(*newDat)
		if err != nil {
			fmt.Println("binary.Write failed:", err)
			return err
		}
		return nil
	}

	r.hasPendingChanges = true

	return &dat, writeFn, nil
}

func (r *DBListRepo) HasPendingChanges() bool {
	return r.hasPendingChanges
}
