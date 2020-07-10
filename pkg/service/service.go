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
	Add(line string, note *[]byte, item *ListItem) error
	Update(line string, note *[]byte, listItem *ListItem) error
	Delete(listItem *ListItem) error
	Match(keys [][]rune, active *ListItem, showHidden bool) ([]*ListItem, error)
	HasPendingChanges() bool
}

type DBListRepo struct {
	rootPath          string
	notesPath         string
	root              *ListItem
	nextID            uint32
	hasPendingChanges bool
	pendingDeletions  []*ListItem
}

type ListItem struct {
	Line     string
	Parent   *ListItem
	Child    *ListItem
	ID       uint32
	Note     *[]byte
	IsHidden bool
}

// FileHeader will store the schema id, so we know which pageheader to use
type FileHeader struct {
	SchemaID uint32
}

type bits uint32

const (
	hidden bits = 1 << iota
)

func set(b, flag bits) bits    { return b | flag }
func clear(b, flag bits) bits  { return b &^ flag }
func toggle(b, flag bits) bits { return b ^ flag }
func has(b, flag bits) bool    { return b&flag != 0 }

type ItemHeader struct {
	PageID     uint32
	Metadata   bits
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
		if header.PageID >= r.nextID {
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

		dat, err := r.loadPage(header.PageID)
		if err != nil {
			return err
		}

		nextItem := ListItem{
			Line:     string(data),
			Parent:   cur,
			ID:       header.PageID,
			Note:     dat,
			IsHidden: has(header.Metadata, hidden),
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

func (r *DBListRepo) Save() error {
	for _, item := range r.pendingDeletions {
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
	}

	r.pendingDeletions = []*ListItem{}
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
		r.hasPendingChanges = false
		return nil
	}

	r.root = listItem

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
		var metadata bits = 0
		if listItem.IsHidden {
			metadata = set(metadata, hidden)
		}
		header := ItemHeader{
			PageID:     listItem.ID,
			Metadata:   metadata,
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
		// If Note is not empty, save as a note file
		if listItem.Note != nil && len(*listItem.Note) > 0 {
			r.savePage(listItem.ID, listItem.Note)
		}

		if listItem.Child == nil {
			break
		}
		listItem = listItem.Child
	}
	r.hasPendingChanges = false
	return nil
}

func (r *DBListRepo) Add(line string, note *[]byte, child *ListItem) error {
	r.hasPendingChanges = true

	if note == nil {
		note = &[]byte{}
	}
	newItem := ListItem{
		Line:  line,
		ID:    r.nextID,
		Child: child,
		Note:  note,
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

func (r *DBListRepo) Update(line string, note *[]byte, listItem *ListItem) error {
	listItem.Line = line
	listItem.Note = note
	r.hasPendingChanges = true
	return nil
}

func (r *DBListRepo) Delete(item *ListItem) error {
	r.hasPendingChanges = true

	if item.Child != nil {
		item.Child.Parent = item.Parent
	} else {
		// If the item has no child, it is at the top of the list and therefore we need to update the root
		r.root = item.Parent
	}

	if item.Parent != nil {
		item.Parent.Child = item.Child
	}

	r.pendingDeletions = append(r.pendingDeletions, item)

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
		if len(sub) >= 2 {
			// Inverse string match if a search group begins with `#!`
			if sub[1] == '!' {
				return !isSubString(string(sub[2:]), full)
			}
		}
		return isSubString(string(sub[1:]), full)
	}
	return isFuzzyMatch(sub, full)
}

func (r *DBListRepo) Match(keys [][]rune, active *ListItem, showHidden bool) ([]*ListItem, error) {
	/*For each line, iterate through each searchGroup. We should be left with lines with fulfil all groups. */
	cur := r.root

	res := make([]*ListItem, 0)

	if cur == nil {
		return res, nil
	}

	for {
		if showHidden || !cur.IsHidden {
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
		}

		if cur.Parent == nil {
			return res, nil
		}
		cur = cur.Parent
	}
}

func (r *DBListRepo) loadPage(id uint32) (*[]byte, error) {
	strID := fmt.Sprint(id)
	filePath := path.Join(r.notesPath, strID)

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

func (r *DBListRepo) savePage(id uint32, data *[]byte) error {
	strID := fmt.Sprint(id)
	filePath := path.Join(r.notesPath, strID)

	// Open or create a file in the `/notes/` subdir using the listItem ID as the file name
	// This needs to be before the ReadFile below to ensure the file exists
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0755)
	defer f.Close()

	_, err = f.Write(*data)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
		return err
	}
	return nil
}

func (r *DBListRepo) HasPendingChanges() bool {
	return r.hasPendingChanges
}
