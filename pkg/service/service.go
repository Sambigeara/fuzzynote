package service

import (
	"encoding/binary"
	//"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"unicode"
)

type ListRepo interface {
	Load() (*ListItem, error)
	Save(*ListItem) error
	Add(line string, item *ListItem, addAsChild bool) (*ListItem, error)
	Update(line string, listItem *ListItem) error
	Delete(listItem *ListItem) error
	Match(keys [][]rune, active *ListItem) ([]*ListItem, error)
	//GetPage(id uint32) (io.Reader, error)
	//SavePage()
}

type DBListRepo struct {
	RootPath string
	Root     *ListItem
	NextID   uint32
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

func NewDBListRepo(rootDir string) *DBListRepo {
	rootPath := path.Join(rootDir, "primary.db")
	return &DBListRepo{RootPath: rootPath}
}

func (r *DBListRepo) Load() (*ListItem, error) {
	file, err := os.OpenFile(r.RootPath, os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	defer file.Close()

	// Retrieve first line from the file, which will be the oldest (and therefore bottom) entry
	var cur, oldest *ListItem

	r.NextID = 1

OuterLoop:
	for {

		header := ItemHeader{}
		err := binary.Read(file, binary.LittleEndian, &header)

		if err != nil {
			switch err {
			case io.EOF:
				break OuterLoop
			case io.ErrUnexpectedEOF:
				fmt.Println("binary.Read failed on page header:", err)
				return nil, err
			}
		}

		// Initially we need to find the next available index for the ENTIRE dataset
		if header.PageID > r.NextID {
			r.NextID = header.PageID + 1
		}

		data := make([]byte, header.DataLength)
		err = binary.Read(file, binary.LittleEndian, &data)

		if err != nil {
			switch err {
			case io.EOF:
				break OuterLoop
			case io.ErrUnexpectedEOF:
				fmt.Println("binary.Read failed on page header:", err)
				return nil, err
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
		return nil, nil
	}

	// Now we have know the global NextID (to account for unordered IDs), iterate through (from oldest to youngest) and assign any indexes where required.
	for {
		if oldest.Child == nil {
			break
		}
		if oldest.ID == 0 {
			oldest.ID = r.NextID
			r.NextID++
		}
		oldest = oldest.Child
	}

	r.Root = cur

	// `cur` is now a ptr to the most recent ListItem
	for {
		if cur.Parent == nil {
			break
		}
		cur.Parent.Child = cur
		cur = cur.Parent
	}

	return r.Root, nil
}

// TODO untangle boundaries between data store and local data model
// TODO should not require string - should be a method attached to a datastore with config baked in
func writeListItemToFile(l *ListItem, file *os.File) error {
	header := ItemHeader{
		PageID:     0, // TODO id generator
		Metadata:   0, // TODO
		FileID:     0, // TODO
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

func (r *DBListRepo) Save(listItem *ListItem) error {
	// Account for edge condition where Load hasn't been run, and the ID is incorrectly set to 0
	if r.NextID == 0 {
		r.NextID = 1
	}

	// TODO save to temp file and then transfer over

	// TODO when appending individual item rather than overwriting
	//file, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	file, err := os.Create(r.RootPath)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer file.Close()

	root := listItem

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
			listItem.ID = r.NextID
			r.NextID++
		}
		header := ItemHeader{
			PageID:     listItem.ID,
			Metadata:   0,           // TODO
			FileID:     listItem.ID, // TODO
			DataLength: uint64(len(listItem.Line)),
		}

		// TODO the below writes need to be atomic
		err := binary.Write(file, binary.LittleEndian, &header)
		if err != nil {
			fmt.Println("binary.Write failed:", err)
			log.Fatal(err)
			return err
		}
		data := []byte(listItem.Line)
		err = binary.Write(file, binary.LittleEndian, &data)
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

	r.Root = root

	return nil
}

func (r *DBListRepo) Add(line string, item *ListItem, addAsChild bool) (*ListItem, error) {
	newItem := ListItem{
		Line: line,
		ID:   r.NextID,
	}
	r.NextID++

	// If `item` is nil, it's the first item in the list so set as root and return
	if item == nil {
		r.Root = &newItem
		return &newItem, nil
	}

	if !addAsChild {
		newItem.Child = item
		newItem.Parent = item.Parent

		oldParent := item.Parent
		item.Parent = &newItem
		if oldParent != nil {
			oldParent.Child = &newItem
		}
	} else {
		// If the item was the previous youngest, update the root to the new item
		if item.Child == nil {
			r.Root = &newItem
		}

		newItem.Parent = item
		newItem.Child = item.Child

		oldChild := item.Child
		item.Child = &newItem
		if oldChild != nil {
			oldChild.Parent = &newItem
		}
	}

	return &newItem, nil
}

func (r *DBListRepo) Update(line string, listItem *ListItem) error {
	listItem.Line = line
	return nil
}

func (r *DBListRepo) Delete(item *ListItem) error {
	if item.Child != nil {
		item.Child.Parent = item.Parent
	} else {
		// If the item has no child, it is at the top of the list and therefore we need to update the root
		r.Root = item.Parent
	}

	if item.Parent != nil {
		item.Parent.Child = item.Child
	}

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
	cur := r.Root

	res := make([]*ListItem, 0)

	if cur == nil {
		return res, nil
	}

	for {
		matched := true
		for _, group := range keys {
			if cur == active {
				// "active" listItems pass automatically to allow mid-search item editing
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

//func (r *DBListRepo) GetPage(id uint32) (io.Reader, error) {
//}
