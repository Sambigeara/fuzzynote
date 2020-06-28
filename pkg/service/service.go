package service

import (
	"encoding/binary"
	//"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"unicode"
)

type ListRepo interface {
	Load() error
	Save(*ListItem) error
	Add(line string, item *ListItem, addAsChild bool) (*ListItem, error)
	Update(line string, listItem *ListItem) error
	Delete(listItem *ListItem) error
	Match(keys [][]rune, active *ListItem) ([]*ListItem, error)
	GetRoot() *ListItem
	//OpenFile()
}

type DBListRepo struct {
	RootPath string
	Root     *ListItem
}

type ListItem struct {
	Line   string
	Parent *ListItem
	Child  *ListItem
}

// FileHeader will store the schema id, so we know which pageheader to use
type FileHeader struct {
	SchemaID uint32
}

type PageHeader struct {
	PageID     uint32
	FileID     uint64
	DataLength uint64
}

func NewDBListRepo(rootPath string) *DBListRepo {
	return &DBListRepo{RootPath: rootPath}
}

func fetchPage(r io.Reader) ([]byte, error) {
	header := PageHeader{}
	err := binary.Read(r, binary.LittleEndian, &header)
	if err != nil {
		return nil, err
	}

	data := make([]byte, header.DataLength)
	err = binary.Read(r, binary.LittleEndian, &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (r *DBListRepo) Load() error {
	file, err := os.OpenFile(r.RootPath, os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer file.Close()

	// Retrieve first line from the file, which will be the oldest (and therefore bottom) entry
	var cur *ListItem

OuterLoop:
	for {
		data, err := fetchPage(file)
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
		}
		cur = &nextItem
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

	return nil
}

// TODO untangle boundaries between data store and local data model
// TODO should not require string - should be a method attached to a datastore with config baked in
func writeListItemToFile(l *ListItem, file *os.File) error {
	var header = PageHeader{
		PageID:     1, // TODO id generator
		FileID:     1, // TODO
		DataLength: uint64(len((*l).Line)),
	}
	err := binary.Write(file, binary.LittleEndian, &header)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
		return err
	}

	data := []byte((*l).Line)
	err = binary.Write(file, binary.LittleEndian, &data)
	if err != nil {
		fmt.Println("binary.Write failed:", err)
		return err
	}
	return nil
}

func (r *DBListRepo) Save(listItem *ListItem) error {
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
		err = writeListItemToFile(listItem, file)
		if err != nil {
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

func (r *DBListRepo) GetRoot() *ListItem {
	return r.Root
}
