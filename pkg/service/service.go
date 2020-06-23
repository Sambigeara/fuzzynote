package service

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"unicode"
)

type ListRepo interface {
	Load() (*ListItem, error)
	Save(*ListItem) error
	Add(line string, parent *ListItem, child *ListItem) (*ListItem, error)
	Update(line string, listItem *ListItem) error
	Delete(listItem ListItem) (*ListItem, error)
	Match(keys [][]rune, listItem *ListItem) (*ListItem, error)
	//OpenFile()
}

type DBListRepo struct {
	RootPath string
}

type List struct {
	RootPath  string // TODO convert to native path type
	ListItems []ListItem
	CurPos    int
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
	return &DBListRepo{rootPath}
}

func fetchPage(r io.Reader) ([]byte, error) {
	header := PageHeader{}
	err := binary.Read(r, binary.LittleEndian, &header)
	if err != nil {
		return nil, err
		//switch err {
		//case io.EOF:
		//    return data, nil
		//case io.ErrUnexpectedEOF:
		//    fmt.Println("binary.Read failed on page header:", err)
		//    return nil, err
		//}
	}

	data := make([]byte, header.DataLength)
	err = binary.Read(r, binary.LittleEndian, &data)
	if err != nil {
		return nil, err
		//switch err {
		//case io.EOF:
		//    return data, nil
		//case io.ErrUnexpectedEOF:
		//    fmt.Println("binary.Read failed on page data:", err)
		//    return nil, err
		//}
	}
	return data, nil
}

func (r *DBListRepo) Load() (*ListItem, error) {
	file, err := os.OpenFile(r.RootPath, os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
		return nil, err
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
				return nil, err
			}
		}

		nextItem := ListItem{
			Line:   string(data),
			Parent: cur,
		}
		cur = &nextItem
	}

	youngest := cur

	// `cur` is now a ptr to the most recent ListItem
	for {
		if cur.Parent == nil {
			break
		}
		cur.Parent.Child = cur
		cur = cur.Parent
	}

	return youngest, nil
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

	return nil
}

func (r *DBListRepo) Add(line string, child *ListItem, parent *ListItem) (*ListItem, error) {
	if child == nil && parent != nil {
		child = parent.Child
	} else if child != nil && parent == nil {
		parent = child.Parent
	} else {
		return nil, errors.New("Add requires either a child or parent ptr, not both or neither")
	}
	newItem := ListItem{
		Line:   line,
		Child:  child,
		Parent: parent,
	}
	if child != nil {
		child.Parent = &newItem
	}
	if parent != nil {
		parent.Child = &newItem
	}
	return &newItem, nil
}

func (r *DBListRepo) Update(line string, listItem *ListItem) error {
	listItem.Line = line
	return nil
}

func (r *DBListRepo) Delete(listItem *ListItem) (*ListItem, error) {
	if listItem.Child != nil {
		listItem.Child.Parent = listItem.Parent
	}
	if listItem.Parent != nil {
		listItem.Parent.Child = listItem.Child
	}
	// Always return root/youngest/top listItem
	var cur *ListItem
	if listItem.Child != nil {
		cur = listItem.Child
	} else if listItem.Parent != nil {
		cur = listItem.Parent
	} else {
		return nil, nil
	}
	for {
		if cur.Child == nil {
			return cur, nil
		}
		cur = cur.Child
	}
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

func (r *DBListRepo) Match(keys [][]rune, cur *ListItem) (*ListItem, error) {
	/*For each line, iterate through each searchGroup. We should be left with lines with fulfil all groups. */
	if cur == nil {
		return nil, nil
	}

	var res *ListItem
	for {
		matched := true
		for _, group := range keys {
			if !isMatch(group, cur.Line) {
				matched = false
				break
			}
		}
		if matched {
			// The first listItem will need to be generated, otherwise iterate forwards from the existing
			if res != nil {
				res = res.Parent
			}
			res = &ListItem{
				Line: cur.Line,
			}
		}

		if cur.Parent == nil {
			fmt.Printf("HELLOOOOOOOOO %v\n", res)
			return res, nil
		}
		cur = cur.Parent
	}
}
