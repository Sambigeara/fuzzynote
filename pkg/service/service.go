package service

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"unicode"
)

type ListRepo interface {
	Load() ([]ListItem, error)
	Save([]ListItem) error
	Add(line string, idx int, listItems *[]ListItem) ([]ListItem, error)
	Update(line string, idx int, listItems *[]ListItem) error
	Delete(idx int, listItems *[]ListItem) ([]ListItem, error)
	Match(keys [][]rune, listItems *[]ListItem) ([]ListItem, error)
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
	Line string
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

func PrependListArray(arr []ListItem, l ListItem) []ListItem {
	arr = append(arr, l)
	copy(arr[1:], arr)
	arr[0] = l
	return arr
}

func (r *DBListRepo) Load() ([]ListItem, error) {
	file, err := os.OpenFile(r.RootPath, os.O_CREATE, 0644)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	defer file.Close()

	listItems := make([]ListItem, 0)
	for {
		header := PageHeader{}
		err := binary.Read(file, binary.LittleEndian, &header)
		if err != nil {
			switch err {
			case io.EOF:
				return listItems, nil
			case io.ErrUnexpectedEOF:
				fmt.Println("binary.Read failed on page header:", err)
				return nil, err
			}
		}

		data := make([]byte, header.DataLength)
		err = binary.Read(file, binary.LittleEndian, &data)
		if err != nil {
			switch err {
			case io.EOF:
				return listItems, nil
			case io.ErrUnexpectedEOF:
				fmt.Println("binary.Read failed on page data:", err)
				return nil, err
			}
		}
		listItems = PrependListArray(listItems, ListItem{string(data)})
	}
}

// TODO untangle boundaries between data store and local data model
// TODO should not require string - should be a method attached to a datastore with config baked in
func writeListItemToFile(l ListItem, file *os.File) error {
	var header = PageHeader{
		PageID:     1, // TODO id generator
		FileID:     1, // TODO
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

func (r *DBListRepo) Save(listItems []ListItem) error {
	// TODO save to temp file and then transfer over

	// TODO when appending individual item rather than overwriting
	//file, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

	file, err := os.Create(r.RootPath)
	if err != nil {
		log.Fatal(err)
		return err
	}
	defer file.Close()

	for i := len(listItems) - 1; i >= 0; i-- {
		err = writeListItemToFile(listItems[i], file)
		if err != nil {
			log.Fatal(err)
			return err
		}
	}
	return nil
}

func (r *DBListRepo) Add(line string, idx int, listItems *[]ListItem) ([]ListItem, error) {
	l := *listItems
	l = append(l, ListItem{})
	item := ListItem{line}
	if idx != len(l) {
		l = append(l, item)
		copy(l[idx+1:], l[idx:])
		l[idx] = item
	}
	return l, nil
}

func (r *DBListRepo) Update(line string, idx int, listItems *[]ListItem) error {
	(*listItems)[idx].Line = line
	return nil
}

func (r *DBListRepo) Delete(idx int, listItems *[]ListItem) ([]ListItem, error) {
	//l := *listItems
	//l = append(l[:idx], l[idx+1:]...)
	//*listItems = l
	res := append((*listItems)[:idx], (*listItems)[idx+1:]...)
	return res, nil
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

func (r *DBListRepo) Match(keys [][]rune, listItems *[]ListItem) ([]ListItem, error) {
	/*For each line, iterate through each searchGroup. We should be left with lines with fulfil all groups. */
	res := []ListItem{}
	for _, i := range *listItems {
		matched := true
		for _, group := range keys {
			if !isMatch(group, i.Line) {
				matched = false
				break
			}
		}
		if matched {
			res = append(res, i)
		}
	}
	return res, nil // TODO
}
