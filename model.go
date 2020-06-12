package main

import (
	"time"
)

type SearchString struct {
	Keys [][]rune
}

type List struct {
	RootPath  string // TODO convert to native path type
	Search    SearchString
	ListItems []ListItem
	CurPos    int
}

type ListItem struct {
	Line      string
	DtCreated time.Time
}

func (p *List) FetchMatches(searchGroups [][]rune) ([]ListItem, error) {
	/*For each line, iterate through each searchGroup. We should be left with lines with fulfil all groups. */
	res := []ListItem{}
	for _, p := range p.ListItems {
		isMatch := true
		for _, group := range searchGroups {
			if !IsMatch(group, p.Line) {
				isMatch = false
				break
			}
		}
		if isMatch {
			res = append(res, p)
		}
	}
	return res, nil // TODO
}

func (p *List) AddListItem(pi ListItem) error {
	return nil
}
