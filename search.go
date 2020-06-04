package main

import (
	"strings"
)

func isSubString(sub string, full string) bool {
	if strings.Contains(full, sub) {
		return true
	}
	return false
}

func IsFuzzyMatch(sub []rune, full string) bool {
	/*
			   Iterate through the full string, when you match the "head" of the sub rune slice, pop it and continue through. If you clear sub, return true.
		       Searches in O(n)
	*/
	for _, c := range full {
		if len(sub) == 0 {
			return true
		}
		if c == sub[0] {
			_, sub = sub[0], sub[1:]
		}
	}
	return false
}
