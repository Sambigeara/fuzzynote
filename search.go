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

func IsFuzzyMatch(sub string, full string) bool {
	return isSubString(sub, full)
}
