package service

import (
	"strings"
	"unicode"
)

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

const (
	openOp  rune = '{'
	closeOp rune = '}'
)

type MatchPattern int

const (
	FullMatchPattern MatchPattern = iota
	InverseMatchPattern
	FuzzyMatchPattern
	NoMatchPattern
)

// matchChars represents the number of characters at the start of the string
// which are attributed to the match pattern.
// This is used elsewhere to strip the characters where appropriate

func GetNewLinePrefix(search [][]rune) string {
	var searchStrings []string
	for _, group := range search {
		pattern, nChars := GetMatchPattern(group)
		if pattern != InverseMatchPattern && len(group) > 0 {
			searchStrings = append(searchStrings, string(group[nChars:]))
		}
	}
	newString := ""
	if len(searchStrings) > 0 {
		newString = strings.Join(searchStrings, " ") + " "
	}
	return newString
}

// GetMatchPattern will return the MatchPattern of a given string, if any, plus the number
// of chars that can be omitted to leave only the relevant text
func GetMatchPattern(sub []rune) (MatchPattern, int) {
	if len(sub) == 0 {
		return NoMatchPattern, 0
	}
	switch sub[0] {
	case '~':
		return FuzzyMatchPattern, 1
	case '!':
		return InverseMatchPattern, 1
	}
	return FullMatchPattern, 0
}

// If a matching group starts with `=` do a substring match, otherwise do a fuzzy search
func isMatch(sub []rune, full string, pattern MatchPattern) bool {
	if len(sub) == 0 {
		return true
	}
	switch pattern {
	case FullMatchPattern:
		return isSubString(string(sub), full)
	case InverseMatchPattern:
		return !isSubString(string(sub), full)
	case FuzzyMatchPattern:
		return isFuzzyMatch(sub, full)
	default:
		// Shouldn't reach here
		return false
	}
}
