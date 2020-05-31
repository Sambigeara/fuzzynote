package main

import (
	"bufio"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"
	"unicode"

	"golang.org/x/crypto/ssh/terminal"
)

// TODO Will be path eventually, read from env var
const Root = "rootPage"

type PageItem struct {
	Line      string
	DtCreated time.Time
}

func loadRootPage() []PageItem {
	file, err := os.Open(Root)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	page := []PageItem{}

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		t := scanner.Text()
		pageItem := PageItem{t, time.Now()} // TODO need a way to persist datetime in files
		page = append(page, pageItem)
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return page
}

type SearchString struct {
	Key []rune
}

func clearScreen() {
	// TODO cross platform screen-cleaning, or figure out how to clear stdout in place
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	cmd.Run()
}

func (s *SearchString) ProcessKeyPress(r rune) error {
	if unicode.IsLetter(r) || unicode.IsNumber(r) {
		s.Key = append(s.Key, r)
	} else if r == '\u007f' {
		// Delete removes last item from slice
		if len(s.Key) > 0 {
			s.Key = s.Key[:len(s.Key)-1]
		}
	} else if r == '\x1b' {
		return errors.New("Esc: exiting...")
	}
	clearScreen()
	return nil
}

// TODO move method from SearchString
func (s *SearchString) FetchMatches(page []PageItem) ([]PageItem, error) {
	res := []PageItem{}
	for _, p := range page {
		if IsFuzzyMatch(string(s.Key), p.Line) {
			res = append(res, p)
		}
	}
	return res, nil // TODO
}

// TODO this will represent the painted output with data and cursor position
type StdScr struct {
	SearchString []SearchString
	Lines        []PageItem
	CursX        int
	CursY        int
	LenX         int
	LenY         int
}

func (scr *StdScr) Paint() {
}

func main() {
	page := loadRootPage()

	//https://godoc.org/golang.org/x/crypto/ssh/terminal
	oldState, err := terminal.MakeRaw(0)
	if err != nil {
		panic(err)
	}
	defer clearScreen()
	defer terminal.Restore(0, oldState)

	clearScreen()

	reader := bufio.NewReader(os.Stdin)
	s := SearchString{}
	for {
		// Retrieve single byte
		//ascii, _ := reader.ReadByte()
		//fmt.Print(ascii)

		// Retrieve single UTF-8 encoded Unicode char
		// TODO at the mo many buttons start with escape sequence \x1b, so many chars trigger exit
		r, _, err := reader.ReadRune()
		if err != nil {
			log.Println("stdin:", err)
			break
		}
		err = s.ProcessKeyPress(r)
		if err != nil {
			log.Println("stdin:", err)
			break
		}

		matches, err := s.FetchMatches(page)
		if err != nil {
			log.Println("stdin:", err)
			break
		}

		fmt.Printf("%s\n\r", string(s.Key))
		for _, r := range matches {
			fmt.Printf("%s\n\r", r.Line)
		}
		// Set cursor position
		line, col := 1, 1
		fmt.Printf("\033[%d;%dH", line, col)
	}
}
