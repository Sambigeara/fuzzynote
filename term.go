package main

import (
	"bufio"
	"errors"
	"fmt"
	"golang.org/x/crypto/ssh/terminal"
	"log"
	"os"
	"os/exec"
	"unicode"
)

type SearchString struct {
	Key []rune
}

type StdScr struct {
	Search SearchString
	Lines  []PageItem
	CurPos int
}

func (scr *StdScr) clearScreen() {
	// TODO cross platform screen-cleaning, or figure out how to clear stdout in place
	cmd := exec.Command("clear")
	cmd.Stdout = os.Stdout
	cmd.Run()
}

func (scr *StdScr) ProcessKeyPress(r rune) error {
	s := &scr.Search
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
	scr.clearScreen()

	// Set cursor highlight position
    //col := len(s.Key)
    //fmt.Printf("\033[%d;%dH", 1, col)
    fmt.Printf("\033[%d;%dH", 1, 1)

	return nil
}

func (scr *StdScr) Paint(matches []PageItem) error {
	fmt.Printf("%s\n\r", string(scr.Search.Key))
	for _, r := range matches {
		fmt.Printf("%s\n\r", r.Line)
	}
	return nil
}

func (scr *StdScr) HandleKeyPresses(db DbRepo) {
    oldState, err := terminal.MakeRaw(0)
    if err != nil {
            panic(err)
    }
    defer terminal.Restore(0, oldState)
	scr.clearScreen()

	reader := bufio.NewReader(os.Stdin)
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
		err = scr.ProcessKeyPress(r)
		if err != nil {
			log.Println("stdin:", err)
			break
		}
		matches, err := db.FetchMatches(scr.Search.Key)
		if err != nil {
			log.Println("stdin:", err)
			break
		}
		scr.Paint(matches)
	}
}
