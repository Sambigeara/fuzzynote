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
	Line       string
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

type searchString struct {
    Key []rune
}

func clearScreen() {
    // TODO cross platform screen-cleaning, or figure out how to clear stdout in place
    cmd := exec.Command("clear")
    cmd.Stdout = os.Stdout
    cmd.Run()
}

func (s *searchString) ProcessKeyPress(r rune) error {
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

func (s *searchString) FetchMatches(page []PageItem) ([]string, error) {
    res := []string{}
    for _, p := range page {
        if IsFuzzyMatch(string(s.Key), p.Line) {
            res = append(res, p.Line)
        }
    }
    return res, nil // TODO
}

func main() {
    page := loadRootPage()

    //https://godoc.org/golang.org/x/crypto/ssh/terminal
    oldState, err := terminal.MakeRaw(0)
    if err != nil {
            panic(err)
    }
    defer terminal.Restore(0, oldState)

    clearScreen()

	reader := bufio.NewReader(os.Stdin)
    s := searchString{}
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

        fmt.Printf("%s\r", string(s.Key))
        for _, r := range matches {
            fmt.Printf("%s\n", r)
        }
	}
}
