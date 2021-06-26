package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/ardanlabs/conf"

	"github.com/sambigeara/fuzzynote/pkg/prompt"
	"github.com/sambigeara/fuzzynote/pkg/service"
	"github.com/sambigeara/fuzzynote/pkg/term"
)

const (
	namespace  = "FZN"
	loginArg   = "login"
	deleteArg  = "delete"
	remotesArg = "cfg"
)

var (
	version = "development"
	date    = "0" // the linker passes a unixtime string which needs to be converted
)

func main() {
	var cfg struct {
		Version           conf.Version
		Root              string
		Colour            string `conf:"default:light"`
		Editor            string `conf:"default:vim"`
		SyncFrequencyMs   uint16 `conf:"default:10000"`
		GatherFrequencyMs uint16 `conf:"default:30000"`
	}

	// Pre-instantiate default root direct (can't pass value dynamically to default above)
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}
	cfg.Root = path.Join(home, ".fzn/")

	// Override if set via CLI/envvar
	if err := conf.Parse(os.Args[1:], namespace, &cfg); err != nil {
		// Handle `--help` on first attempt of parsing inputs
		if err == conf.ErrHelpWanted {
			usage, err := conf.Usage(namespace, &cfg)
			if err != nil {
				log.Fatalf("generating config usage: %s", err)
			}
			fmt.Println(usage)
			os.Exit(0)
		} else if err == conf.ErrVersionWanted {
			// Convert to ISO-8601 date, fail silently if unable
			dateString := "1970-01-01"
			i, err := strconv.ParseInt(date, 10, 64)
			if err == nil {
				unixTime := time.Unix(i, 0)
				dateString = unixTime.Format("2006-01-02")
			}
			fmt.Printf("fuzzynote %s (%s)\n", version, dateString)
			os.Exit(0)
		}
		log.Fatalf("main : Parsing Root Config : %v", err)
	}

	// Make sure the root directory exists
	// This also occurs in NewDBListRepo, but is required in the Login/WebToken flows below, so ensure
	// existence here.
	os.Mkdir(cfg.Root, os.ModePerm)

	// Create and register local app WalFile (based in root directory)
	localWalFile := service.NewLocalFileWalFile(cfg.Root)

	// Check for Login or Remotes management flow (run and exit - bypassing the main program)
	// TODO atm only triggers on last arg, make smarter!
	if len(os.Args) > 1 {
		switch os.Args[len(os.Args)-1] {
		case loginArg:
			prompt.Login(cfg.Root)
		case deleteArg:
			localWalFile.Purge(nil)
		case remotesArg:
			webTokens := service.NewFileWebTokenStore(cfg.Root)
			web := service.NewWeb(webTokens)
			prompt.LaunchRemotesCLI(web, localWalFile)
		}
	}

	// Generate FileWebTokenStore
	webTokens := service.NewFileWebTokenStore(cfg.Root)

	// Instantiate listRepo
	listRepo := service.NewDBListRepo(
		localWalFile,
		webTokens,
		cfg.SyncFrequencyMs,
		cfg.GatherFrequencyMs,
	)

	s3Remotes := service.GetS3Config(cfg.Root)
	for _, r := range s3Remotes {
		// centralise this logic across different remote types when relevant
		// TODO gracefully deal with missing config
		s3FileWal := service.NewS3WalFile(r, cfg.Root)
		listRepo.RegisterWalFile(s3FileWal)
	}

	walChan := make(chan *[]service.EventLog)
	// TODO stricter control around event type
	//inputEvtsChan := make(chan tcell.Event)
	inputEvtsChan := make(chan interface{})

	// Create term client
	client := term.NewTerm(listRepo, cfg.Colour, cfg.Editor)

	err = listRepo.Start(client, walChan, inputEvtsChan)
	if err != nil {
		log.Fatal(err)
	}

	// This is the main loop of operation in the app.
	// We consume all term events into our own channel (handled above).
	for {
		inputEvtsChan <- client.AwaitEvent()
	}
}
