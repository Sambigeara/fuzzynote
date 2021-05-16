package main

import (
	"fmt"
	"log"
	"os"
	"path"

	"github.com/ardanlabs/conf"

	"github.com/sambigeara/fuzzynote/pkg/prompt"
	"github.com/sambigeara/fuzzynote/pkg/service"
	"github.com/sambigeara/fuzzynote/pkg/term"
)

const (
	namespace  = "FZN"
	loginArg   = "login"
	remotesArg = "cfg"
)

func main() {
	var cfg struct {
		Root            string
		Colour          string `conf:"default:light"`
		Editor          string `conf:"default:vim"`
		SyncFrequency   uint16 `conf:"default:10000"`
		GatherFrequency uint16 `conf:"default:30000"`
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
		}
		log.Fatalf("main : Parsing Root Config : %v", err)
	}

	// Make sure the root directory exists
	// This also occurs in NewDBListRepo, but is required in the Login/WebToken flows below, so ensure
	// existence here.
	os.Mkdir(cfg.Root, os.ModePerm)

	// Check for Login or Remotes management flow (run and exit - bypassing the main program)
	// TODO atm only triggers on last arg, make smarter!
	if len(os.Args) > 1 {
		switch os.Args[len(os.Args)-1] {
		case loginArg:
			prompt.Login(cfg.Root)
		case remotesArg:
			webTokens := service.NewFileWebTokenStore(cfg.Root)
			web := service.NewWeb(webTokens)
			prompt.LaunchRemotesCLI(web)
		}
	}

	// Create and register local app WalFile (based in root directory)
	localWalFile := service.NewLocalFileWalFile(cfg.Root)

	// Generate FileWebTokenStore
	webTokens := service.NewFileWebTokenStore(cfg.Root)

	// Instantiate listRepo
	listRepo := service.NewDBListRepo(
		localWalFile,
		webTokens,
		cfg.SyncFrequency,
		cfg.GatherFrequency,
	)

	s3Remotes := service.GetS3Config(cfg.Root)
	for _, r := range s3Remotes {
		// centralise this logic across different remote types when relevant
		if (r.Mode == service.ModePush || r.Mode == service.ModeSync) && r.Match == "" && !r.MatchAll {
			log.Fatal("`match` or `matchall` must be explicitly set if mode is `push` or `sync`")
		}
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
