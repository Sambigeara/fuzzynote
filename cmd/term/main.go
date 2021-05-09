package main

import (
	"fmt"
	"log"
	"os"
	"path"

	"github.com/ardanlabs/conf"
	"github.com/gdamore/tcell/v2"

	"github.com/sambigeara/fuzzynote/pkg/prompt"
	"github.com/sambigeara/fuzzynote/pkg/service"
	"github.com/sambigeara/fuzzynote/pkg/term"
)

const (
	namespace  = "FZN"
	loginArg   = "login"
	remotesArg = "cfg"

	localRefreshFrequencyMs = 10000 // 10 seconds
	localGatherFrequencyMs  = 30000 // 30 seconds
	webRefreshFrequencyMs   = 10000 // 10 seconds
	webGatherFrequencyMs    = 60000 // 1 minute

	pushFrequencyMs = 10000
)

func main() {
	var cfg struct {
		Root   string
		Colour string `conf:"default:light"`
		Editor string `conf:"default:vim"`
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
	localWalFile := service.NewLocalFileWalFile(localRefreshFrequencyMs, localGatherFrequencyMs, cfg.Root)
	// Generate FileWebTokenStore
	webTokens := service.NewFileWebTokenStore(cfg.Root)
	// Instantiate listRepo
	listRepo := service.NewDBListRepo(
		cfg.Root,
		localWalFile,
		webTokens,
		pushFrequencyMs,
	)

	s3Remotes := service.GetS3Config(cfg.Root)
	for _, r := range s3Remotes {
		// centralise this logic across different remote types when relevant
		if (r.Mode == service.ModePush || r.Mode == service.ModeSync) && r.Match == "" && !r.MatchAll {
			log.Fatal("`match` or `matchall` must be explicitly set if mode is `push` or `sync`")
		}
		// TODO gracefully deal with missing config
		s3FileWal := service.NewS3FileWal(r, cfg.Root)
		listRepo.RegisterWalFile(s3FileWal)
	}

	// To avoid blocking key presses on the main processing loop, run heavy sync ops in a separate
	// loop, and only add to channel for processing if there's any changes that need syncing
	walChan := make(chan *[]service.EventLog)

	err = listRepo.Start(walChan)
	if err != nil {
		log.Fatal(err)
	}

	// Create term client
	t := term.NewTerm(listRepo, cfg.Colour, cfg.Editor)

	// We need atomicity between wal pull/replays and handling of keypress events, as we need
	// events to operate on a predictable state (rather than a keypress being applied to state
	// that differs from when the user intended due to async updates).
	// Therefore, we consume tcell events into a channel, and consume from it in the same loop
	// as the pull/replay loop.
	keyPressEvts := make(chan tcell.Event)
	go func() {
		for {
			select {
			case partialWal := <-walChan:
				if err := listRepo.Replay(partialWal); err != nil {
					log.Fatal(err)
				}
				t.S.PostEvent(&term.RefreshKey{})
			case ev := <-keyPressEvts:
				cont, err := t.HandleKeyEvent(ev)
				if err != nil {
					log.Fatal(err)
				} else if !cont {
					err := listRepo.Stop()
					if err != nil {
						log.Fatal(err)
					}
					os.Exit(0)
				}
			}
		}
	}()

	// This is the main loop of operation in the app.
	// We consume all term events into our own channel (handled above).
	for {
		keyPressEvts <- t.S.PollEvent()
	}
}
