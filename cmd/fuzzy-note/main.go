package main

import (
	"fmt"
	"log"
	"os"
	"path"

	"github.com/ardanlabs/conf"
	"github.com/gdamore/tcell/v2"

	"fuzzy-note/pkg/client"
	"fuzzy-note/pkg/service"
)

const (
	namespace   = "FZN"
	refreshFile = "_refresh_lock.db"
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

	//cfg.Colour = "light"
	//cfg.S3.Prefix = "main"

	localRefreshFrequency := uint16(1000)
	localGatherFrequency := uint16(10000)

	// Create and register local app WalFile (based in root directory)
	localWalFile := service.NewLocalWalFile(localRefreshFrequency, localGatherFrequency, cfg.Root)

	// Instantiate listRepo
	listRepo := service.NewDBListRepo(cfg.Root, localWalFile, localRefreshFrequency)

	// We explicitly pass the localWalFile to the listRepo above because it ultimately gets attached to the
	// Wal independently (there are certain operations that require us to only target the local walfile rather
	// that all).
	// We still need to register it as we all all walfiles in the next line.
	listRepo.RegisterWalFile(localWalFile)

	remotes := service.GetRemotesConfig(cfg.Root)

	for _, r := range remotes.S3 {
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

	// Create terminal client
	term := client.NewTerm(listRepo, cfg.Colour, cfg.Editor)

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
				term.S.PostEvent(&client.RefreshKey{})
			case ev := <-keyPressEvts:
				cont, err := term.HandleKeyEvent(ev)
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
		keyPressEvts <- term.S.PollEvent()
	}
}
