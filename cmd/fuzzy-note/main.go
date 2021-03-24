package main

import (
	"fmt"
	"log"
	"os"
	"path"
	//"time"
	//"runtime"

	"github.com/ardanlabs/conf"
	"github.com/gdamore/tcell/v2"
	"gopkg.in/yaml.v2"
	//"github.com/rogpeppe/go-internal/lockedfile"

	"fuzzy-note/pkg/client"
	"fuzzy-note/pkg/service"
)

const refreshFile = "_refresh_lock.db"

func main() {
	//runtime.Breakpoint()

	// Before parsing generic config, we need to infer the project root, as there may be a config
	// file present, and we need to follow a certain precedence (CLI/envvars over file config)
	// This leads to a weird clienvvar/file/clienvvar parse process which seems weird, but works for now
	type s3 struct {
		Key    string `conf:"env:S3_KEY,flag:s3-key"`
		Secret string `conf:"env:S3_SECRET,flag:s3-secret"`
		Bucket string `conf:"env:S3_BUCKET,flag:s3-bucket"`
		Prefix string `conf:"env:S3_PREFIX,flag:s3-prefix"`
	}
	var cfg struct {
		Root                string
		Colour              string
		S3                  s3
		Editor              string `conf:"default:vim"`
		LocalRefreshFreqMs  uint16 `conf:"default:1000"`
		RemoteRefreshFreqMs uint16 `conf:"default:2000"`
		GatherRefreshFreqMs uint16 `conf:"default:10000"`
	}

	// Instantiate default root direct in case it's not set
	home, err := os.UserHomeDir()
	if err != nil {
		log.Fatal(err)
	}
	cfg.Root = path.Join(home, ".fzn/")

	// Override if set via CLI/envvar
	namespace := "FZN"
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

	// We can now specify our general config for use when parsing yaml (if available) and then CLI/envvars,
	// in that order.
	// Because we're using the same cfg to parse file and cli/envvar, we can't rely on the supported `default`
	// tags, so pre-set any defaults prior to parsing
	cfg.Colour = "light"
	cfg.S3.Prefix = "main"

	// Initially retrieve config from the config file, if available
	cfgFile := path.Join(cfg.Root, ".config.yml")
	f, err := os.Open(cfgFile)
	// If no errors (e.g. file definitely exists) attempt to parse config
	if err == nil {
		decoder := yaml.NewDecoder(f)
		err = decoder.Decode(&cfg)
		if err != nil {
			log.Fatalf("main : Parsing File Config : %v", err)
		}
		defer f.Close()
	}

	// Then run through CLI/envvar config retrieval, and override and settings if applicable
	if err := conf.Parse(os.Args[1:], namespace, &cfg); err != nil {
		log.Fatalf("main : Parsing Config : %v", err)
	}

	// Instantiate listRepo
	listRepo := service.NewDBListRepo(cfg.Root)

	// Create and register local app WalFile (based in root directory)
	localWalFile := service.NewLocalWalFile(cfg.LocalRefreshFreqMs, cfg.GatherRefreshFreqMs, cfg.Root)
	listRepo.RegisterWalFile(localWalFile)

	if cfg.S3.Key != "" && cfg.S3.Secret != "" && cfg.S3.Bucket != "" && cfg.S3.Prefix != "" {
		s3FileWal := service.NewS3FileWal(
			cfg.RemoteRefreshFreqMs,
			cfg.GatherRefreshFreqMs,
			cfg.S3.Key,
			cfg.S3.Secret,
			cfg.S3.Bucket,
			cfg.S3.Prefix,
			cfg.Root,
		)
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
