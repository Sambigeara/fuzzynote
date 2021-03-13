package main

import (
	"fmt"
	"log"
	"os"
	"path"
	"time"
	//"runtime"

	"github.com/ardanlabs/conf"
	"github.com/gdamore/tcell"
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
		LocalRefreshFreqMs  uint16 `conf:"default:250"`
		RemoteRefreshFreqMs uint16 `conf:"default:500"`
		FullRefreshFreqMs   uint16 `conf:"default:60000"`
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
	cfg.S3.Prefix = "home"

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

	// Make sure the root directory exists
	os.Mkdir(cfg.Root, os.ModePerm)

	localWalFiles := []service.WalFile{service.NewLocalWalFile(cfg.Root)}
	remoteWalFiles := []service.WalFile{}

	if cfg.S3.Key != "" && cfg.S3.Secret != "" && cfg.S3.Bucket != "" && cfg.S3.Prefix != "" {
		s3FileWal := service.NewS3FileWal(
			cfg.S3.Key,
			cfg.S3.Secret,
			cfg.S3.Bucket,
			cfg.S3.Prefix,
			cfg.Root,
		)
		remoteWalFiles = append(remoteWalFiles, s3FileWal)
	}
	allWalFiles := append(localWalFiles, remoteWalFiles...)

	// Instantiate listRepo
	listRepo := service.NewDBListRepo(cfg.Root, allWalFiles)

	// List instantiation
	err = listRepo.Load(localWalFiles)
	if err != nil {
		log.Fatal(err)
	}

	partialRefreshTicker := time.NewTicker(time.Millisecond * time.Duration(cfg.LocalRefreshFreqMs))
	remoteRefreshTicker := time.NewTicker(time.Millisecond * time.Duration(cfg.RemoteRefreshFreqMs))
	fullRefreshTicker := time.NewTicker(time.Millisecond * time.Duration(cfg.FullRefreshFreqMs))

	term := client.NewTerm(listRepo, cfg.Colour)

	// We retrieve keypresses from tcell through a blocking function call which can't be used
	// in the main select below
	keyPressEvts := make(chan tcell.Event)
	go func() {
		for {
			select {
			case <-partialRefreshTicker.C:
				err := listRepo.Refresh(localWalFiles, nil, false)
				if err != nil {
					log.Fatalf("Failed on partial sync: %v", err)
				}
				term.S.PostEvent(&client.RefreshKey{})
			case <-remoteRefreshTicker.C:
				err := listRepo.Refresh(remoteWalFiles, nil, false)
				if err != nil {
					log.Fatalf("Failed on remote sync: %v", err)
				}
			case <-fullRefreshTicker.C:
				err := listRepo.Refresh(allWalFiles, nil, true)
				if err != nil {
					log.Fatalf("Failed on full sync: %v", err)
				}
				term.S.PostEvent(&client.RefreshKey{})
			case ev := <-keyPressEvts:
				cont, err := term.HandleKeyEvent(ev)
				if err != nil {
					log.Fatal(err)
				} else if !cont {
					err := listRepo.Save(allWalFiles)
					if err != nil {
						log.Fatal(err)
					}
					partialRefreshTicker.Stop()
					fullRefreshTicker.Stop()
					os.Exit(0)
				}
			}
		}
	}()

	for {
		keyPressEvts <- term.S.PollEvent()
	}
}
