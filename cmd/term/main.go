package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path"
	"strconv"
	"time"

	"github.com/ardanlabs/conf"

	"github.com/sambigeara/fuzzynote/pkg/prompt"
	"github.com/sambigeara/fuzzynote/pkg/s3"
	"github.com/sambigeara/fuzzynote/pkg/service"
	"github.com/sambigeara/fuzzynote/pkg/term"
)

const (
	namespace = "FZN"
	loginArg  = "login"
	deleteArg = "delete"
	importArg = "import"
)

var (
	version = "development"
	date    = "0" // the linker passes a unixtime string which needs to be converted
)

func main() {
	var cfg struct {
		Version conf.Version
		Root    string
		Colour  string `conf:"default:light"`
		Editor  string `conf:"default:vim"`
		//SyncFrequencyMs   uint32 `conf:"default:10000"`
		//GatherFrequencyMs uint32 `conf:"default:30000"`
		Args conf.Args
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
	if len(cfg.Args) > 0 {
		switch cfg.Args.Num(0) {
		case loginArg:
			prompt.Login(cfg.Root)
		case deleteArg:
			localWalFile.Purge()
		case importArg:
			// Gather and assert existence of the remaining args.
			// Bit of an odd way of handling it, but we need to assert existence of `--show` or `--hide` explicitly, and then accept any
			// arbitrary input for the file path (within reason)
			filePath := ""
			visibilityArg := ""
			for i := 1; i <= 2; i++ {
				switch a := cfg.Args.Num(i); a {
				case "--show":
					visibilityArg = "s"
				case "--hide":
					visibilityArg = "h"
				default:
					filePath = a
				}
			}
			if filePath == "" || visibilityArg == "" {
				fmt.Println("please specify imported item visibility via one of: `--show` or `--hide`.\ne.g: `./fzn import --show path/to/file`")
				os.Exit(0)
			}

			hideItems := false
			if visibilityArg == "h" {
				hideItems = true
			}

			curWd, err := os.Getwd()
			if err != nil {
				fmt.Println("failed to retrieve local directory")
				os.Exit(1)
			}

			f, err := os.Open(path.Join(curWd, filePath))
			if err != nil {
				fmt.Println("failed to open plain text file:", filePath)
				os.Exit(0)
			}
			defer f.Close()

			if err := service.BuildWalFromPlainText(context.Background(), localWalFile, f, hideItems); err != nil {
				fmt.Println("failed to generate wal file from plain text file")
				os.Exit(1)
			}
			os.Exit(0)
		default:
			fmt.Println("unrecognised arg:", cfg.Args.Num(0))
			os.Exit(0)
		}
	}

	// Generate FileWebTokenStore
	webTokens := service.NewFileWebTokenStore(cfg.Root)

	// Instantiate listRepo
	listRepo := service.NewDBListRepo(
		localWalFile,
		webTokens,
		//cfg.SyncFrequencyMs,
		//cfg.GatherFrequencyMs,
	)

	s3Remotes := s3.GetS3Config(cfg.Root)
	for _, r := range s3Remotes {
		// centralise this logic across different remote types when relevant
		// TODO gracefully deal with missing config
		s3FileWal := s3.NewS3WalFile(r, cfg.Root)
		listRepo.AddWalFile(s3FileWal, true)
	}

	// Create term client
	client := term.NewTerm(listRepo, cfg.Colour, cfg.Editor)

	err = listRepo.Start(client)
	if err != nil {
		log.Fatal(err)
	}
	os.Exit(0)
}
