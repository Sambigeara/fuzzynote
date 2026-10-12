// Command export materialises a fuzzynote WAL directory into a JSON Lines file:
//
//	go run ./cmd/export -root ~/.fzn -out notes.jsonl [-email you@example.com]
//
// One JSON object per list item, in display order, hidden (archived) items included.
// This is the migration bridge out of the Go/gob storage format.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/sambigeara/fuzzynote/pkg/service"
)

func main() {
	root := flag.String("root", "", "directory containing wal_*.db files (required)")
	out := flag.String("out", "", "output .jsonl path (default: stdout)")
	email := flag.String("email", "", "your fuzzynote login email, if any (keeps it out of friends lists)")
	flag.Parse()
	if *root == "" {
		flag.Usage()
		os.Exit(2)
	}

	tokens := service.NewFileWebTokenStore(*root)
	if *email != "" {
		tokens.SetEmail(*email)
	}
	repo := service.NewDBListRepo(service.NewLocalFileWalFile(*root), tokens)

	if err := repo.LoadLocal(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "load:", err)
		os.Exit(1)
	}
	items, err := repo.ExportItems()
	if err != nil {
		fmt.Fprintln(os.Stderr, "export:", err)
		os.Exit(1)
	}

	w := os.Stdout
	if *out != "" {
		f, err := os.Create(*out)
		if err != nil {
			fmt.Fprintln(os.Stderr, "create:", err)
			os.Exit(1)
		}
		defer f.Close()
		w = f
	}

	enc := json.NewEncoder(w)
	var hidden, noted, shared int
	var minT, maxT int64
	for _, it := range items {
		if err := enc.Encode(it); err != nil {
			fmt.Fprintln(os.Stderr, "encode:", err)
			os.Exit(1)
		}
		if it.Hidden {
			hidden++
		}
		if it.Note != "" {
			noted++
		}
		if len(it.Friends) > 0 {
			shared++
		}
		if t := it.CreatedUnixNano; t != 0 {
			if minT == 0 || t < minT {
				minT = t
			}
			if t > maxT {
				maxT = t
			}
		}
	}
	fmt.Fprintf(os.Stderr, "wals processed: %d\n", repo.ProcessedWalCount())
	fmt.Fprintf(os.Stderr, "items: %d (hidden %d, with note %d, shared %d)\n", len(items), hidden, noted, shared)
	if minT != 0 {
		fmt.Fprintf(os.Stderr, "created between %s and %s\n",
			time.Unix(0, minT).Format("2006-01-02"), time.Unix(0, maxT).Format("2006-01-02"))
	}
}
