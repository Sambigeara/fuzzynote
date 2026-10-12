package service

import (
	"context"
	"strconv"
	"strings"
)

// ExportedItem is a language-neutral snapshot of a single list item, emitted in display order.
// It exists to migrate data out of the Go/gob WAL format.
type ExportedItem struct {
	Index      int    `json:"index"`
	Key        string `json:"key"`
	OriginUUID uint32 `json:"origin_uuid"`
	Lamport    int64  `json:"lamport"`
	// CreatedUnixNano and UpdatedUnixNano are only populated when the timestamps embedded in
	// legacy (schema <= 6) keys and events look like wall-clock unix nanoseconds.
	CreatedUnixNano int64    `json:"created_unix_nano,omitempty"`
	UpdatedUnixNano int64    `json:"updated_unix_nano,omitempty"`
	Line            string   `json:"line"`
	RawLine         string   `json:"raw_line,omitempty"` // only set when it differs from Line (i.e. carries @friends)
	Note            string   `json:"note,omitempty"`
	Hidden          bool     `json:"hidden"`
	Friends         []string `json:"friends,omitempty"`
}

// LoadLocal synchronously pulls and replays every WAL file found in the local WalFile root.
func (r *DBListRepo) LoadLocal(ctx context.Context) error {
	c := make(chan namedWal)
	done := make(chan error, 1)
	go func() {
		for n := range c {
			if err := r.Replay(n.wal); err != nil {
				done <- err
				for range c { // drain so the producer can finish
				}
				return
			}
			if n.name != "" {
				r.setProcessedWalChecksum(n.name)
			}
		}
		done <- nil
	}()
	err := r.pull(ctx, []WalFile{r.LocalWalFile}, c)
	close(c)
	if rerr := <-done; rerr != nil {
		return rerr
	}
	return err
}

// ProcessedWalCount reports how many WAL files were successfully parsed and replayed.
func (r *DBListRepo) ProcessedWalCount() int {
	r.processedWalChecksumLock.Lock()
	defer r.processedWalChecksumLock.Unlock()
	return len(r.processedWalChecksums)
}

// Anything above this is treated as a wall-clock unix nanosecond timestamp (2001-09-09 onwards).
const plausibleUnixNanoFloor = int64(1_000_000_000_000_000_000)

func unixNanoIfPlausible(ts int64) int64 {
	if ts > plausibleUnixNanoFloor {
		return ts
	}
	return 0
}

// ExportItems returns every list item, hidden ones included, in display order.
func (r *DBListRepo) ExportItems() ([]ExportedItem, error) {
	items, _, err := r.Match([][]rune{}, true, "", 0, 0)
	if err != nil {
		return nil, err
	}
	out := make([]ExportedItem, 0, len(items))
	for i, it := range items {
		e := ExportedItem{
			Index:   i,
			Key:     it.key,
			Line:    it.Line(),
			Note:    string(it.Note),
			Hidden:  it.IsHidden,
			Friends: it.Friends(),
		}
		if it.rawLine != e.Line {
			e.RawLine = it.rawLine
		}
		if parts := strings.SplitN(it.key, ":", 2); len(parts) == 2 {
			if u, err := strconv.ParseUint(parts[0], 10, 32); err == nil {
				e.OriginUUID = uint32(u)
			}
			if ts, err := strconv.ParseInt(parts[1], 10, 64); err == nil {
				e.Lamport = ts
				e.CreatedUnixNano = unixNanoIfPlausible(ts)
			}
		}
		if ue, ok := r.crdt.addEventSet[it.key]; ok {
			e.UpdatedUnixNano = unixNanoIfPlausible(ue.LamportTimestamp)
		}
		out = append(out, e)
	}
	return out, nil
}
