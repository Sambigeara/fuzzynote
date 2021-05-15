package service

import (
	"log"
	"os"
)

type fileHeader struct {
	SchemaID fileSchemaID
	UUID     uuid
	// TODO introduce migration to remove this legacy ID
	NextListItemID uint64
}

type listItemSchema1 struct {
	PageID     uint32
	Metadata   bits
	LineLength uint64
	NoteLength uint64
}

// Start begins push/pull for all WalFiles
func (r *DBListRepo) Start(client Client, walChan chan *[]EventLog, inputEvtsChan chan interface{}) error {
	// To avoid blocking key presses on the main processing loop, run heavy sync ops in a separate
	// loop, and only add to channel for processing if there's any changes that need syncing
	err := r.startSync(walChan)
	if err != nil {
		return err
	}

	// We need atomicity between wal pull/replays and handling of keypress events, as we need
	// events to operate on a predictable state (rather than a keypress being applied to state
	// that differs from when the user intended due to async updates).
	// Therefore, we consume client events into a channel, and consume from it in the same loop
	// as the pull/replay loop.
	go func() {
		for {
			select {
			case partialWal := <-walChan:
				if err := r.Replay(partialWal); err != nil {
					log.Fatal(err)
				}
				client.Refresh()
			case ev := <-inputEvtsChan:
				cont, err := client.HandleEvent(ev)
				if err != nil {
					log.Fatal(err)
				} else if !cont {
					err := r.Stop()
					if err != nil {
						log.Fatal(err)
					}
					os.Exit(0)
				}
			}
		}
	}()

	return nil
}

// Stop is called on app shutdown. It flushes all state changes in memory to disk
func (r *DBListRepo) Stop() error {
	fakeCtx := ""
	err := r.LocalWalFile.Stop(uint32(r.uuid), fakeCtx)
	if err != nil {
		return err
	}

	err = r.finish()
	if err != nil {
		return err
	}

	return nil
}

func (r *DBListRepo) RegisterWeb(w *Web) error {
	return w.establishWebSocketConnection()
}

func (r *DBListRepo) RegisterWalFile(wf WalFile) {
	r.walFiles = append(r.walFiles, wf)
	// TODO improve
	switch v := wf.(type) {
	case *s3WalFile:
		r.s3WalFiles = append(r.s3WalFiles, v)
	case *WebWalFile:
		r.webWalFiles = append(r.webWalFiles, v)
	}
	// Add the walFile to the map. We use this to retrieve the processed event cache, which we set
	// when consuming websocket events or on pull. This covers some edge cases where local updates
	// on foreign items will not emit to remotes, as we can use the cache in the getMatchedWal call
	if r.web != nil && wf.GetUUID() != "" {
		r.web.walFileMap[wf.GetUUID()] = &wf
	}
}
