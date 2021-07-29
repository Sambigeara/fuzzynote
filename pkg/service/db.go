package service

import (
	"errors"
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
func (r *DBListRepo) Start(client Client) error {
	// TODO stricter control around event type
	//inputEvtsChan := make(chan tcell.Event)
	inputEvtsChan := make(chan interface{})

	walChan := make(chan *[]EventLog)

	// To avoid blocking key presses on the main processing loop, run heavy sync ops in a separate
	// loop, and only add to channel for processing if there's any changes that need syncing
	err := r.startSync(walChan)
	if err != nil {
		return err
	}

	// In the case of wal merges and receiving remote cursor positions below, we emit generic
	// null events which are handled in the main loop to refresh the client/UI state.
	// There is no need to schedule a refresh if there is already one waiting - in fact, this can
	// lead to a large backlog of unnecessary work.
	// Therefore, we create a channel buffered to 1 slot, and check this when scheduling a refresh.
	// If the slot is already taken, we skip, otherwise we schedule. The main loop consumer below
	// is responsible for clearing the slot once it's handled the refresh event.
	type refreshKey struct{}
	refreshChan := make(chan refreshKey, 1)
	scheduleRefresh := func() {
		// TODO pointless error return
		go func() error {
			select {
			case refreshChan <- refreshKey{}:
				inputEvtsChan <- refreshKey{}
				return nil
			default:
				return errors.New("Refresh channel already full")
			}
		}()
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
				// TODO figure out how to only add if there's not 1 in the channel already
				scheduleRefresh()
				//go func() {
				//    inputEvtsChan <- struct{}{}
				//}()
			case ev := <-r.remoteCursorMoveChan:
				// Update active key position of collaborator if changes have occurred
				updated := r.SetCollabPosition(ev)
				if updated {
					scheduleRefresh()
					//go func() {
					//    inputEvtsChan <- struct{}{}
					//}()
				}
			case ev := <-inputEvtsChan:
				cont, err := client.HandleEvent(ev)
				if err != nil {
					log.Fatal(err)
				}
				// Clear refreshChan if the event is of type refreshKey
				if _, isRefreshKey := ev.(refreshKey); isRefreshKey {
					<-refreshChan
				}
				if !cont {
					err := r.Stop()
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
		// TODO handle exiting more gracefully
		inputEvtsChan <- client.AwaitEvent()
	}

	//return nil
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

func (r *DBListRepo) registerWeb() error {
	// registerWeb is used periodically during runtime as well as at startup, so purge any
	// web walfiles prior to continuing
	r.clearWebWalFiles()

	if err := r.web.establishWebSocketConnection(); err != nil {
		return err
	}

	// Retrieve remotes from API
	remotes, err := r.web.GetRemotes("", nil)
	if err != nil {
		return errors.New("Error when trying to retrieve remotes config from API")
	}

	for _, remote := range remotes {
		if remote.IsActive {
			webWalFile := NewWebWalFile(remote, r.web)
			r.RegisterWalFile(webWalFile)
		}
	}

	return nil
}

// clearWebWalFiles is used to remove references of stored web walfiles. At the mo, primary use-case
// is to allow us to refresh them periodically.
func (r *DBListRepo) clearWebWalFiles() {
	for _, wf := range r.webWalFiles {
		delete(r.web.walFileMap, wf.GetUUID())
	}
	r.webWalFiles = []WalFile{}
}

func (r *DBListRepo) RegisterWalFile(wf WalFile) {
	switch wf.(type) {
	case *s3WalFile:
		r.s3WalFiles = append(r.s3WalFiles, wf)
	case *WebWalFile:
		r.webWalFiles = append(r.webWalFiles, wf)
	}
	// Add the walFile to the map. We use this to retrieve the processed event cache, which we set
	// when consuming websocket events or on pull. This covers some edge cases where local updates
	// on foreign items will not emit to remotes, as we can use the cache in the getMatchedWal call
	if r.web != nil && wf.GetUUID() != "" {
		r.web.walFileMap[wf.GetUUID()] = &wf
	}
}

func (r *DBListRepo) allWalFiles() []WalFile {
	return append(append(r.s3WalFiles, r.LocalWalFile), r.webWalFiles...)
}
