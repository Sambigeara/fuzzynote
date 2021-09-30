package service

import (
	"errors"
	"regexp"
	"sync"
)

var EmailRegex = regexp.MustCompile("^[a-zA-Z0-9.!#$%&'*+\\/=?^_`{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*$")

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
	errChan := make(chan error, 1)
	go func() {
		for {
			select {
			case partialWal := <-walChan:
				if err := r.Replay(partialWal); err != nil {
					errChan <- err
				}
				scheduleRefresh()
			case ev := <-r.remoteCursorMoveChan:
				// Update active key position of collaborator if changes have occurred
				updated := r.SetCollabPosition(ev)
				if updated {
					scheduleRefresh()
				}
			case ev := <-inputEvtsChan:
				cont, err := client.HandleEvent(ev)
				if err != nil {
					errChan <- err
				}
				// Clear refreshChan if the event is of type refreshKey
				if _, isRefreshKey := ev.(refreshKey); isRefreshKey {
					<-refreshChan
				}
				if !cont {
					err := r.Stop()
					if err != nil {
						errChan <- err
					}
					errChan <- nil
				}
			}
		}
	}()

	// This is the main loop of operation in the app.
	// We consume all term events into our own channel (handled above).
	for {
		select {
		case inputEvtsChan <- client.AwaitEvent():
		case err := <-errChan:
			return err
		}
	}

	//return nil
}

// Stop is called on app shutdown. It flushes all state changes in memory to disk
func (r *DBListRepo) Stop() error {
	err := r.LocalWalFile.Stop(uint32(r.uuid))
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
	if err := r.web.establishWebSocketConnection(); err != nil {
		return err
	}

	// Retrieve remotes from API
	//remotes, err := r.web.GetRemotes("", nil)
	//if err != nil {
	//    return errors.New("Error when trying to retrieve remotes config from API")
	//}

	//// At the moment remotes can be legacy number UUIDs, or now (more recently) email addresses
	//// Pull all down, and skip over any non email address remotes
	//// TODO remove!
	//for _, remote := range remotes {
	//    // Check if UUID matches an email pattern
	//    if EmailRegex.MatchString(remote.UUID) {
	//        hasFullAccess := false
	//        if remote.UUID == r.email {
	//            hasFullAccess = true
	//        }
	//        r.AddWalFile(
	//            &WebWalFile{
	//                uuid:               remote.UUID,
	//                processedEventLock: &sync.Mutex{},
	//                processedEventMap:  make(map[string]struct{}),
	//                web:                r.web,
	//            },
	//            hasFullAccess,
	//        )
	//    }
	//}

	r.DeleteWalFile(string(r.email))
	r.AddWalFile(
		&WebWalFile{
			uuid:               string(r.email),
			processedEventLock: &sync.Mutex{},
			processedEventMap:  make(map[string]struct{}),
			web:                r.web,
		},
		true,
	)

	return nil
}

func (r *DBListRepo) AddWalFile(wf WalFile, hasFullAccess bool) {
	r.allWalFiles[wf.GetUUID()] = &wf
	if hasFullAccess {
		r.syncWalFiles[wf.GetUUID()] = &wf
	}
	if _, ok := wf.(*WebWalFile); ok {
		r.webWalFiles[wf.GetUUID()] = &wf
	}
}

func (r *DBListRepo) DeleteWalFile(name string) {
	delete(r.allWalFiles, name)
	delete(r.syncWalFiles, name)
	delete(r.webWalFiles, name)
}
