package service

import (
	"errors"
	"sync"
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

	walChan := make(chan []EventLog)

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

	// TODO SL 2021-11-18: wasm browser loading fresh (len(wal) == 0) db will not load the client as
	// no `refresh` is triggered to refresh DOM state. This should be handled in the browser client code,
	// but this is so cheap and easy, I'm doing it here for now.
	go func() {
		scheduleRefresh()
	}()

	// We need atomicity between wal pull/replays and handling of keypress events, as we need
	// events to operate on a predictable state (rather than a keypress being applied to state
	// that differs from when the user intended due to async updates).
	// Therefore, we consume client events into a channel, and consume from it in the same loop
	// as the pull/replay loop.
	errChan := make(chan error)
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
				cont, purge, err := client.HandleEvent(ev)
				if err != nil {
					errChan <- err
				}
				// Clear refreshChan if the event is of type refreshKey
				if _, isRefreshKey := ev.(refreshKey); isRefreshKey {
					<-refreshChan
				}
				if !cont {
					errChan <- r.finish(purge)
				}
			}
		}
	}()

	// This is the main loop of operation in the app.
	// We consume all term events into our own channel (handled above).
	// TODO SL 2021-11-17: This is handled in a separate goroutine solely for the wasm web app -
	// at present, without this operating in a separate loop, the app behaves oddly when trying
	// to exit gracefully. Running in a separate goroutine solves this apparently.
	go func() {
		for {
			inputEvtsChan <- client.AwaitEvent()
		}
	}()
	//for {
	//    select {
	//    case inputEvtsChan <- client.AwaitEvent():
	//    case err := <-errChan:
	//        return err
	//    }
	//}
	return <-errChan
}

func (r *DBListRepo) registerWeb() error {
	if err := r.web.establishWebSocketConnection(); err != nil {
		return err
	}

	if r.email == "" {
		if pong, err := r.web.ping(); err == nil {
			r.setEmail(pong.User)
			r.web.tokens.SetEmail(pong.User)
			r.web.tokens.Flush()
		}
	}

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
	r.allWalFileMut.Lock()
	r.allWalFiles[wf.GetUUID()] = wf
	r.allWalFileMut.Unlock()

	if hasFullAccess {
		r.syncWalFileMut.Lock()
		r.syncWalFiles[wf.GetUUID()] = wf
		r.syncWalFileMut.Unlock()
	}

	if _, ok := wf.(*WebWalFile); ok {
		r.webWalFileMut.Lock()
		r.webWalFiles[wf.GetUUID()] = wf
		r.webWalFileMut.Unlock()
	}
}

func (r *DBListRepo) DeleteWalFile(name string) {
	r.allWalFileMut.Lock()
	r.syncWalFileMut.Lock()
	r.webWalFileMut.Lock()
	defer r.allWalFileMut.Unlock()
	defer r.syncWalFileMut.Unlock()
	defer r.webWalFileMut.Unlock()
	delete(r.allWalFiles, name)
	delete(r.syncWalFiles, name)
	delete(r.webWalFiles, name)
}
