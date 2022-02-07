package service

import (
	"context"
)

type refreshKey struct{}

// Start begins push/pull for all WalFiles
func (r *DBListRepo) Start(client Client) error {
	inputEvtsChan := make(chan interface{})

	ctx, cancel := context.WithCancel(context.Background())

	// In the case of wal merges and receiving remote cursor positions below, we emit generic
	// null events which are handled in the main loop to refresh the client/UI state.
	// Only schedule a refresh if there is not one currently pending.
	scheduleRefresh := func() {
		go func() {
			select {
			case inputEvtsChan <- refreshKey{}:
			case <-ctx.Done():
				return
			default:
			}
		}()
	}

	replayChan := make(chan []EventLog)
	reorderAndReplayChan := make(chan []EventLog)

	// We need atomicity between wal pull/replays and handling of keypress events, as we need
	// events to operate on a predictable state (rather than a keypress being applied to state
	// that differs from when the user intended due to async updates).
	// Therefore, we consume client events into a channel, and consume from it in the same loop
	// as the pull/replay loop.
	errChan := make(chan error)
	go func() {
		for {
			select {
			case wal := <-reorderAndReplayChan:
				// merge in any events added between the wal being added to reorderAndReplayChan and now
				wal = merge(wal, r.log)
				wal = reorderWal(wal)
				//wal, _ = compact(wal)
				// r.log will contain the ~uncompressed~ potentially unordered log, and r.Replay
				// attempts to merge the new wal with r.log (which will render the compaction
				// pointless, as we just merge the full log back in), therefore set the eventlog
				// to a new empty one. This is acceptable given that mutations to r.log only
				// occur within this thread
				r.log = []EventLog{}
				if err := r.Replay(wal); err != nil {
					errChan <- err
					return
				}
				scheduleRefresh()
			case wal := <-replayChan:
				if err := r.Replay(wal); err != nil {
					errChan <- err
					return
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
					return
				}
				if !cont {
					cancel()
					errChan <- r.finish(purge)
					return
				}
			}
		}
	}()

	// To avoid blocking key presses on the main processing loop, run heavy sync ops in a separate
	// loop, and only add to channel for processing if there's any changes that need syncing
	// This is run after the goroutine above is triggered to ensure a thread is consuming from replayChan
	err := r.startSync(ctx, replayChan, reorderAndReplayChan)
	if err != nil {
		return err
	}

	// This is the main loop of operation in the app.
	// We consume all term events into our own channel (handled above).
	// This is handled in a separate goroutine due to pontential contention in the loop above, whereby we consume
	// from inputEvtsChan and publish to the errChan (rather than a single select with two options in the main thread),
	// e.g.
	// select {
	// case inputEvtsChan <- client.AwaitEvent():
	// case err <- errchan:
	//     ...
	// }
	go func() {
		for {
			inputEvtsChan <- client.AwaitEvent()
		}
	}()
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
			uuid: string(r.email),
			web:  r.web,
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
