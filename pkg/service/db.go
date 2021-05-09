package service

//import ()

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
func (r *DBListRepo) Start(walChan chan *[]EventLog) error {
	return r.startSync(walChan)
}

// Stop is called on app shutdown. It flushes all state changes in memory to disk
func (r *DBListRepo) Stop() error {
	err := r.LocalWalFile.Stop(r.uuid)
	if err != nil {
		return err
	}

	err = r.finish()
	if err != nil {
		return err
	}

	return nil
}

func (r *DBListRepo) RegisterWeb(w *Web) {
	r.web = w
	r.web.uuid = r.uuid
	w.establishWebSocketConnection()
}

func (r *DBListRepo) RegisterWalFile(wf WalFile) {
	r.walFiles = append(r.walFiles, wf)
	// Add the walFile to the map. We use this to retrieve the processed event cache, which we set
	// when consuming websocket events or on pull. This covers some edge cases where local updates
	// on foreign items will not emit to remotes, as we can use the cache in the getMatchedWal call
	if r.web != nil && wf.GetUUID() != "" {
		r.web.walFileMap[wf.GetUUID()] = &wf
	}
}
