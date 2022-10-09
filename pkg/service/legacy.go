package service

import (
	"compress/gzip"
	"encoding/binary"
	"encoding/gob"
	"errors"
	"io"
	"sort"
	"strconv"
	"strings"
)

type walItemSchema2 struct {
	UUID                       uuid
	TargetUUID                 uuid
	ListItemCreationTime       int64
	TargetListItemCreationTime int64
	EventTime                  int64
	EventType                  EventType
	LineLength                 uint64
	NoteExists                 bool
	NoteLength                 uint64
}

type LineFriendsSchema4 struct {
	IsProcessed bool
	Offset      int
	Emails      map[string]struct{}
}

type EventLogSchema4 struct {
	UUID, TargetUUID           uuid
	ListItemCreationTime       int64
	TargetListItemCreationTime int64
	UnixNanoTime               int64
	EventType                  EventType
	Line                       string
	Note                       []byte
	Friends                    LineFriendsSchema4
	key, targetKey             string
}

type EventLogSchema5 struct {
	UUID, TargetUUID           uuid
	ListItemCreationTime       int64
	TargetListItemCreationTime int64
	UnixNanoTime               int64
	EventType                  EventType
	Line                       string
	Note                       []byte
	Friends                    LineFriends
	key, targetKey             string
}

type EventLogSchema6 struct {
	UUID                           uuid // origin UUID
	LamportTimestamp               int64
	EventType                      EventType
	ListItemKey, TargetListItemKey string
	Line                           string
	Note                           []byte
	Friends                        LineFriends
	cachedKey                      string
}

func (e *EventLogSchema6) key() string {
	if e.cachedKey == "" {
		e.cachedKey = strconv.Itoa(int(e.UUID)) + ":" + strconv.Itoa(int(e.LamportTimestamp))
	}
	return e.cachedKey
}

type LegacyListItem struct {
	rawLine string
	Note    []byte

	IsHidden bool

	child       *LegacyListItem
	parent      *LegacyListItem
	matchChild  *LegacyListItem
	matchParent *LegacyListItem

	friends LineFriends

	localEmail string // set at creation time and used to exclude from Friends() method
	key        string

	isDeleted bool
}

func getNextEventLogFromWalFile(r io.Reader, schemaVersionID uint16) (*EventLogSchema6, error) {
	el := EventLogSchema6{}

	switch schemaVersionID {
	case 3:
		wi := walItemSchema2{}
		err := binary.Read(r, binary.LittleEndian, &wi)
		if err != nil {
			return nil, err
		}

		el.UUID = wi.UUID
		el.EventType = wi.EventType
		el.ListItemKey = strconv.Itoa(int(wi.UUID)) + ":" + strconv.Itoa(int(wi.ListItemCreationTime))
		el.TargetListItemKey = strconv.Itoa(int(wi.TargetUUID)) + ":" + strconv.Itoa(int(wi.TargetListItemCreationTime))
		el.LamportTimestamp = wi.EventTime

		line := make([]byte, wi.LineLength)
		err = binary.Read(r, binary.LittleEndian, &line)
		if err != nil {
			return nil, err
		}
		el.Line = string(line)

		if wi.NoteExists {
			el.Note = []byte{}
		}
		if wi.NoteLength > 0 {
			note := make([]byte, wi.NoteLength)
			err = binary.Read(r, binary.LittleEndian, &note)
			if err != nil {
				return nil, err
			}
			el.Note = note
		}
	default:
		return nil, errors.New("unrecognised wal schema version")
	}
	return &el, nil
}

func getOldEventLogKeys(i interface{}) (string, string) {
	var key, targetKey string
	switch e := i.(type) {
	case EventLogSchema4:
		key = strconv.Itoa(int(e.UUID)) + ":" + strconv.Itoa(int(e.ListItemCreationTime))
		targetKey = strconv.Itoa(int(e.TargetUUID)) + ":" + strconv.Itoa(int(e.TargetListItemCreationTime))
	case EventLogSchema5:
		key = strconv.Itoa(int(e.UUID)) + ":" + strconv.Itoa(int(e.ListItemCreationTime))
		targetKey = strconv.Itoa(int(e.TargetUUID)) + ":" + strconv.Itoa(int(e.TargetListItemCreationTime))
	}
	return key, targetKey
}

func legacyMigrateToVersionSix(pr *io.PipeReader, walSchemaVersionID uint16, errChan chan error) ([]EventLogSchema6, error) {
	var el []EventLogSchema6
	switch walSchemaVersionID {
	case 1, 2, 3:
		for {
			select {
			case err := <-errChan:
				if err != nil {
					return el, err
				}
			default:
				e, err := getNextEventLogFromWalFile(pr, walSchemaVersionID)
				if err != nil {
					switch err {
					case io.EOF:
						return el, nil
					case io.ErrUnexpectedEOF:
						// Given the distributed concurrent nature of this app, we sometimes pick up partially
						// uploaded files which will fail, but may well be complete later on, therefore just
						// return for now and attempt again later
						// TODO implement a decent retry mech here
						return el, nil
					default:
						return el, err
					}
				}
				el = append(el, *e)
			}
		}
	case 4:
		var oel []EventLogSchema4
		dec := gob.NewDecoder(pr)
		if err := dec.Decode(&oel); err != nil {
			return el, err
		}
		if err := <-errChan; err != nil {
			return el, err
		}
		for _, oe := range oel {
			key, targetKey := getOldEventLogKeys(oe)
			e := EventLogSchema6{
				UUID:              oe.UUID,
				ListItemKey:       key,
				TargetListItemKey: targetKey,
				EventType:         oe.EventType,
				Line:              oe.Line,
				Note:              oe.Note,
				Friends: LineFriends{
					IsProcessed: oe.Friends.IsProcessed,
					Offset:      oe.Friends.Offset,
					emailsMap:   oe.Friends.Emails,
				},
			}
			e.LamportTimestamp = oe.UnixNanoTime
			for f := range oe.Friends.Emails {
				e.Friends.Emails = append(e.Friends.Emails, f)
			}
			sort.Strings(e.Friends.Emails)
			el = append(el, e)
		}
	case 5:
		var oel []EventLogSchema5
		dec := gob.NewDecoder(pr)
		if err := dec.Decode(&oel); err != nil {
			return el, err
		}
		if err := <-errChan; err != nil {
			return el, err
		}
		for _, oe := range oel {
			key, targetKey := getOldEventLogKeys(oe)
			e := EventLogSchema6{
				UUID:              oe.UUID,
				ListItemKey:       key,
				TargetListItemKey: targetKey,
				EventType:         oe.EventType,
				Line:              oe.Line,
				Note:              oe.Note,
				Friends:           oe.Friends,
			}
			e.LamportTimestamp = oe.UnixNanoTime
			el = append(el, e)
		}
	case 6:
		dec := gob.NewDecoder(pr)
		if err := dec.Decode(&el); err != nil {
			return el, err
		}
		if err := <-errChan; err != nil {
			return el, err
		}
	}

	return el, nil
}

type legacyDBListRepo struct {
	Root                               *LegacyListItem
	listItemTracker                    map[string]*LegacyListItem
	processedEventLogCache             map[string]struct{}
	listItemProcessedEventLogTypeCache map[EventType]map[string]EventLogSchema6
}

func (r *legacyDBListRepo) add(key string, line string, friends LineFriends, note []byte, childItem *LegacyListItem) (*LegacyListItem, error) {
	newItem := &LegacyListItem{
		key:     key,
		child:   childItem,
		rawLine: line,
		Note:    note,
		friends: friends,
	}

	// If `child` is nil, it's the first item in the list so set as root and return
	if childItem == nil {
		oldRoot := r.Root
		r.Root = newItem
		if oldRoot != nil {
			newItem.parent = oldRoot
			oldRoot.child = newItem
		}
		return newItem, nil
	}

	if childItem.parent != nil {
		childItem.parent.child = newItem
		newItem.parent = childItem.parent
	}
	childItem.parent = newItem

	return newItem, nil
}

func (r *legacyDBListRepo) update(item *LegacyListItem, line string, friends LineFriends, note []byte) error {
	if len(line) > 0 {
		item.rawLine = line
	} else {
		item.Note = note
	}

	// Just in case an Update occurs on a Deleted item (distributed race conditions)
	item.isDeleted = false

	return nil
}

func (r *legacyDBListRepo) del(item *LegacyListItem) error {
	item.isDeleted = true

	if item.child != nil {
		item.child.parent = item.parent
	} else {
		// If the item has no child, it is at the top of the list and therefore we need to update the root
		r.Root = item.parent
	}

	if item.parent != nil {
		item.parent.child = item.child
	}

	return nil
}

func (r *legacyDBListRepo) move(item *LegacyListItem, childItem *LegacyListItem) (*LegacyListItem, error) {
	var err error
	err = r.del(item)
	isHidden := item.IsHidden
	item, err = r.add(item.key, item.rawLine, item.friends, item.Note, childItem)
	if isHidden {
		r.setVisibility(item, false)
	}
	return item, err
}

func (r *legacyDBListRepo) setVisibility(item *LegacyListItem, isVisible bool) error {
	item.IsHidden = !isVisible
	return nil
}

func legacyCheckEquality(event1 EventLogSchema6, event2 EventLogSchema6) int {
	if event1.LamportTimestamp < event2.LamportTimestamp ||
		event1.LamportTimestamp == event2.LamportTimestamp && event1.UUID < event2.UUID {
		return leftEventOlder
	} else if event2.LamportTimestamp < event1.LamportTimestamp ||
		event2.LamportTimestamp == event1.LamportTimestamp && event2.UUID < event1.UUID {
		return rightEventOlder
	}
	return eventsEqual
}

func (r *legacyDBListRepo) legacyProcessEventLog(e EventLogSchema6) (*LegacyListItem, error) {
	item := r.listItemTracker[e.ListItemKey]
	targetItem := r.listItemTracker[e.TargetListItemKey]

	// Skip any events that have already been processed
	if _, exists := r.processedEventLogCache[e.key()]; exists {
		return item, nil
	}
	r.processedEventLogCache[e.key()] = struct{}{}

	// Else, skip any event of equal EventType that is <= the most recently processed for a given ListItem
	// TODO storing a whole EventLog might be expensive, use a specific/reduced type in the nested map
	if eventTypeCache, exists := r.listItemProcessedEventLogTypeCache[e.EventType]; exists {
		if ce, exists := eventTypeCache[e.ListItemKey]; exists {
			switch legacyCheckEquality(ce, e) {
			// if the new event is older or equal, skip
			case rightEventOlder, eventsEqual:
				return item, nil
			}
		}
	} else {
		r.listItemProcessedEventLogTypeCache[e.EventType] = make(map[string]EventLogSchema6)
	}
	r.listItemProcessedEventLogTypeCache[e.EventType][e.ListItemKey] = e

	// We need to maintain records of deleted items in the cache, but if deleted, want to assign nil ptrs
	// in the various funcs below, so set to nil
	if item != nil && item.isDeleted {
		item = nil
	}
	if targetItem != nil && targetItem.isDeleted {
		targetItem = nil
	}

	// When we're calling this function on initial WAL merge and load, we may come across
	// orphaned items. There MIGHT be a valid case to keep events around if the EventType
	// is Update. Item will obviously never exist for Add. For all other eventTypes,
	// we should just skip the event and return
	// TODO remove this AddEvent nil item passthrough
	if item == nil && e.EventType != AddEvent && e.EventType != UpdateEvent {
		return item, nil
	}

	var err error
	switch e.EventType {
	case AddEvent:
		if item != nil {
			// 21/11/21: There was a bug caused when a collaborator `Undo` was carried out on a collaborator origin
			// `Delete`, when the `Delete` was not picked up by the local. This resulted in an `Add` being carried out
			// with a duplicate ListItem key, which led to some F'd up behaviour in the match set. This catch covers
			// this case by doing a dumb "if Add on existing item, change to Update". We need to run two updates, as
			// Note and Line updates are individual operations.
			// TODO remove this when `Compact`/wal post-processing is smart enough to iron out these broken logs.
			err = r.update(item, e.Line, e.Friends, e.Note)
			err = r.update(item, "", e.Friends, e.Note)
		} else {
			item, err = r.add(e.ListItemKey, e.Line, e.Friends, e.Note, targetItem)
		}
	case UpdateEvent:
		// We have to cover an edge case here which occurs when merging two remote WALs. If the following occurs:
		// - wal1 creates item A
		// - wal2 copies wal1
		// - wal2 deletes item A
		// - wal1 updates item A
		// - wal1 copies wal2
		// We will end up with an attempted Update on a nonexistent item.
		// In this case, we will Add an item back in with the updated content
		// NOTE A side effect of this will be that the re-added item will be at the top of the list as it
		// becomes tricky to deal with child IDs
		if item != nil {
			err = r.update(item, e.Line, e.Friends, e.Note)
		} else {
			item, err = r.add(e.ListItemKey, e.Line, e.Friends, e.Note, targetItem)
		}
	case MoveDownEvent:
		if targetItem == nil {
			return item, nil
		}
		fallthrough
	case MoveUpEvent:
		item, err = r.move(item, targetItem)
	case ShowEvent:
		err = r.setVisibility(item, true)
	case HideEvent:
		err = r.setVisibility(item, false)
	case DeleteEvent:
		err = r.del(item)
	}

	if item != nil {
		r.listItemTracker[e.ListItemKey] = item
	}

	return item, err
}

// Build wals from unzipped data for old (pre CRDT tree related schema)
func (r *DBListRepo) legacyBuildFromRaw(pr *io.PipeReader, walSchemaVersionID uint16, errChan chan error) ([]EventLog, error) {
	// walSchemaVersionID == 6 represents the last of the legacy event logs which backed the old event CRDT.
	// These previous versions were brittle and probably quite buggy, and efforts to migrate on
	// a per-log basis proved fruitless and unpredictable.
	// In the interest of a clean start, we will migrate from any version <6 -> 6, then build a
	// fresh fzn document based on that state, and then use that to build a completely new
	// event log using the new CRDT model.

	legacyEl, err := legacyMigrateToVersionSix(pr, walSchemaVersionID, errChan)
	if err != nil {
		return []EventLog{}, err
	}

	legacyRepo := legacyDBListRepo{
		listItemTracker:                    make(map[string]*LegacyListItem),
		processedEventLogCache:             make(map[string]struct{}),
		listItemProcessedEventLogTypeCache: make(map[EventType]map[string]EventLogSchema6),
	}

	for _, e := range legacyEl {
		legacyRepo.legacyProcessEventLog(e)
	}

	i := 0

	// traverse from the Root item, and for each, creates an `UpdateEvent` and a `PositionEvent`
	el := []EventLog{}
	item := legacyRepo.Root
	var prev *LegacyListItem
	for item != nil {
		s := strings.Split(item.key, ":")
		id, _ := strconv.ParseInt(s[0], 10, 64)
		lamport, _ := strconv.ParseInt(s[1], 10, 64)

		u := EventLog{
			UUID:             uuid(id),
			LamportTimestamp: lamport,
			EventType:        UpdateEvent,
			ListItemKey:      item.key,
			Line:             item.rawLine,
			Note:             item.Note,
			IsHidden:         item.IsHidden,
		}
		u = r.repositionActiveFriends(u)
		el = append(el, u)

		p := EventLog{
			UUID:             uuid(id),
			LamportTimestamp: lamport,
			EventType:        PositionEvent,
			ListItemKey:      item.key,
		}
		if prev != nil {
			p.TargetListItemKey = prev.key
		}
		el = append(el, p)

		prev = item
		item = item.parent
		i++
	}

	return el, nil
}

func (r *DBListRepo) legacyBuildFromFile(walSchemaVersionID uint16, raw io.Reader) ([]EventLog, error) {
	pr, pw := io.Pipe()
	errChan := make(chan error, 1)
	go func() {
		defer pw.Close()
		switch walSchemaVersionID {
		case 1, 2:
			if _, err := io.Copy(pw, raw); err != nil {
				errChan <- err
			}
		default:
			// Versions >=3 of the wal schema is gzipped after the first 2 bytes. Therefore, unzip those bytes
			// prior to passing it to the loop below
			zr, err := gzip.NewReader(raw)
			if err != nil {
				errChan <- err
			}
			defer zr.Close()
			if _, err := io.Copy(pw, zr); err != nil {
				errChan <- err
			}
		}
		errChan <- nil
	}()

	return r.legacyBuildFromRaw(pr, walSchemaVersionID, errChan)
}
