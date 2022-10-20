package service

import (
	"container/list"
	"strconv"
	"strings"
)

const (
	crdtRootKey   = ""
	crdtOrphanKey = "_orphan"
)

const (
	eventsEqual int = iota
	leftEventOlder
	rightEventOlder
)

func checkEquality(left, right EventLog) int {
	if left.LamportTimestamp < right.LamportTimestamp ||
		left.LamportTimestamp == right.LamportTimestamp && left.UUID < right.UUID ||
		left.LamportTimestamp == right.LamportTimestamp && left.UUID == right.UUID && left.EventType < right.EventType {
		return leftEventOlder
	} else if right.LamportTimestamp < left.LamportTimestamp ||
		right.LamportTimestamp == left.LamportTimestamp && right.UUID < left.UUID ||
		right.LamportTimestamp == left.LamportTimestamp && right.UUID == left.UUID && right.EventType < left.EventType {
		return rightEventOlder
	}
	return eventsEqual
}

func (a *EventLog) before(b EventLog) bool {
	return leftEventOlder == checkEquality(*a, b)
}

type crdtTree struct {
	cache                                         map[string]*node
	addEventSet, deleteEventSet, positionEventSet map[string]EventLog
	log                                           *list.List // A time ordered DLL of events
}

type node struct {
	key                    string
	originUUID             uuid
	parent, left, right    *node
	children               nodeDll
	latestLamportTimestamp int64
}

func (n *node) moreRecentThan(o *node) bool {
	if n.key == "_orphan" {
		return false
	} else if o.key == "_orphan" {
		return false
	}

	if n.latestLamportTimestamp > o.latestLamportTimestamp {
		return true
	} else if n.latestLamportTimestamp < o.latestLamportTimestamp {
		return false
	}
	return n.originUUID > o.originUUID
}

// Seek out the next node in this precedence:
// - immediate child
// - right ptr
// - right ptr of next parent which is not root
func (n *node) getNext() *node {
	if n.children.front != nil {
		return n.children.front
	}

	if n.right != nil {
		return n.right
	}

	cur := n.parent
	for cur != nil {
		if cur.right != nil {
			return cur.right
		}
		cur = cur.parent
	}

	return nil
}

// Consider using https://pkg.go.dev/container/list
type nodeDll struct {
	front *node
}

func (l *nodeDll) insertInPlace(n *node) {
	right := l.front

	var left *node
	for right != nil {
		if n.moreRecentThan(right) {
			break
		}
		left = right
		right = right.right
	}

	// insert into place
	if left != nil {
		left.right = n
	} else {
		l.front = n
	}
	if right != nil {
		right.left = n
	}
	n.left = left
	n.right = right
}

func (l *nodeDll) remove(n *node) {
	if n.left != nil {
		n.left.right = n.right
	} else {
		// node is front, so reset front in dll
		l.front = n.right
	}
	if r := n.right; r != nil {
		r.left = n.left
	}
	n.left = nil
	n.right = nil
	return
}

func newTree() *crdtTree {
	orphan := &node{
		key: crdtOrphanKey,
	}
	root := &node{
		key:      crdtRootKey,
		children: nodeDll{orphan},
	}
	orphan.parent = root
	return &crdtTree{
		cache: map[string]*node{
			crdtOrphanKey: orphan,
			crdtRootKey:   root,
		},
		addEventSet:      make(map[string]EventLog),
		deleteEventSet:   make(map[string]EventLog),
		positionEventSet: make(map[string]EventLog),
		log:              list.New(),
	}
}

func (crdt *crdtTree) itemIsLive(key string) bool {
	latestAddEvent, isAdded := crdt.addEventSet[key]
	latestDeleteEvent, isDeleted := crdt.deleteEventSet[key]
	if isAdded && (!isDeleted || (isAdded && latestDeleteEvent.before(latestAddEvent))) {
		return true
	}
	return false
}

func (crdt *crdtTree) traverse(n *node) *node {
	if n == nil {
		n = crdt.cache[crdtRootKey]
	}
	n = n.getNext()
	for n != nil {
		if n.key != crdtRootKey && n.key != crdtOrphanKey && n.parent.key != crdtOrphanKey && crdt.itemIsLive(n.key) {
			return n
		}
		n = n.getNext()
	}
	return nil
}

func (crdt *crdtTree) getEventLog() []EventLog {
	events := []EventLog{}
	for i := crdt.log.Front(); i != nil; i = i.Next() {
		e := i.Value.(EventLog)
		events = append(events, e)
	}
	return events
}

// sync is used to generate events to pass to remotes which may not have them.
// The passed in event is from the remote. If it exists locally, we only return all following events (including
// those which have the same lamport, in case of collisions).
// If we don't have the local event, we can't guarantee that the remote has local events, so it's safer to replay all
// TODO - optimise
func (crdt *crdtTree) sync(e EventLog) []EventLog {
	// iterate backwards until we find the matching event, or reach the beginning, then iterate forwards again to build
	// the resultant eventlog
	// `i.Next()` will be the first returned event in the log
	i := crdt.log.Back()

	events := []EventLog{}

	if i == nil {
		return events
	}

	// return empty if local "head" == incoming events (remote is already in sync)
	if head := i.Value.(EventLog); checkEquality(head, e) == eventsEqual {
		return events
	}

	// Otherwise iterate backwards until `i` is the matching event, or nil
	for i = i.Prev(); i != nil; i = i.Prev() {
		curEvent := i.Value.(EventLog)
		if checkEquality(curEvent, e) == eventsEqual {
			// If we find the match:
			// Check for lamport collisions - we continue to iterate until the "earlier" event with the same lamport timestamp,
			// as we can't guarantee that the remote has all of the events with the shared lamport.

			// If there are _any_ more events with the same lamport, we include all of them, so we need to manually step
			// left once more if we end up traversing through lamport collisions
			includeFirstLamport := false
			for i.Prev() != nil && i.Prev().Value.(EventLog).LamportTimestamp == e.LamportTimestamp {
				i = i.Prev()
				includeFirstLamport = true
			}
			if includeFirstLamport {
				i = i.Prev()
			}
			break
		}
	}

	if i == nil {
		i = crdt.log.Front()
	} else {
		i = i.Next()
	}

	// And back fowards again
	for ; i != nil; i = i.Next() {
		events = append(events, i.Value.(EventLog))
	}

	return events
}

func (crdt *crdtTree) addToTargetChildArray(item, target *node) {
	item.parent = target

	target.children.insertInPlace(item)
}

type eventCacheAction int

const (
	eventCreated eventCacheAction = iota
	eventOverridden
	eventSkipped
)

func (crdt *crdtTree) updateCacheOrSkip(e EventLog) eventCacheAction {
	var eventCache map[string]EventLog
	switch e.EventType {
	case UpdateEvent:
		eventCache = crdt.addEventSet
	case DeleteEvent:
		eventCache = crdt.deleteEventSet
	case PositionEvent:
		eventCache = crdt.positionEventSet
	}

	action := eventCreated

	// Check the event cache and skip if the event is older than the most-recently processed
	if ce, exists := eventCache[e.ListItemKey]; exists {
		if checkEquality(ce, e) != leftEventOlder {
			return eventSkipped
		}
		action = eventOverridden
	}

	if e.EventType == UpdateEvent {
		// Also skip if the event is an UpdateEvent, and there is a newer DeleteEvent that exists
		if de, exists := crdt.deleteEventSet[e.ListItemKey]; exists && checkEquality(de, e) != leftEventOlder {
			return eventSkipped
		}
	} else if e.EventType == DeleteEvent {
		// We need to trigger a search through the linked list log if an update event exists (inferring that this
		// delete occurred during normal operation, rather than at startup)
		if _, exists := crdt.addEventSet[e.ListItemKey]; exists {
			action = eventOverridden
		}
	}

	// Add the event to the cache after the pre-existence checks above
	eventCache[e.ListItemKey] = e

	return action
}

func (crdt *crdtTree) updateLog(e EventLog, bypassDelete bool) {
	// Traverse from the end of the list, backwards. Under the majority of cases, this is much more efficient.
	// We only continue to seek for duplicate listItemKey:EventType combos if we know that a previous event exists
	// in the log (inferred from bypassDelete).
	// If the incoming event is a DeleteEvent (and bypassDelete is false), we also traverse back to remove any UpdateEvent
	// for the matching ListItemKey (but maintain the PositionEvent for referential data).

	isInserted := false

	for i := crdt.log.Back(); i != nil; i = i.Prev() {
		curEvent := i.Value.(EventLog)
		comp := checkEquality(curEvent, e)
		if !isInserted && comp == leftEventOlder {
			crdt.log.InsertAfter(e, i)
			if bypassDelete {
				return
			}
			isInserted = true
		}
		if !bypassDelete && curEvent.ListItemKey == e.ListItemKey {
			if e.EventType == DeleteEvent {
				if curEvent.EventType == UpdateEvent && comp == leftEventOlder {
					crdt.log.Remove(i)
				}
			} else if curEvent.EventType == e.EventType {
				if comp != eventsEqual {
					crdt.log.Remove(i)
				}
				return
			}
		}
	}

	if !isInserted {
		crdt.log.PushFront(e)
	}
}

func (crdt *crdtTree) add(e EventLog) {
	target, exists := crdt.cache[e.TargetListItemKey]
	if !exists {
		// TODO centralise
		// TODO more efficient storage/inferrence of target item uuid/lamport
		r := strings.Split(e.TargetListItemKey, ":")
		var ts int64
		var originUUID uuid
		if len(r) > 0 {
			i, _ := strconv.ParseInt(r[0], 10, 64)
			originUUID = uuid(i)
			if len(r) > 1 {
				ts, _ = strconv.ParseInt(r[1], 10, 64)
			}
		}

		target = &node{
			key:                    e.TargetListItemKey,
			originUUID:             originUUID,
			latestLamportTimestamp: ts,
		}
		crdt.cache[e.TargetListItemKey] = target
		orphanParent := crdt.cache[crdtOrphanKey]
		crdt.addToTargetChildArray(target, orphanParent)
	}

	item, exists := crdt.cache[e.ListItemKey]
	if !exists {
		item = &node{
			key:                    e.ListItemKey,
			originUUID:             e.UUID,
			latestLamportTimestamp: e.LamportTimestamp,
		}
		crdt.cache[e.ListItemKey] = item
	} else {
		// remove from parent.children if pre-existing
		item.parent.children.remove(item)
	}
	item.latestLamportTimestamp = e.LamportTimestamp

	crdt.addToTargetChildArray(item, target)
}
