package service

import (
	"strconv"
	"strings"
)

const (
	crdtRootKey   = ""
	crdtOrphanKey = "_orphan"
)

type node struct {
	key                    string
	originUUID             uuid
	parent, left, right    *node
	children               childDll
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
	if n.children.firstChild != nil {
		return n.children.firstChild
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
type childDll struct {
	firstChild *node
}

func (l *childDll) insertInPlace(n *node) {
	right := l.firstChild

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
		l.firstChild = n
	}
	// If we traversed to the end
	if right != nil {
		right.left = n
	}
	n.left = left
	n.right = right
}

func (l *childDll) removeChild(n *node) {
	if n.left != nil {
		n.left.right = n.right
	} else {
		// node is firstChild, so reset firstChild in dll
		l.firstChild = n.right
	}
	if r := n.right; r != nil {
		r.left = n.left
	}
	n.left = nil
	n.right = nil
	return
}

type crdtTree struct {
	cache                                            map[string]*node
	updateEventSet, deleteEventSet, positionEventSet map[string]EventLog
}

func newTree() *crdtTree {
	orphan := &node{
		key: crdtOrphanKey,
	}
	root := &node{
		key:      crdtRootKey,
		children: childDll{orphan},
	}
	orphan.parent = root
	return &crdtTree{
		cache: map[string]*node{
			crdtOrphanKey: orphan,
			crdtRootKey:   root,
		},
		updateEventSet:   make(map[string]EventLog),
		deleteEventSet:   make(map[string]EventLog),
		positionEventSet: make(map[string]EventLog),
	}
}

func (crdt *crdtTree) getEventCache(t EventType) map[string]EventLog {
	var eventCache map[string]EventLog
	switch t {
	case UpdateEvent:
		eventCache = crdt.updateEventSet
	case DeleteEvent:
		eventCache = crdt.deleteEventSet
	case PositionEvent:
		eventCache = crdt.positionEventSet
	}
	return eventCache
}

func (crdt *crdtTree) skipOrUpdate(e EventLog) bool {
	var eventCache map[string]EventLog
	switch e.EventType {
	case UpdateEvent:
		eventCache = crdt.updateEventSet
	case PositionEvent:
		eventCache = crdt.positionEventSet
	case DeleteEvent:
		eventCache = crdt.deleteEventSet
	}

	// If the incoming event is older than the current most recent cached event, return early with true
	if ce, exists := eventCache[e.ListItemKey]; exists {
		if e.before(ce) {
			return true
		}
	}

	// Otherwise, update the cache with the newer event and return false
	eventCache[e.ListItemKey] = e

	return false
}

func (crdt *crdtTree) itemIsLive(key string) bool {
	latestAddEvent, isAdded := crdt.updateEventSet[key]
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

func (crdt *crdtTree) generateEvents() []EventLog {
	events := []EventLog{}

	// Add all PositionEvents
	for _, e := range crdt.positionEventSet {
		events = append(events, e)
	}

	// Add UpdateEvents for active items (deleted items don't need content state, as any update that overrides deleted state will
	// come with it's own
	for k, addEvent := range crdt.updateEventSet {
		if crdt.itemIsLive(k) {
			events = append(events, addEvent)
		}
	}

	return events
}

func (crdt *crdtTree) addToTargetChildArray(item, target *node) {
	item.parent = target

	target.children.insertInPlace(item)
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
		item.parent.children.removeChild(item)
	}
	item.latestLamportTimestamp = e.LamportTimestamp

	crdt.addToTargetChildArray(item, target)
}
