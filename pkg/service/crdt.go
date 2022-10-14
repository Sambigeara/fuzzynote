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
	events := make([]EventLog, crdt.log.Len())

	for i := crdt.log.Front(); i != nil; i = i.Next() {
		e := i.Value.(EventLog)
		events = append(events, e)
	}

	return events
}

func (crdt *crdtTree) addToTargetChildArray(item, target *node) {
	item.parent = target

	target.children.insertInPlace(item)
}

func (crdt *crdtTree) updateCacheOrSkip(e EventLog) bool {
	var eventCache map[string]EventLog
	switch e.EventType {
	case UpdateEvent:
		eventCache = crdt.addEventSet
	case DeleteEvent:
		eventCache = crdt.deleteEventSet
	case PositionEvent:
		eventCache = crdt.positionEventSet
	}

	// Check the event cache and skip if the event is older than the most-recently processed
	if ce, exists := eventCache[e.ListItemKey]; exists {
		if e.before(ce) {
			return true
		}
	}

	// Add the event to the cache after the pre-existence checks above
	eventCache[e.ListItemKey] = e

	return false
}

func (crdt *crdtTree) updateLog(e EventLog) {
	for i := crdt.log.Front(); i != nil; i = i.Next() {
		curEvent := i.Value.(EventLog)

		comp := checkEquality(curEvent, e)

		// same ListItemKey:EventType mapping
		if curEvent.EventType == e.EventType && curEvent.ListItemKey == e.ListItemKey {
			switch comp {
			case eventsEqual:
				// the events are equal, return early
				return
			case leftEventOlder:
				// if ListItemKey is equal, and curEvent is older, remove
				crdt.log.Remove(i)
				continue
			}
		}

		// if curEvent is more recent than the new one, insert new one before
		if comp == rightEventOlder {
			crdt.log.InsertBefore(e, i)
			return
		}
	}

	crdt.log.PushBack(e)
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
