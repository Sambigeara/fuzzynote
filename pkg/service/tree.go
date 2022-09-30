package service

import (
	"fmt"

	"github.com/xlab/treeprint"
)

const (
	crdtRootKey   = ""
	crdtOrphanKey = "_orphan"
)

type crdtTree struct {
	cache                                         map[string]*node
	addEventSet, deleteEventSet, positionEventSet map[string]EventLog
}

type node struct {
	key                 string
	parent, left, right *node
	children            childDll
	latestVectorClock   map[uuid]int64
}

func (n *node) moreRecentThan(o *node) bool {
	if n.key == "_orphan" {
		return false
	} else if o.key == "_orphan" {
		return false
	}
	return !vectorClockBefore(n.latestVectorClock, o.latestVectorClock)
}

// Consider using https://pkg.go.dev/container/list
type childDll struct {
	firstChild *node
}

func (l *childDll) iter() <-chan *node {
	ch := make(chan *node)
	go func() {
		n := l.firstChild
		for n != nil {
			ch <- n
			n = n.right
		}
		close(ch)
	}()
	return ch
}

func (l *childDll) insertInPlace(n *node) {
	right := l.firstChild

	if right == nil {
		l.firstChild = n
		return
	}

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

func (l *childDll) removeChild(key string) {
	n := l.firstChild

	if n == nil {
		return
	}

	for n != nil {
		if n.key == key {
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
		n = n.right
	}
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
		addEventSet:      make(map[string]EventLog),
		deleteEventSet:   make(map[string]EventLog),
		positionEventSet: make(map[string]EventLog),
	}
}

// String implements Stringer so that we can get a nicely printable
// version of the CRDT internal tree structure.
func (crdt *crdtTree) String() string {
	var addNode func(t treeprint.Tree, n *node)
	addNode = func(t treeprint.Tree, n *node) {
		treeNode := t.AddBranch(fmt.Sprintf("%s (%v)", n.key, n.latestVectorClock))
		for c := range n.children.iter() {
			addNode(treeNode, c)
		}
	}

	tree := treeprint.New()
	rootNode := crdt.cache[crdtRootKey]
	addNode(tree, rootNode)

	return tree.String()
}

func (crdt *crdtTree) itemIsLive(key string) bool {
	latestAddEvent, isAdded := crdt.addEventSet[key]
	latestDeleteEvent, isDeleted := crdt.deleteEventSet[key]
	if isAdded && (!isDeleted || (isAdded && latestDeleteEvent.before(latestAddEvent))) {
		return true
	}
	return false
}

func (crdt *crdtTree) traverse() <-chan string {
	ch := make(chan string)
	go func() {
		root := crdt.cache[crdtRootKey]
		items := []*node{root}
		for len(items) > 0 {
			cur := items[0]
			items = items[1:]
			curChildren := []*node{}
			for c := range cur.children.iter() {
				curChildren = append(curChildren, c)
			}
			items = append(curChildren, items...)
			if cur.key != crdtRootKey && cur.key != crdtOrphanKey && cur.parent.key != crdtOrphanKey && crdt.itemIsLive(cur.key) {
				ch <- cur.key
			}
		}
		close(ch)
	}()
	return ch
}

func (crdt *crdtTree) generateEvents() []EventLog {
	events := []EventLog{}

	// Add all PositionEvents
	for _, e := range crdt.positionEventSet {
		events = append(events, e)
	}

	// Add UpdateEvents for active items (deleted items don't need content state, as any update that overrides deleted state will
	// come with it's own
	for k, addEvent := range crdt.addEventSet {
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
		target = &node{
			key:               e.TargetListItemKey,
			latestVectorClock: e.VectorClock,
		}
		crdt.cache[e.TargetListItemKey] = target
		orphanParent := crdt.cache[crdtOrphanKey]
		crdt.addToTargetChildArray(target, orphanParent)
	}

	item, exists := crdt.cache[e.ListItemKey]
	if !exists {
		item = &node{
			key: e.ListItemKey,
		}
		crdt.cache[e.ListItemKey] = item
	} else {
		// remove from parent.children if pre-existing
		item.parent.children.removeChild(item.key)
	}
	item.latestVectorClock = e.VectorClock

	crdt.addToTargetChildArray(item, target)
}
