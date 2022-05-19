package service

import (
	"fmt"
	"sort"

	"github.com/xlab/treeprint"
)

const (
	crdtRootKey   = ""
	crdtOrphanKey = "_orphan"
)

type crdtTree struct {
	cache map[string]*node
}

type node struct {
	key               string
	parent            *node
	children          []string
	latestVectorClock map[uuid]int64
}

func newTree() *crdtTree {
	orphan := &node{
		key: crdtOrphanKey,
	}
	root := &node{
		key:      crdtRootKey,
		children: []string{crdtOrphanKey},
	}
	orphan.parent = root
	return &crdtTree{
		cache: map[string]*node{
			crdtOrphanKey: orphan,
			crdtRootKey:   root,
		},
	}
}

// String implements Stringer so that we can get a nicely printable
// version of the CRDT internal tree structure.
func (crdt *crdtTree) String() string {
	var addNode func(t treeprint.Tree, n *node)
	addNode = func(t treeprint.Tree, n *node) {
		treeNode := t.AddBranch(fmt.Sprintf("%s (%v)", n.key, n.latestVectorClock))
		for _, c := range n.children {
			addNode(treeNode, crdt.cache[c])
		}
	}

	tree := treeprint.New()
	rootNode := crdt.cache[crdtRootKey]
	addNode(tree, rootNode)

	return tree.String()
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
			for _, k := range cur.children {
				curChildren = append(curChildren, crdt.cache[k])
			}
			items = append(curChildren, items...)
			if cur.key != crdtRootKey && cur.key != crdtOrphanKey && cur.parent.key != crdtOrphanKey {
				ch <- cur.key
			}
		}
		close(ch)
	}()
	return ch
}

func (crdt *crdtTree) addToTargetChildArray(item, target *node) {
	item.parent = target

	newChildren := append(target.children, item.key)

	sort.Slice(newChildren, func(i, j int) bool {
		if newChildren[i] == "_orphan" {
			return true
		} else if newChildren[j] == "_orphan" {
			return false
		}
		left := crdt.cache[newChildren[i]]
		right := crdt.cache[newChildren[j]]
		return !vectorClockBefore(left.latestVectorClock, right.latestVectorClock)
	})

	target.children = newChildren
}

func (crdt *crdtTree) removeFromParentChildArray(item *node) {
	newParentChildren := []string{}
	for _, c := range item.parent.children {
		if c != item.key {
			newParentChildren = append(newParentChildren, c)
		}
	}
	item.parent.children = newParentChildren
}

func (crdt *crdtTree) del(e EventLog) {
	item, exists := crdt.cache[e.ListItemKey]
	if !exists {
		return
	}
	crdt.removeFromParentChildArray(item)

	// if the parent is not an orphan child, we move all children of the deleted item
	// to the children of the parent (resolving with clocks as we go)
	if item.parent.key != crdtOrphanKey {
		for _, c := range item.children {
			crdt.addToTargetChildArray(crdt.cache[c], item.parent)
		}
		item.children = []string{}
	}

	orphan := crdt.cache[crdtOrphanKey]
	crdt.addToTargetChildArray(item, orphan)
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
		crdt.removeFromParentChildArray(item)
	}
	item.latestVectorClock = e.VectorClock

	crdt.addToTargetChildArray(item, target)
}

func (r *DBListRepo) Tree() *crdtTree {
	return r.crdt
}
