package service

import "sort"

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

func (t *crdtTree) traverse() <-chan string {
	ch := make(chan string)
	go func() {
		root := t.cache[crdtRootKey]
		items := []*node{root}
		for len(items) > 0 {
			cur := items[0]
			items = items[1:]
			curChildren := []*node{}
			for _, k := range cur.children {
				curChildren = append(curChildren, t.cache[k])
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

func (t *crdtTree) addToTargetChildArray(item, target *node, newVectorClock map[uuid]int64) {
	item.parent = target

	newChildren := append(target.children, item.key)

	sort.Slice(newChildren, func(i, j int) bool {
		if newChildren[i] == "_orphan" {
			return true
		} else if newChildren[j] == "_orphan" {
			return false
		}
		left := t.cache[newChildren[i]]
		right := t.cache[newChildren[j]]
		return !vectorClockBefore(left.latestVectorClock, right.latestVectorClock)
	})

	target.children = newChildren
}

func (t *crdtTree) removeFromTargetChildArray(item *node) {
	newParentChildren := []string{}
	for _, c := range item.parent.children {
		if c != item.key {
			newParentChildren = append(newParentChildren, c)
		}
	}
	item.parent.children = newParentChildren
}

func (t *crdtTree) del(e EventLog) {
	item, exists := t.cache[e.ListItemKey]
	if !exists {
		return
	}
	t.removeFromTargetChildArray(item)

	orphan := t.cache[crdtOrphanKey]
	t.addToTargetChildArray(item, orphan, e.VectorClock)
}

func (t *crdtTree) add(e EventLog) {
	target, exists := t.cache[e.TargetListItemKey]
	if !exists {
		target = &node{
			key:               e.TargetListItemKey,
			latestVectorClock: e.VectorClock,
		}
		t.cache[e.TargetListItemKey] = target
		orphanParent := t.cache[crdtOrphanKey]
		t.addToTargetChildArray(target, orphanParent, e.VectorClock)
	}

	item, exists := t.cache[e.ListItemKey]
	if !exists {
		item = &node{
			key:               e.ListItemKey,
			latestVectorClock: e.VectorClock,
		}
		t.cache[e.ListItemKey] = item
	} else {
		// remove from parent.children if pre-existing
		t.removeFromTargetChildArray(item)
	}

	t.addToTargetChildArray(item, target, e.VectorClock)
}
