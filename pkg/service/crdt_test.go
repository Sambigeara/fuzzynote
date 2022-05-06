package service

import (
	"container/list"
	"fmt"

	//"runtime"
	"testing"

	"github.com/google/btree"
)

const (
	walDirPattern = "wal_%v.db"
)

func TestCRDTEventEquality(t *testing.T) {
	t.Run("Check event comparisons", func(t *testing.T) {
		var lamport int64
		id := uuid(1)
		event1 := EventLog{
			VectorClock: map[uuid]int64{
				id: lamport,
			},
			UUID:      id,
			EventType: AddEvent,
		}

		event2 := EventLog{
			VectorClock: map[uuid]int64{
				id: lamport + 1,
			},
			UUID:      id,
			EventType: AddEvent,
		}

		equality := checkEquality(event1, event2)
		if equality != leftEventOlder {
			t.Fatalf("Expected left event to be older")
		}

		equality = checkEquality(event2, event1)
		if equality != rightEventOlder {
			t.Fatalf("Expected right event to be older")
		}

		equality = checkEquality(event1, event1)
		if equality != eventsEqual {
			t.Fatalf("Expected events to be equal")
		}
	})
}

func TestCRDTList(t *testing.T) {
	t.Run("Test add node and iterate", func(t *testing.T) {
		dll := list.New()

		k := "1"
		n := ListItem{
			key: k,
		}

		dll.PushFront(n)

		expectedLen := 1
		if l := dll.Len(); l != expectedLen {
			t.Fatalf("list should have len %d but has %d", expectedLen, l)
		}

		if n := dll.Front(); n == nil || n.Value.(ListItem).key != k {
			t.Fatalf("first node should contain the pushed event log")
		}
	})
}

func TestCRDTPositionTree(t *testing.T) {
	t.Run("Test put node and get", func(t *testing.T) {
		cache := btree.New(crdtPositionTreeDegree)

		n := positionTreeNode{}

		cache.ReplaceOrInsert(n)

		if cache.Get(n) == nil {
			t.Fatalf("node should be available in the cache")
		}

		expectedLen := 1
		if l := cache.Len(); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}
	})
	t.Run("Test put same node twice", func(t *testing.T) {
		cache := btree.New(crdtPositionTreeDegree)

		n := positionTreeNode{}

		cache.ReplaceOrInsert(n)
		cache.ReplaceOrInsert(n)

		if cache.Get(n) == nil {
			t.Fatalf("node should be available in the cache")
		}

		expectedLen := 1
		if l := cache.Len(); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}
	})
	t.Run("Test put two nodes different targets", func(t *testing.T) {
		cache := btree.New(crdtPositionTreeDegree)

		n0 := positionTreeNode{
			listItemKey: "1",
		}
		n1 := positionTreeNode{
			listItemKey: "2",
		}

		cache.ReplaceOrInsert(n0)
		cache.ReplaceOrInsert(n1)

		if cache.Get(n0) == nil {
			t.Fatalf("n0 should be available in the cache")
		}
		if cache.Get(n1) == nil {
			t.Fatalf("n1 should be available in the cache")
		}

		expectedLen := 2
		if l := cache.Len(); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		// n0 has "" target (aka root) so should be first
		if n0.listItemKey != cache.Min().(positionTreeNode).listItemKey {
			t.Fatalf("n0 should be the minimum node")
		}
		if n1.listItemKey != cache.Max().(positionTreeNode).listItemKey {
			t.Fatalf("n1 should be the maximum node")
		}
	})
}

func TestCRDTProcessEvent(t *testing.T) {
	t.Run("Test update event", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		key := "1"
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 1,
			},
			EventType:   UpdateEvent,
			ListItemKey: key,
		})

		expectedLen := 1
		if l := repo.crdtPositionTree.Len(); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		expectedNode := positionTreeNode{
			listItemKey: "",
		}
		if repo.crdtPositionTree.Get(expectedNode) == nil {
			t.Fatalf("expectedNode should be available in the cache")
		}

		treeNode := repo.crdtPositionTree.Min().(positionTreeNode)
		if expectedNode.listItemKey != treeNode.listItemKey {
			t.Fatalf("expectedNode should be the minimum node")
		}
		if expectedNode.listItemKey != treeNode.listItemKey {
			t.Fatalf("expectedNode should be the minimum node")
		}
		if treeNode.root == nil || treeNode.root.key != key {
			t.Fatalf("expectedNode should root listItem node should have key: %s", key)
		}
	})
	t.Run("Test update two linked events in order", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		nodeKey0 := "1"
		nodeKey1 := "2"
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 1,
			},
			EventType:   UpdateEvent,
			ListItemKey: nodeKey0,
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 2,
			},
			EventType:         UpdateEvent,
			ListItemKey:       nodeKey1,
			TargetListItemKey: nodeKey0,
		})

		expectedLen := 1
		if l := repo.crdtPositionTree.Len(); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		expectedNode := positionTreeNode{
			listItemKey: "",
		}

		if repo.crdtPositionTree.Get(expectedNode) == nil {
			t.Fatalf("expectedNode should be available in the cache")
		}

		treeNode := repo.crdtPositionTree.Min().(positionTreeNode)
		if expectedNode.listItemKey != treeNode.listItemKey {
			t.Fatalf("expectedNode should be the minimum node")
		}
		if treeNode.root == nil || treeNode.root.key != nodeKey0 {
			t.Fatalf("node0 should have key: %s", nodeKey0)
		}
		if treeNode.root.parent == nil || treeNode.root.parent.key != nodeKey1 {
			t.Fatalf("node1 should have key: %s", nodeKey1)
		}
	})
	t.Run("Test update two linked events reverse order", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		nodeKey0 := "1"
		nodeKey1 := "2"

		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 2,
			},
			EventType:         UpdateEvent,
			ListItemKey:       nodeKey1,
			TargetListItemKey: nodeKey0,
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 1,
			},
			EventType:   UpdateEvent,
			ListItemKey: nodeKey0,
		})

		expectedLen := 1
		if l := repo.crdtPositionTree.Len(); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		expectedNode := positionTreeNode{
			listItemKey: "",
		}

		if repo.crdtPositionTree.Get(expectedNode) == nil {
			t.Fatalf("expectedNode should be available in the cache")
		}

		treeNode := repo.crdtPositionTree.Min().(positionTreeNode)
		if expectedNode.listItemKey != treeNode.listItemKey {
			t.Fatalf("expectedNode should be the minimum node")
		}
		if treeNode.root == nil || treeNode.root.key != nodeKey0 {
			t.Fatalf("node0 should have key: %s", nodeKey0)
		}
		if treeNode.root.parent == nil || treeNode.root.parent.key != nodeKey1 {
			t.Fatalf("node1 should have key: %s", nodeKey1)
		}
	})
	t.Run("Test update two events targeting root", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		nodeKey0 := "1"
		nodeKey1 := "2"
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 1,
			},
			EventType:   UpdateEvent,
			ListItemKey: nodeKey0,
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 2,
			},
			EventType:   UpdateEvent,
			ListItemKey: nodeKey1,
		})

		expectedLen := 1
		if l := repo.crdtPositionTree.Len(); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		expectedNode := positionTreeNode{
			listItemKey: "",
		}

		if repo.crdtPositionTree.Get(expectedNode) == nil {
			t.Fatalf("expectedNode should be available in the cache")
		}

		treeNode := repo.crdtPositionTree.Min().(positionTreeNode)
		if expectedNode.listItemKey != treeNode.listItemKey {
			t.Fatalf("expectedNode should be the minimum node")
		}
		if treeNode.root == nil || treeNode.root.key != nodeKey1 {
			t.Fatalf("node0 should have key: %s", nodeKey1)
		}
		if treeNode.root.parent == nil || treeNode.root.parent.key != nodeKey0 {
			t.Fatalf("node1 should have key: %s", nodeKey0)
		}
	})
	t.Run("Test two partial linked lists with eventual merge", func(t *testing.T) {
		// End result should be items with ordered keys:
		// 1 <- 2 <- 3 <- 4 <- 5 <- 6 <- 7 <- 8
		// but we will add in the order, with sequential vector clocks:
		// 1, 3, 2, 4
		// 8, 6, 7, 5

		repo, clearUp := setupRepo()
		defer clearUp()

		i := int64(1)
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: i,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1",
			TargetListItemKey: "",
		})
		i++
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: i,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "3",
			TargetListItemKey: "2",
		})
		i++
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: i,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "2",
			TargetListItemKey: "1",
		})
		i++
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: i,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "4",
			TargetListItemKey: "3",
		})
		i++
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: i,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "8",
			TargetListItemKey: "7",
		})
		i++
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: i,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "6",
			TargetListItemKey: "5",
		})
		i++
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: i,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "7",
			TargetListItemKey: "6",
		})
		i++
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: i,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "5",
			TargetListItemKey: "4",
		})

		expectedLen := 1
		if l := repo.crdtPositionTree.Len(); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		expectedNode := positionTreeNode{
			listItemKey: "",
		}

		if repo.crdtPositionTree.Get(expectedNode) == nil {
			t.Fatalf("expectedNode should be available in the cache")
		}

		treeNode := repo.crdtPositionTree.Min().(positionTreeNode).root

		if treeNode.child != nil {
			t.Fatal("node root child should be nil")
		}

		n := treeNode
		var prev *ListItem
		for i := 1; i < 9; i++ {
			if n.key != fmt.Sprintf("%d", i) {
				t.Fatalf("expected listItem key %d but got %s", i, n.key)
			}

			if prev != nil {
				if n.child.key != prev.key {
					t.Fatal("node child should point to previous list item in iteration")
				}
				if prev.parent.key != n.key {
					t.Fatal("prev node parent should point to current list item in iteration")
				}
			}

			// This conditional is only here so we can run a post-check on the node, below
			if n.parent != nil {
				n = n.parent
			}
		}

		if n.parent != nil {
			t.Fatal("final node parent should be nil")
		}
	})
	t.Run("Test updates and delete from linked list", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 1,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1",
			TargetListItemKey: "",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 2,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "2",
			TargetListItemKey: "1",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 3,
			},
			EventType:   DeleteEvent,
			ListItemKey: "2",
		})

		expectedLen := 1
		if l := repo.crdtPositionTree.Len(); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		expectedNode := positionTreeNode{
			listItemKey: "",
		}

		if repo.crdtPositionTree.Get(expectedNode) == nil {
			t.Fatalf("expectedNode should be available in the cache")
		}

		treeNode := repo.crdtPositionTree.Min().(positionTreeNode).root

		c := 1
		n := treeNode
		for n.parent != nil {
			c++
			n = n.parent
		}

		nItems := 1
		if nItems != c {
			t.Fatalf("expected %d items in the linked list but got %d", nItems, c)
		}

		if treeNode.key != "1" {
			t.Fatalf("the wrong item was deleted from the linked list")
		}
	})
	t.Run("Test updates and delete root", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 1,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1",
			TargetListItemKey: "",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 2,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "2",
			TargetListItemKey: "1",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 3,
			},
			EventType:   DeleteEvent,
			ListItemKey: "1",
		})

		expectedLen := 1
		if l := repo.crdtPositionTree.Len(); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		// The same tree node remains, if there is an item remaining in the linked list
		expectedNode := positionTreeNode{
			listItemKey: "",
		}

		if repo.crdtPositionTree.Get(expectedNode) == nil {
			t.Fatalf("expectedNode should be available in the cache")
		}

		treeNode := repo.crdtPositionTree.Min().(positionTreeNode).root

		c := 1
		n := treeNode
		for n.parent != nil {
			c++
			n = n.parent
		}

		nItems := 1
		if nItems != c {
			t.Fatalf("expected %d items in the linked list but got %d", nItems, c)
		}

		if treeNode.key != "2" {
			t.Fatalf("the wrong item was deleted from the linked list")
		}
	})
	t.Run("Test delete before updates", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 3,
			},
			EventType:   DeleteEvent,
			ListItemKey: "1",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 1,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1",
			TargetListItemKey: "",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 2,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "2",
			TargetListItemKey: "1",
		})
		// throw a duplicate delete in for good measure
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 3,
			},
			EventType:   DeleteEvent,
			ListItemKey: "1",
		})

		expectedLen := 1
		if l := repo.crdtPositionTree.Len(); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		expectedNode := positionTreeNode{
			listItemKey: "",
		}

		if repo.crdtPositionTree.Get(expectedNode) == nil {
			t.Fatalf("expectedNode should be available in the cache")
		}

		treeNode := repo.crdtPositionTree.Min().(positionTreeNode).root

		c := 1
		n := treeNode
		for n.parent != nil {
			c++
			n = n.parent
		}

		nItems := 1
		if nItems != c {
			t.Fatalf("expected %d items in the linked list but got %d", nItems, c)
		}

		if treeNode.key != "2" {
			t.Fatalf("the wrong item was deleted from the linked list")
		}
	})
	t.Run("Test position two updates targetting deleted previous root item", func(t *testing.T) {
		// Add 1 <- 2 <- 3
		// Delete 1
		// Position 1 <- 3
		// should end up with: 3 <- 2, tree node key should be root, aka ""
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 1,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1",
			TargetListItemKey: "",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 2,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "2",
			TargetListItemKey: "1",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 3,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "3",
			TargetListItemKey: "2",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 4,
			},
			EventType:   DeleteEvent,
			ListItemKey: "1",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 5,
			},
			EventType:         PositionEvent,
			ListItemKey:       "3",
			TargetListItemKey: "1",
		})

		expectedLen := 1
		if l := repo.crdtPositionTree.Len(); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		expectedNode := positionTreeNode{
			listItemKey: "",
		}

		if repo.crdtPositionTree.Get(expectedNode) == nil {
			t.Fatalf("expectedNode should be available in the cache")
		}

		matches := []*ListItem{}
		for l := range repo.getListItems() {
			matches = append(matches, l)
		}

		nItems := 2
		if l := len(matches); nItems != l {
			t.Fatalf("expected %d items in the linked list but got %d", nItems, l)
		}

		if matches[0].key != "3" {
			t.Fatalf("root item has the wrong key")
		}
		if matches[1].key != "2" {
			t.Fatalf("root item has the wrong key")
		}
	})
	t.Run("Test position two updates targetting deleted previous central item", func(t *testing.T) {
		// Add 1 <- 2 <- 3 <- 4
		// Delete 1, 2
		// Position 2 <- 4
		// should end up with: 4 <- 3, tree node key should be root, aka ""
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 1,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1",
			TargetListItemKey: "",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 2,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "2",
			TargetListItemKey: "1",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 3,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "3",
			TargetListItemKey: "2",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 4,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "4",
			TargetListItemKey: "3",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 5,
			},
			EventType:   DeleteEvent,
			ListItemKey: "1",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 5,
			},
			EventType:   DeleteEvent,
			ListItemKey: "2",
		})
		repo.processEventLog(EventLog{
			VectorClock: map[uuid]int64{
				1: 6,
			},
			EventType:         PositionEvent,
			ListItemKey:       "4",
			TargetListItemKey: "2",
		})

		expectedLen := 1
		if l := repo.crdtPositionTree.Len(); l != expectedLen {
			t.Fatalf("cache should have len %d but has %d", expectedLen, l)
		}

		expectedNode := positionTreeNode{
			listItemKey: "",
		}

		if repo.crdtPositionTree.Get(expectedNode) == nil {
			t.Fatalf("expectedNode should be available in the cache")
		}

		matches := []*ListItem{}
		for l := range repo.getListItems() {
			matches = append(matches, l)
		}

		nItems := 2
		if l := len(matches); nItems != l {
			t.Fatalf("expected %d items in the linked list but got %d", nItems, l)
		}

		expectedKey := "4"
		if k := matches[0].key; k != expectedKey {
			t.Fatalf("matches[0] key should be %s but is %s", expectedKey, k)
		}

		expectedKey = "3"
		if k := matches[1].key; k != expectedKey {
			t.Fatalf("matches[1] key should be %s but is %s", expectedKey, k)
		}
	})
}

func TestCRDTMerge(t *testing.T) {
	repo, clearUp := setupRepo()
	repoUUID := uuid(1)
	repo.uuid = repoUUID

	exit := make(chan struct{})
	elChan := make(chan []EventLog)
	go func() {
		el := []EventLog{}
		for {
			select {
			case e := <-repo.eventsChan:
				el = append(el, e)
			case <-exit:
				elChan <- el
				return
			}
		}
	}()

	repo.Add("", nil, nil)
	n0 := repo.crdtPositionTree.Min().(positionTreeNode).root
	repo.Update("a", n0)

	repo.Add("", nil, n0)
	n1 := n0.parent
	repo.Update("b", n1)

	repo.Add("", nil, n1)
	n2 := n1.parent
	repo.Update("c", n2)

	// matchChild would usually be set during the Match() call, but set here manually
	n2.matchChild = n1
	repo.MoveUp(n2)

	repo.Delete(n0)

	go func() {
		exit <- struct{}{}
	}()
	correctEl := <-elChan

	// Clear up this repo state prior to running tests below - we only wanted the event log
	clearUp()

	checkFn := func(t *testing.T, n *ListItem) {
		// We expect two items in the list as follows: n2 ("c") -> n1 ("b")
		if n == nil {
			t.Errorf("node 1 should exist")
			//return
		}
		if n.parent == nil {
			t.Errorf("node 2 should exist")
			//return
		}

		if n.key != n2.key {
			t.Errorf("first item key is incorrect")
			//return
		}
		if n.rawLine != n2.rawLine {
			t.Errorf("first item rawLine is incorrect")
			//return
		}
		if n.parent.key != n1.key {
			t.Errorf("second item key is incorrect")
			//return
		}
		if n.parent.rawLine != n1.rawLine {
			t.Errorf("second item rawLine is incorrect")
			//return
		}

		if n.child != nil {
			t.Errorf("first item child should be nil")
			//return
		}
		if n.parent.parent != nil {
			t.Errorf("second item parent should be nil")
			//return
		}
		if n.parent.child.key != n2.key {
			t.Errorf("second item child should be first item")
		}
	}

	t.Run("Replay in order", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()
		repo.uuid = repoUUID

		el := make([]EventLog, len(correctEl))
		copy(el, correctEl)

		repo.Replay(el)

		checkFn(t, repo.crdtPositionTree.Min().(positionTreeNode).root)
	})
	t.Run("Replay adds in reverse order", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()
		repo.uuid = repoUUID

		el := make([]EventLog, len(correctEl))
		copy(el[0:2], correctEl[4:6]) // c
		copy(el[2:4], correctEl[2:4]) // b
		copy(el[4:6], correctEl[0:2]) // a
		copy(el[6:], correctEl[6:])   // move + delete

		repo.Replay(el)

		checkFn(t, repo.crdtPositionTree.Min().(positionTreeNode).root)
	})
	t.Run("Replay delete first", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()
		repo.uuid = repoUUID

		// TODO the below
		el := make([]EventLog, len(correctEl))
		copy(el[0:1], correctEl[7:8]) // delete
		copy(el[1:8], correctEl[0:7])

		repo.Replay(el)

		checkFn(t, repo.crdtPositionTree.Min().(positionTreeNode).root)
	})
	t.Run("Replay move first", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()
		repo.uuid = repoUUID

		// TODO the below
		el := make([]EventLog, len(correctEl))
		copy(el[0:1], correctEl[6:7]) // move
		copy(el[1:7], correctEl[0:6])
		copy(el[7:8], correctEl[7:8]) // delete

		repo.Replay(el)

		checkFn(t, repo.crdtPositionTree.Min().(positionTreeNode).root)
	})
	t.Run("Replay in reverse", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()
		repo.uuid = repoUUID

		el := make([]EventLog, len(correctEl))
		copy(el, correctEl)

		// Reverse the log
		for i, j := 0, len(el)-1; i < j; i, j = i+1, j-1 {
			el[i], el[j] = el[j], el[i]
		}

		repo.Replay(el)

		checkFn(t, repo.crdtPositionTree.Min().(positionTreeNode).root)
	})

	repo, clearUp = setupRepo()
	t.Run("All permutations", func(t *testing.T) {
		for i, p := range permutationsOfEvents(correctEl) {
			//for _, p := range permutationsOfEvents(correctEl) {
			//repo, clearUp := setupRepo()

			// We can't rely on fresh repos each iterations here because OS+file management lags behind and
			// causes inconsistencies. Therefore, use the same repo and refresh state
			repo.crdtPositionTree = btree.New(crdtPositionTreeDegree)
			repo.listItemCache = make(map[string]*ListItem)
			repo.addEventSet = make(map[string]EventLog)
			repo.deleteEventSet = make(map[string]EventLog)
			repo.positionEventSet = make(map[string]EventLog)

			// 8! == 40320
			//if i == 5050 {
			if i == 5051 {
				//runtime.Breakpoint()
			}

			repo.Replay(p)

			checkFn(t, repo.crdtPositionTree.Min().(positionTreeNode).root)
			//clearUp()
		}
	})
	clearUp()
}

func permutationsOfEvents(arr []EventLog) [][]EventLog {
	var helper func([]EventLog, int)
	res := [][]EventLog{}

	helper = func(arr []EventLog, n int) {
		if n == 1 {
			tmp := make([]EventLog, len(arr))
			copy(tmp, arr)
			res = append(res, tmp)
		} else {
			for i := 0; i < n; i++ {
				helper(arr, n-1)
				if n%2 == 1 {
					tmp := arr[i]
					arr[i] = arr[n-1]
					arr[n-1] = tmp
				} else {
					tmp := arr[0]
					arr[0] = arr[n-1]
					arr[n-1] = tmp
				}
			}
		}
	}
	helper(arr, len(arr))
	return res
}

func permutationsOfEventLogs(arr [][]EventLog) [][][]EventLog {
	var helper func([][]EventLog, int)
	res := [][][]EventLog{}

	helper = func(arr [][]EventLog, n int) {
		if n == 1 {
			tmp := make([][]EventLog, len(arr))
			copy(tmp, arr)
			res = append(res, tmp)
		} else {
			for i := 0; i < n; i++ {
				helper(arr, n-1)
				if n%2 == 1 {
					tmp := arr[i]
					arr[i] = arr[n-1]
					arr[n-1] = tmp
				} else {
					tmp := arr[0]
					arr[0] = arr[n-1]
					arr[n-1] = tmp
				}
			}
		}
	}
	helper(arr, len(arr))
	return res
}

func TestCRDTMergeReal(t *testing.T) {
	// The following are event logs generated by a real world scenario with inconsistent merge results
	el1 := []EventLog{
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 1,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:1",
			TargetListItemKey: "",
			Line:              "",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 2,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:1",
			TargetListItemKey: "",
			Line:              "a",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 3,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "",
		},
	}
	el2 := []EventLog{
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 4,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "h",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 5,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "he",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 6,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "hel",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 7,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "hell",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 8,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "hello",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 9,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "hello ",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 10,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "hello h",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 11,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "hello ho",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 12,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "hello how",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 13,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "hello how ",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 14,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "hello how a",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 15,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "hello how ar",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 16,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "hello how are",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 17,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "hello how are ",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 18,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "hello how are y",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 19,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "hello how are yo",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 20,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "hello how are you",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 21,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:21",
			TargetListItemKey: "1:3",
			Line:              "",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 22,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:21",
			TargetListItemKey: "1:3",
			Line:              "b",
		},
	}
	el3 := []EventLog{
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 23,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:23",
			TargetListItemKey: "1:21",
			Line:              "",
		},
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 24,
			},
			EventType:         UpdateEvent,
			ListItemKey:       "1:23",
			TargetListItemKey: "1:21",
			Line:              "c",
		},
	}
	el4 := []EventLog{
		{
			UUID: 1,
			VectorClock: map[uuid]int64{
				1: 25,
			},
			EventType:         DeleteEvent,
			ListItemKey:       "1:3",
			TargetListItemKey: "1:1",
			Line:              "hello how are you",
		},
	}

	checkFn := func(t *testing.T, matches []ListItem) {
		// We expect two items in the list as follows: n2 ("c") -> n1 ("b")
		// We expect 3 nodes, "a" -> "b" -> "c"
		n1 := matches[0]
		n2 := matches[1]
		n3 := matches[2]

		if n1.key != "1:1" {
			t.Errorf("node 1 key should be 1:1 but is %s", n1.key)
		}
		if n1.rawLine != "a" {
			t.Errorf("node 1 line should be a but is %s", n1.rawLine)
		}

		if n2.key != "1:21" {
			t.Errorf("node 2 key should be 1:21 but is %s", n2.key)
		}
		if n2.rawLine != "b" {
			t.Errorf("node 2 line should be b but is %s", n2.rawLine)
		}

		if n3.key != "1:23" {
			t.Errorf("node 3 key should be 1:23 but is %s", n3.key)
		}
		if n3.rawLine != "c" {
			t.Errorf("node 3 line should be c but is %s", n3.rawLine)
		}
	}

	t.Run("All permutations", func(t *testing.T) {
		el := [][]EventLog{el1, el2, el3, el4}
		for _, arr := range permutationsOfEventLogs(el) {
			repo, clearUp := setupRepo()

			el := make([]EventLog, 25)
			i := 0
			for _, partialEl := range arr {
				l := len(partialEl)
				copy(el[i:i+l], partialEl)
				i += l
			}

			repo.Replay(el)

			matches, _, _ := repo.Match([][]rune{}, false, "", 0, 0)
			checkFn(t, matches)
			clearUp()
		}
	})
}

//func TestWalFilter(t *testing.T) {
//    t.Run("Check includes matching item", func(t *testing.T) {
//        matchTerm := "foobar"
//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//            },
//        }
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 matchTerm,
//            ListItemCreationTime: creationTime,
//        })

//        wf := NewLocalFileWalFile(rootDir)
//        wf.pushMatchTerm = []rune(matchTerm)
//        matchedWal := getMatchedWal(&el, wf)
//        if len(*matchedWal) != 2 {
//            t.Fatalf("Matched wal should have the same number of events")
//        }
//    })
//    t.Run("Check includes matching and non matching items", func(t *testing.T) {
//        matchTerm := "foobar"
//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//            },
//        }
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 matchTerm,
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "",
//            ListItemCreationTime: creationTime + 1,
//        })

//        wf := NewLocalFileWalFile(rootDir)
//        wf.pushMatchTerm = []rune(matchTerm)
//        matchedWal := getMatchedWal(&el, wf)
//        if len(*matchedWal) != 2 {
//            t.Fatalf("Matched wal should not include the non matching event")
//        }
//    })
//    t.Run("Check includes previously matching item", func(t *testing.T) {
//        matchTerm := "foobar"
//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//            },
//        }
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 matchTerm,
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "",
//            ListItemCreationTime: creationTime,
//        })

//        wf := NewLocalFileWalFile(rootDir)
//        wf.pushMatchTerm = []rune(matchTerm)
//        matchedWal := getMatchedWal(&el, wf)
//        if len(*matchedWal) != 3 {
//            t.Fatalf("Matched wal should have the same number of events")
//        }
//    })
//    t.Run("Check doesn't include non matching items", func(t *testing.T) {
//        // We currently only operate on full matches
//        matchTerm := "fobar"
//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//            },
//        }
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "foobar",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "",
//            ListItemCreationTime: creationTime,
//        })

//        wf := NewLocalFileWalFile(rootDir)
//        wf.pushMatchTerm = []rune(matchTerm)
//        matchedWal := getMatchedWal(&el, wf)
//        if len(*matchedWal) != 0 {
//            t.Fatalf("Matched wal should have the same number of events")
//        }
//    })
//    t.Run("Check includes matching item mid line", func(t *testing.T) {
//        matchTerm := "foobar"
//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//            },
//        }
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 fmt.Sprintf("something something %s", matchTerm),
//            ListItemCreationTime: creationTime,
//        })

//        wf := NewLocalFileWalFile(rootDir)
//        wf.pushMatchTerm = []rune(matchTerm)
//        matchedWal := getMatchedWal(&el, wf)
//        if len(*matchedWal) != 2 {
//            t.Fatalf("Matched wal should have the same number of events")
//        }
//    })
//    t.Run("Check includes matching item after updates", func(t *testing.T) {
//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//            },
//        }
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "f",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "fo",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "foo",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "foob",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "fooba",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "foobar",
//            ListItemCreationTime: creationTime,
//        })

//        wf := NewLocalFileWalFile(rootDir)
//        wf.pushMatchTerm = []rune("foobar")
//        matchedWal := getMatchedWal(&el, wf)
//        if len(*matchedWal) != 7 {
//            t.Fatalf("Matched wal should have the same number of events")
//        }
//    })
//    t.Run("Check includes matching item no add with update", func(t *testing.T) {
//        matchTerm := "foobar"
//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//                Line:                 matchTerm,
//            },
//        }

//        wf := NewLocalFileWalFile(rootDir)
//        wf.pushMatchTerm = []rune(matchTerm)
//        matchedWal := getMatchedWal(&el, wf)
//        if len(*matchedWal) != 1 {
//            t.Fatalf("Matched wal should have the same number of events")
//        }
//    })
//    t.Run("Check includes matching item after post replay updates", func(t *testing.T) {
//        localWalFile := NewLocalFileWalFile(rootDir)
//        webTokenStore := NewFileWebTokenStore(rootDir)
//        os.Mkdir(rootDir, os.ModePerm)
//        defer clearUp()
//        repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//            },
//        }
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "f",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "fo",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "foo",
//            ListItemCreationTime: creationTime,
//        })

//        repo.log = &[]EventLog{}
//        repo.Replay(&el)
//        repo.Match([][]rune{}, true, "", 0, 0)
//        matches := repo.matchListItems
//        if len(matches) != 1 {
//            t.Fatalf("There should be one matched item")
//        }
//        if matches[0].Line != "foo" {
//            t.Fatalf("The item line should be %s", "foo")
//        }

//        eventTime++
//        newEl := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            UpdateEvent,
//                Line:                 "foo ",
//                ListItemCreationTime: creationTime,
//            },
//        }
//        repo.Replay(&newEl)
//        repo.Match([][]rune{}, true, "", 0, 0)
//        matches = repo.matchListItems
//        if len(matches) != 1 {
//            t.Fatalf("There should be one matched item")
//        }
//        if matches[0].Line != "foo " {
//            t.Fatalf("The item line should be %s", "foo ")
//        }

//        localWalFile.pushMatchTerm = []rune("foo")
//        matchedWal := getMatchedWal(repo.log, localWalFile)
//        if len(*matchedWal) != 5 {
//            t.Fatalf("Matched wal should have the same number of events")
//        }
//    })
//    t.Run("Check includes matching item after remote flushes further matching updates", func(t *testing.T) {
//        os.Mkdir(rootDir, os.ModePerm)
//        os.Mkdir(otherRootDir, os.ModePerm)
//        defer clearUp()

//        // Both repos will talk to the same walfile, but we'll have to instantiate separately, as repo1
//        // needs to set explicit match params
//        walFile1 := NewLocalFileWalFile(rootDir)
//        webTokenStore1 := NewFileWebTokenStore(rootDir)
//        repo1 := NewDBListRepo(walFile1, webTokenStore1, testPushFrequency, testPushFrequency)

//        walFile2 := NewLocalFileWalFile(otherRootDir)
//        webTokenStore2 := NewFileWebTokenStore(otherRootDir)
//        repo2 := NewDBListRepo(walFile2, webTokenStore2, testPushFrequency, testPushFrequency)

//        // Create copy
//        filteredWalFile := NewLocalFileWalFile(otherRootDir)
//        filteredWalFile.pushMatchTerm = []rune("foo")
//        repo1.AddWalFile(filteredWalFile)

//        uuid := uuid(1)
//        eventTime := time.Now().UnixNano()
//        creationTime := eventTime
//        el := []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            AddEvent,
//                ListItemCreationTime: creationTime,
//            },
//        }
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "f",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "fo",
//            ListItemCreationTime: creationTime,
//        })
//        eventTime++
//        el = append(el, EventLog{
//            UnixNanoTime:         eventTime,
//            UUID:                 uuid,
//            EventType:            UpdateEvent,
//            Line:                 "foo",
//            ListItemCreationTime: creationTime,
//        })

//        // repo1 pushes filtered wal to shared walfile
//        repo1.push(&el, filteredWalFile, "")
//        // repo2 pulls from shared walfile
//        filteredEl, _ := repo2.pull([]WalFile{walFile2})

//        // After replay, the remote repo should see a single matched item
//        repo2.Replay(filteredEl)
//        repo2.Match([][]rune{}, true, "", 0, 0)
//        matches := repo2.matchListItems
//        if len(matches) != 1 {
//            t.Fatalf("There should be one matched item")
//        }
//        if matches[0].Line != "foo" {
//            t.Fatalf("The item line should be %s", "foo ")
//        }

//        // Add another update event in original repo
//        eventTime++
//        el = []EventLog{
//            EventLog{
//                UnixNanoTime:         eventTime,
//                UUID:                 uuid,
//                EventType:            UpdateEvent,
//                Line:                 "foo ",
//                ListItemCreationTime: creationTime,
//            },
//        }

//        repo1.push(&el, filteredWalFile, "")
//        filteredEl, _ = repo2.pull([]WalFile{walFile2})
//        repo2.Replay(filteredEl)
//        repo2.Match([][]rune{}, true, "", 0, 0)
//        matches = repo2.matchListItems
//        if len(matches) != 1 {
//            t.Fatalf("There should be one matched item")
//        }
//        if matches[0].Line != "foo " {
//            t.Fatalf("The item line should be %s", "foo ")
//        }
//    })
//}
