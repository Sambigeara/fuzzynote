package service

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"strconv"
	"testing"
)

var (
	wd, _        = os.Getwd()
	rootDir      = path.Join(wd, "folder_to_delete")
	otherRootDir = path.Join(wd, "other_folder_to_delete")

	testEventChan chan interface{}
)

type testClient struct {
}

func (c *testClient) Refresh() {
}

type testCloseEvent struct{}

func (c *testClient) AwaitEvent() interface{} {
	//return struct{}{}
	return <-testEventChan
}

func (c *testClient) HandleEvent(ev interface{}) error {
	if _, isClose := ev.(testCloseEvent); isClose {
		return errors.New("test close")
	}
	return nil
}

func newTestClient() *testClient {
	return &testClient{}
}

func setupRepo() (*DBListRepo, func()) {
	localWalFile := NewLocalFileWalFile(rootDir)
	webTokenStore := NewFileWebTokenStore(rootDir)
	os.Mkdir(rootDir, os.ModePerm)
	repo := NewDBListRepo(localWalFile, webTokenStore)

	testEventChan = make(chan interface{})
	closeChan := make(chan struct{})

	go func() {
		repo.Start(newTestClient())
		closeChan <- struct{}{}
	}()

	closeFn := func() {
		go func() {
			testEventChan <- testCloseEvent{}
		}()
		<-closeChan

		walPathPattern := fmt.Sprintf(path.Join(rootDir, walFilePattern), "*")
		wals, _ := filepath.Glob(walPathPattern)
		for _, wal := range wals {
			os.Remove(wal)
		}
		walPathPattern = fmt.Sprintf(path.Join(otherRootDir, walFilePattern), "*")
		wals, _ = filepath.Glob(walPathPattern)
		for _, wal := range wals {
			os.Remove(wal)
		}

		os.Remove(rootDir)
		os.Remove(otherRootDir)
	}

	return repo, closeFn
}

func checkVectorClockEquality(a, b map[uuid]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}

func checkEventLogEquality(a, b EventLog) bool {
	if a.UUID != b.UUID ||
		!checkVectorClockEquality(a.VectorClock, b.VectorClock) ||
		//a.LamportTimestamp != b.LamportTimestamp ||
		a.EventType != b.EventType ||
		a.ListItemKey != b.ListItemKey ||
		a.TargetListItemKey != b.TargetListItemKey ||
		a.Line != b.Line ||
		string(a.Note) != string(b.Note) {
		return false
	}
	return true
}

func TestServicePushPull(t *testing.T) {
	t.Run("Pushes to file and pulls back", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		var lamport int64 = 0
		wal := []EventLog{
			{
				UUID: repo.uuid,
				VectorClock: map[uuid]int64{
					repo.uuid: lamport,
				},
				EventType:   AddEvent,
				ListItemKey: strconv.Itoa(int(repo.uuid)) + ":" + strconv.Itoa(int(lamport)),
				Line:        "Old newly created line",
			},
		}
		wal = append(wal, EventLog{
			UUID: repo.uuid,
			VectorClock: map[uuid]int64{
				repo.uuid: lamport,
			},
			EventType:         AddEvent,
			ListItemKey:       strconv.Itoa(int(repo.uuid)) + ":" + strconv.Itoa(int(lamport+1)),
			TargetListItemKey: strconv.Itoa(int(repo.uuid)) + ":" + strconv.Itoa(int(lamport)),
			Line:              "New newly created line",
		})

		localWalFile := NewLocalFileWalFile(rootDir)
		ctx := context.Background()

		repo.push(ctx, localWalFile, wal, nil, "")

		// Clear the cache to make sure we can pick the file up again
		repo.processedWalChecksums = make(map[string]struct{})

		c := make(chan namedWal)
		repo.pull(ctx, []WalFile{localWalFile}, c)
		nw := <-c

		if len(wal) != len(nw.wal) {
			t.Error("Pulled wal should be the same as the pushed one")
		}

		// Check equality of wals
		for i := range wal {
			o := wal[i]
			n := nw.wal[i]
			if !checkEventLogEquality(o, n) {
				t.Fatalf("Old event %v does not equal new event %v at index %d", o, n, i)
			}
		}
	})
}

func TestServiceAdd(t *testing.T) {
	t.Run("Add new item to empty list", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		newLine := "First item in list"
		repo.Add(newLine, nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)

		if len(matches) != 1 {
			t.Errorf("Matches should only have 1 item")
		}

		newItem := matches[0]
		if newItem.Line() != newLine {
			t.Errorf("Expected %s but got %s", newLine, newItem.Line())
		}
	})

	repo, clearUp := setupRepo()
	defer clearUp()

	repo.Add("Old existing created line", nil, nil)
	repo.Add("New existing created line", nil, nil)

	t.Run("Add item at head of list", func(t *testing.T) {
		newLine := "Now I'm first"
		repo.Add(newLine, nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)

		expectedLen := 3
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if matches[0].Line() != newLine {
			t.Errorf("Expected %s but got %s", newLine, matches[0].Line())
		}
	})
	t.Run("Add item at end of list", func(t *testing.T) {
		newLine := "I should be last"

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		oldParent := matches[len(matches)-1]

		repo.Add(newLine, nil, &oldParent)

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)
		newItem := matches[len(matches)-1]

		expectedLen := 4
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if newItem.Line() != newLine {
			t.Errorf("Expected %s but got %s", newLine, newItem.Line())
		}
	})
	t.Run("Add item in middle of list", func(t *testing.T) {
		newLine := "I'm somewhere in the middle"

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		root := repo.matchListItems[matches[0].key]

		repo.Add(newLine, nil, root)

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		if matches[0].key != root.key {
			t.Errorf("Root item should have remained unchanged")
		}

		newItem := matches[1]
		if newItem.Line() != newLine {
			t.Errorf("Expected %s but got %s", newLine, newItem.Line())
		}
	})
}

func TestServiceDelete(t *testing.T) {
	t.Run("Delete item from head of list", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		oldItem2 := repo.matchListItems[matches[1].key]

		repo.Delete(&matches[0])

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		expectedLen := 2
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if matches[0].Key() != oldItem2.Key() {
			t.Errorf("oldItem2 should be new root")
		}

		expectedLine := "Second"
		if matches[0].Line() != expectedLine {
			t.Errorf("Expected %s but got %s", expectedLine, matches[0].Line())
		}
	})
	t.Run("Delete item from end of list", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		expectedLastItem := repo.matchListItems[matches[1].key]

		repo.Delete(&(matches[len(matches)-1]))

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		expectedLen := 2
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if matches[expectedLen-1].Key() != expectedLastItem.Key() {
			t.Errorf("Last item should be item2")
		}

		expectedLine := "Second"
		if matches[expectedLen-1].Line() != expectedLine {
			t.Errorf("Expected %s but got %s", expectedLine, matches[expectedLen-1].Line())
		}
	})
	t.Run("Delete item from middle of list", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		repo.Delete(item2)
		repo.Delete(item2) // duplicate (idempotent) delete for good measure

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		expectedLen := 2
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if matches[0].Key() != item1.Key() {
			t.Errorf("First item should be previous first item")
		}

		if matches[1].Key() != item3.Key() {
			t.Errorf("Second item should be previous last item")
		}
	})
	t.Run("Delete nonexistent item", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		k := "key"
		repo.Delete(&ListItem{key: k})

		if matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0); len(matches) != 0 {
			t.Errorf("Matches should be len 0")
		}

		if _, exists := repo.crdt.deleteEventSet[k]; !exists {
			t.Errorf("Delete event should be present in the deleteEventSet")
		}
	})
	t.Run("Delete, update, then delete item again", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		repo.Delete(item2)
		repo.Update("", item2)
		repo.Delete(item2)

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		expectedLen := 2
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if matches[0].Key() != item1.Key() {
			t.Errorf("First item should be previous first item")
		}

		if matches[1].Key() != item3.Key() {
			t.Errorf("Second item should be previous last item")
		}
	})
	t.Run("Position event only then delete item", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		k := "key"
		matchK := "matchKey"
		matchChild := ListItem{
			key: matchK,
		}
		item := ListItem{
			key:        k,
			matchChild: &matchChild,
		}
		repo.MoveUp(&item)
		repo.Delete(&item)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		if l := len(matches); l != 0 {
			t.Errorf("Matches should be len 0 but is %d", l)
		}

		if _, exists := repo.crdt.positionEventSet[k]; !exists {
			t.Errorf("Position event should be present in the positionEventSet")
		}
		if _, exists := repo.crdt.deleteEventSet[k]; !exists {
			t.Errorf("Delete event should be present in the deleteEventSet")
		}
	})
}

func TestServiceMove(t *testing.T) {
	t.Run("Move item up from bottom all root children", func(t *testing.T) {
		repo, clearUp := setupRepo()
		repo.uuid = 1
		defer clearUp()

		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		repo.MoveUp(item3)

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		if matches[0].Key() != item1.Key() {
			t.Errorf("item1 should still be root")
		}
		if matches[1].Key() != item3.Key() {
			t.Errorf("item3 should have moved up one")
		}
		if matches[2].Key() != item2.Key() {
			t.Errorf("item2 should have moved down one")
		}
	})
	t.Run("Move item up from bottom single tree branch", func(t *testing.T) {
		repo, clearUp := setupRepo()
		repo.uuid = 1
		defer clearUp()

		k, _ := repo.Add("First", nil, nil)
		k, _ = repo.Add("Second", nil, repo.listItemCache[k])
		repo.Add("Third", nil, repo.listItemCache[k])

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		repo.MoveUp(item3)

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		if matches[0].Key() != item1.Key() {
			t.Errorf("item1 should still be root")
		}
		if matches[1].Key() != item3.Key() {
			t.Errorf("item3 should have moved up one")
		}
		if matches[2].Key() != item2.Key() {
			t.Errorf("item2 should have moved down one")
		}
	})
	t.Run("Move item up from middle all root children", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		repo.MoveUp(item2)

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		if matches[0].Key() != item2.Key() {
			t.Errorf("item2 should have become root")
		}
		if matches[1].Key() != item1.Key() {
			t.Errorf("previous root should have moved up one")
		}
		if matches[2].Key() != item3.Key() {
			t.Errorf("previous oldest should have stayed the same")
		}
	})
	t.Run("Move item up from middle single tree branch", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		k, _ := repo.Add("First", nil, nil)
		k, _ = repo.Add("Second", nil, repo.listItemCache[k])
		repo.Add("Third", nil, repo.listItemCache[k])

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		repo.MoveUp(item2)

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		if matches[0].Key() != item2.Key() {
			t.Errorf("item2 should have become root")
		}
		if matches[1].Key() != item1.Key() {
			t.Errorf("previous root should have moved up one")
		}
		if matches[2].Key() != item3.Key() {
			t.Errorf("previous oldest should have stayed the same")
		}
	})
	t.Run("Move item up from top all root children", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		repo.MoveUp(item1)

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		if matches[0].Key() != item1.Key() {
			t.Errorf("All items should remain unchanged")
		}
		if matches[1].Key() != item2.Key() {
			t.Errorf("All items should remain unchanged")
		}
		if matches[2].Key() != item3.Key() {
			t.Errorf("All items should remain unchanged")
		}
	})
	t.Run("Move item up from top single tree branch", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		k, _ := repo.Add("First", nil, nil)
		k, _ = repo.Add("Second", nil, repo.listItemCache[k])
		repo.Add("Third", nil, repo.listItemCache[k])

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		repo.MoveUp(item1)

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		if matches[0].Key() != item1.Key() {
			t.Errorf("All items should remain unchanged")
		}
		if matches[1].Key() != item2.Key() {
			t.Errorf("All items should remain unchanged")
		}
		if matches[2].Key() != item3.Key() {
			t.Errorf("All items should remain unchanged")
		}
	})
	t.Run("Move item down from top all root children", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		repo.MoveDown(item1)

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		if matches[0].Key() != item2.Key() {
			t.Errorf("item2 should now be root")
		}
		if matches[1].Key() != item1.Key() {
			t.Errorf("item1 should have moved down one")
		}
		if matches[2].Key() != item3.Key() {
			t.Errorf("item3 should still be at the bottom")
		}
	})
	t.Run("Move item down from top single tree branch", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		k, _ := repo.Add("First", nil, nil)
		k, _ = repo.Add("Second", nil, repo.listItemCache[k])
		repo.Add("Third", nil, repo.listItemCache[k])

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		repo.MoveDown(item1)

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		if matches[0].Key() != item2.Key() {
			t.Errorf("item2 should now be root")
		}
		if matches[1].Key() != item1.Key() {
			t.Errorf("item1 should have moved down one")
		}
		if matches[2].Key() != item3.Key() {
			t.Errorf("item3 should still be at the bottom")
		}
	})
	t.Run("Move item down from middle all root children", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		repo.MoveDown(item2)

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		if matches[0].Key() != item1.Key() {
			t.Errorf("Root should have remained the same")
		}
		if matches[1].Key() != item3.Key() {
			t.Errorf("previous oldest should have moved up one")
		}
		if matches[2].Key() != item2.Key() {
			t.Errorf("moved item should now be oldest")
		}
	})
	t.Run("Move item down from middle single tree branch", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		k, _ := repo.Add("First", nil, nil)
		k, _ = repo.Add("Second", nil, repo.listItemCache[k])
		repo.Add("Third", nil, repo.listItemCache[k])

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		repo.MoveDown(item2)

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		if matches[0].Key() != item1.Key() {
			t.Errorf("Root should have remained the same")
		}
		if matches[1].Key() != item3.Key() {
			t.Errorf("previous oldest should have moved up one")
		}
		if matches[2].Key() != item2.Key() {
			t.Errorf("moved item should now be oldest")
		}
	})
	t.Run("Move item down from bottom all root children", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		repo.MoveDown(item3)

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		if matches[0].Key() != item1.Key() {
			t.Errorf("All items should remain unchanged")
		}
		if matches[1].Key() != item2.Key() {
			t.Errorf("All items should remain unchanged")
		}
		if matches[2].Key() != item3.Key() {
			t.Errorf("All items should remain unchanged")
		}
	})
	t.Run("Move item down from bottom single tree branch", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		k, _ := repo.Add("First", nil, nil)
		k, _ = repo.Add("Second", nil, repo.listItemCache[k])
		repo.Add("Third", nil, repo.listItemCache[k])

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		repo.MoveDown(item3)

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		if matches[0].Key() != item1.Key() {
			t.Errorf("All items should remain unchanged")
		}
		if matches[1].Key() != item2.Key() {
			t.Errorf("All items should remain unchanged")
		}
		if matches[2].Key() != item3.Key() {
			t.Errorf("All items should remain unchanged")
		}
	})
	t.Run("Move item down from top to bottom all root children", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		repo.MoveDown(item1)

		// Update match pointers again
		repo.Match([][]rune{}, true, "", 0, 0)

		// ATM, the returned matches are copies of a source-of-truth index stored on the ListRepo, and
		// don't reflect the match pointers. To get correct state, we need to reference the item within
		// the index
		// TODO update this when I centralise the logic
		repo.MoveDown(repo.matchListItems[item1.Key()])

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		if matches[0].Key() != item2.Key() {
			t.Errorf("Root should be previous middle")
		}
		if matches[1].Key() != item3.Key() {
			t.Errorf("Previous oldest should have moved up one")
		}
		if matches[2].Key() != item1.Key() {
			t.Errorf("Preview root should have moved to the bottom")
		}
	})
	t.Run("Move item down from top to bottom single tree branch", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		k, _ := repo.Add("First", nil, nil)
		k, _ = repo.Add("Second", nil, repo.listItemCache[k])
		repo.Add("Third", nil, repo.listItemCache[k])

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		repo.MoveDown(item1)

		// Update match pointers again
		repo.Match([][]rune{}, true, "", 0, 0)

		// ATM, the returned matches are copies of a source-of-truth index stored on the ListRepo, and
		// don't reflect the match pointers. To get correct state, we need to reference the item within
		// the index
		// TODO update this when I centralise the logic
		repo.MoveDown(repo.matchListItems[item1.Key()])

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		if matches[0].Key() != item2.Key() {
			t.Errorf("Root should be previous middle")
		}
		if matches[1].Key() != item3.Key() {
			t.Errorf("Previous oldest should have moved up one")
		}
		if matches[2].Key() != item1.Key() {
			t.Errorf("Preview root should have moved to the bottom")
		}
	})
}

func TestServiceUpdate(t *testing.T) {
	t.Run("Add one and update", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		line := "New item"
		repo.Add(line, nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)

		updatedLine := "Updated item"
		repo.Update(updatedLine, &matches[0])

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		expectedLen := 1
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		match := matches[0]
		if match.Line() != updatedLine {
			t.Errorf("Expected %s but got %s", updatedLine, match.Line())
		}
	})
	t.Run("Update middle item", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		// Call matches to trigger matchListItems creation
		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)

		expectedLine := "Oooo I'm new"
		targetIdx := 1
		repo.Update(expectedLine, &(matches[targetIdx]))

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		expectedLen := 3
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if matches[targetIdx].Line() != expectedLine {
			t.Errorf("Expected %s but got %s", expectedLine, matches[1].Line())
		}
	})
}

func TestServiceMatch(t *testing.T) {
	t.Run("Full match items in list", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("Also not second", nil, nil)
		repo.Add("Not second", nil, nil)
		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item2 := repo.matchListItems[matches[1].key]
		item4 := repo.matchListItems[matches[3].key]
		item5 := repo.matchListItems[matches[4].key]

		search := [][]rune{
			{'s', 'e', 'c', 'o', 'n', 'd'},
		}

		matches, _, _ = repo.Match(search, true, "", 0, 0)

		if matches[0].Key() != item2.Key() {
			t.Errorf("First match is incorrect")
		}

		if matches[1].Key() != item4.Key() {
			t.Errorf("Second match is incorrect")
		}

		if matches[2].Key() != item5.Key() {
			t.Errorf("Third match is incorrect")
		}

		expectedLen := 3
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		// TODO remove this once logic is centralised
		p := repo.matchListItems

		if p[matches[0].Key()].matchChild != nil {
			t.Errorf("New root matchChild should be null")
		}
		if p[matches[0].Key()].matchParent.Key() != matches[1].Key() {
			t.Errorf("New root matchParent should be second match")
		}
		if p[matches[1].Key()].matchChild.Key() != matches[0].Key() {
			t.Errorf("Second item matchChild should be new root")
		}
		if p[matches[1].Key()].matchParent.Key() != matches[2].Key() {
			t.Errorf("Second item matchParent should be third match")
		}
		if p[matches[2].Key()].matchChild.Key() != matches[1].Key() {
			t.Errorf("Third item matchChild should be second match")
		}
		if p[matches[2].Key()].matchParent != nil {
			t.Errorf("Third item matchParent should be null")
		}

		expectedLine := "Second"
		if matches[0].Line() != expectedLine {
			t.Errorf("Expected line %s but got %s", expectedLine, matches[0].Line())
		}

		expectedLine = "Not second"
		if matches[1].Line() != expectedLine {
			t.Errorf("Expected line %s but got %s", expectedLine, matches[1].Line())
		}

		expectedLine = "Also not second"
		if matches[2].Line() != expectedLine {
			t.Errorf("Expected line %s but got %s", expectedLine, matches[2].Line())
		}
	})
	t.Run("Fuzzy match items in list", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("Also not second", nil, nil)
		repo.Add("Not second", nil, nil)
		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item2 := repo.matchListItems[matches[1].key]
		item4 := repo.matchListItems[matches[3].key]
		item5 := repo.matchListItems[matches[4].key]

		search := [][]rune{
			{'~', 's', 'c', 'o', 'n', 'd'},
		}

		matches, _, _ = repo.Match(search, true, "", 0, 0)

		if matches[0].Key() != item2.Key() {
			t.Errorf("First match is incorrect")
		}

		if matches[1].Key() != item4.Key() {
			t.Errorf("Second match is incorrect")
		}

		if matches[2].Key() != item5.Key() {
			t.Errorf("Third match is incorrect")
		}

		expectedLen := 3
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		// TODO remove this once logic is centralised
		p := repo.matchListItems

		if p[matches[0].Key()].matchChild != nil {
			t.Errorf("New root matchChild should be null")
		}
		if p[matches[0].Key()].matchParent.Key() != matches[1].Key() {
			t.Errorf("New root matchParent should be second match")
		}
		if p[matches[1].Key()].matchChild.Key() != matches[0].Key() {
			t.Errorf("Second item matchChild should be new root")
		}
		if p[matches[1].Key()].matchParent.Key() != matches[2].Key() {
			t.Errorf("Second item matchParent should be third match")
		}
		if p[matches[2].Key()].matchChild.Key() != matches[1].Key() {
			t.Errorf("Third item matchChild should be second match")
		}
		if p[matches[2].Key()].matchParent != nil {
			t.Errorf("Third item matchParent should be null")
		}

		expectedLine := "Second"
		if matches[0].Line() != expectedLine {
			t.Errorf("Expected line %s but got %s", expectedLine, matches[0].Line())
		}

		expectedLine = "Not second"
		if matches[1].Line() != expectedLine {
			t.Errorf("Expected line %s but got %s", expectedLine, matches[1].Line())
		}

		expectedLine = "Also not second"
		if matches[2].Line() != expectedLine {
			t.Errorf("Expected line %s but got %s", expectedLine, matches[2].Line())
		}
	})
	t.Run("Inverse match items in list", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("Also not second", nil, nil)
		repo.Add("Not second", nil, nil)
		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item3 := repo.matchListItems[matches[2].key]

		search := [][]rune{
			{'!', 's', 'e', 'c', 'o', 'n', 'd'},
		}

		matches, _, _ = repo.Match(search, true, "", 0, 0)

		if matches[0].Key() != item1.Key() {
			t.Errorf("First match is incorrect")
		}

		if matches[1].Key() != item3.Key() {
			t.Errorf("Active item should be returned even with no string match")
		}

		expectedLen := 2
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}
	})
	t.Run("Full match items in list with offset and limit", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("Fifth", nil, nil)
		repo.Add("Fourth", nil, nil)
		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		// Partial response
		matches, _, _ := repo.Match([][]rune{}, true, "", 2, 0)

		expectedLen := 3
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}
		if matches[0].Line() != "Third" {
			t.Error()
		}
		if matches[1].Line() != "Fourth" {
			t.Error()
		}
		if matches[2].Line() != "Fifth" {
			t.Error()
		}

		// Partial response with limit
		matches, _, _ = repo.Match([][]rune{}, true, "", 2, 2)
		expectedLen = 2
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}
		if matches[0].Line() != "Third" {
			t.Error()
		}
		if matches[1].Line() != "Fourth" {
			t.Error()
		}

		// Partial response with out-of-range limit
		matches, _, _ = repo.Match([][]rune{}, true, "", 2, 5)
		expectedLen = 3
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}
		if matches[0].Line() != "Third" {
			t.Error()
		}
		if matches[1].Line() != "Fourth" {
			t.Error()
		}
		if matches[2].Line() != "Fifth" {
			t.Error()
		}

		// Offset = boundary, zero response
		matches, _, _ = repo.Match([][]rune{}, true, "", 5, 0)
		expectedLen = 0
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		// Invalid offset
		matches, _, err := repo.Match([][]rune{}, true, "", -1, 0)
		if err == nil {
			t.Error()
		}
	})
	t.Run("Move item up from bottom with hidden middle", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		repo.Match([][]rune{}, true, "", 0, 0)

		// Hide middle item
		repo.ToggleVisibility(item2)

		// Preset Match pointers with Match call
		// showHidden needs to be set to `false`
		repo.Match([][]rune{}, false, "", 0, 0)

		repo.MoveUp(repo.matchListItems[item3.Key()])

		matches, _, _ = repo.Match([][]rune{}, false, "", 0, 0)

		expectedLen := 2
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if matches[0].Key() != item3.Key() {
			t.Errorf("item3 should now be root")
		}
		if matches[1].Key() != item1.Key() {
			t.Errorf("item1 should now be the lowest item")
		}

		// TODO remove this once logic is centralised
		p := repo.matchListItems

		if p[matches[0].Key()].matchChild != nil {
			t.Errorf("Moved item matchChild should now be nil")
		}
		if p[matches[0].Key()].matchParent.Key() != item1.Key() {
			t.Errorf("Moved item matchParent should be previous root")
		}

		if p[item1.Key()].matchChild.Key() != matches[0].Key() {
			t.Errorf("Previous root matchChild should be moved item")
		}
	})
	t.Run("Move item up persist between Save Load with hidden middle", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		repo.Match([][]rune{}, false, "", 0, 0)

		// Hide middle item
		repo.ToggleVisibility(item2)

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, false, "", 0, 0)

		repo.MoveUp(repo.matchListItems[item3.Key()])

		go func() {
			repo.Start(newTestClient())
		}()

		matches, _, _ = repo.Match([][]rune{}, false, "", 0, 0)

		if matches[0].Key() != item3.Key() {
			t.Errorf("item3 should now be root")
		}
		if matches[1].Key() != item1.Key() {
			t.Errorf("item1 should now be the lowest item")
		}

		// TODO remove this once logic is centralised
		p := repo.matchListItems

		if p[matches[0].Key()].matchChild != nil {
			t.Errorf("Moved item matchChild should now be nil")
		}
		if p[matches[0].Key()].matchParent.Key() != item1.Key() {
			t.Errorf("Moved item matchParent should be previous root")
		}
		if p[matches[1].Key()].matchChild.Key() != item3.Key() {
			t.Errorf("Previous root matchChild should be moved item")
		}
	})
	t.Run("Move item down persist between Save Load with hidden middle", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("Third", nil, nil)
		repo.Add("Second", nil, nil)
		repo.Add("First", nil, nil)

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		item1 := repo.matchListItems[matches[0].key]
		item2 := repo.matchListItems[matches[1].key]
		item3 := repo.matchListItems[matches[2].key]

		repo.Match([][]rune{}, false, "", 0, 0)

		// Hide middle item
		repo.ToggleVisibility(item2)

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, false, "", 0, 0)

		repo.MoveDown(repo.matchListItems[item1.Key()])

		matches, _, _ = repo.Match([][]rune{}, false, "", 0, 0)

		if matches[0].Key() != item3.Key() {
			t.Errorf("item3 should now be top match")
		}
		if matches[1].Key() != item1.Key() {
			t.Errorf("item1 should now be the lowest item")
		}

		// TODO remove this once logic is centralised
		p := repo.matchListItems

		if p[matches[0].Key()].matchChild != nil {
			t.Errorf("Moved item matchChild should now be nil")
		}
		if p[matches[0].Key()].matchParent.Key() != item1.Key() {
			t.Errorf("Moved item matchParent should be previous root")
		}
		if p[matches[1].Key()].matchChild.Key() != item3.Key() {
			t.Errorf("Previous root matchChild should be moved item")
		}
	})
}
