package service

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
)

const (
	rootDir           = "folder_to_delete"
	otherRootDir      = "other_folder_to_delete"
	testPushFrequency = 60000
)

var (
	rootPath      = path.Join(rootDir, rootFileName)
	otherRootPath = path.Join(otherRootDir, rootFileName)
)

type testClient struct {
}

func (c *testClient) Refresh() {
	//t.S.PostEvent(&RefreshKey{})
}

func (c *testClient) AwaitEvent() interface{} {
	//return t.S.PollEvent()
	return struct{}{}
}

func (c *testClient) HandleEvent(ev interface{}) (bool, error) {
	return false, nil
}

func newTestClient() *testClient {
	return &testClient{}
}

func clearUp() {
	os.Remove(rootPath)
	os.Remove(otherRootPath)

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

	files, err := filepath.Glob("wal_*.db")
	if err != nil {
		panic(err)
	}
	for _, f := range files {
		if err := os.Remove(f); err != nil {
			panic(err)
		}
	}
}

func generateProcessingWalChan() chan *[]EventLog {
	testWalChan := make(chan *[]EventLog)
	go func() {
		for {
			<-testWalChan
		}
	}()
	return testWalChan
}

func TestServicePushPull(t *testing.T) {
	t.Run("Pushes to file and pulls back", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)

		now := time.Now().UnixNano()
		wal := []EventLog{
			EventLog{
				UUID:                       repo.uuid,
				TargetUUID:                 0,
				ListItemCreationTime:       now,
				TargetListItemCreationTime: 0,
				UnixNanoTime:               now,
				EventType:                  AddEvent,
				Line:                       "Old newly created line",
			},
		}
		now++
		wal = append(wal, EventLog{
			UUID:                       repo.uuid,
			TargetUUID:                 0,
			ListItemCreationTime:       now,
			TargetListItemCreationTime: 0,
			UnixNanoTime:               now,
			EventType:                  AddEvent,
			Line:                       "New newly created line",
		})

		repo.push(&wal, localWalFile, "")
		// Clear the cache to make sure we can pick the file up again
		repo.processedPartialWals = make(map[string]struct{})
		newWal, _ := repo.pull([]WalFile{localWalFile})

		if len(wal) != len(*newWal) {
			t.Error("Pulled wal should be the same as the pushed one")
		}

		// Check equality of wals
		for i := range wal {
			oldEvent := wal[i]
			newEvent := (*newWal)[i]
			if !(cmp.Equal(oldEvent, newEvent, cmp.AllowUnexported(oldEvent, newEvent))) {
				t.Fatalf("Old event %v does not equal new event %v at index %d", oldEvent, newEvent, i)
			}
		}
	})
}

func TestServiceAdd(t *testing.T) {
	localWalFile := NewLocalFileWalFile(rootDir)
	webTokenStore := NewFileWebTokenStore(rootDir)
	os.Mkdir(rootDir, os.ModePerm)
	defer clearUp()
	repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
	testWalChan := generateProcessingWalChan()
	inputEvtChan := make(chan interface{})
	repo.Start(newTestClient(), testWalChan, inputEvtChan)
	repo.Add("Old existing created line", nil, 0)
	repo.Add("New existing created line", nil, 0)

	item1 := repo.Root
	item2 := repo.Root.parent

	t.Run("Add item at head of list", func(t *testing.T) {
		newLine := "Now I'm first"
		_, err := repo.Add(newLine, nil, 0)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true, "", 0, 0)
		matches := repo.matchListItems
		newItem := matches[0]

		expectedLen := 3
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if newItem != item1.child {
			t.Errorf("New item should be original root's child")
		}

		if repo.Root != newItem {
			t.Errorf("item2 should be new root")
		}

		if newItem.Line != newLine {
			t.Errorf("Expected %s but got %s", newLine, newItem.Line)
		}

		if newItem.child != nil {
			t.Errorf("Newly generated listItem should have a nil child")
		}

		if newItem.parent != item1 {
			t.Errorf("Newly generated listItem has incorrect parent")
		}

		if item1.child != newItem {
			t.Errorf("Original young listItem has incorrect child")
		}
	})

	t.Run("Add item at end of list", func(t *testing.T) {
		newLine := "I should be last"

		repo.Match([][]rune{}, true, "", 0, 0)
		matches := repo.matchListItems
		oldLen := len(matches)
		oldParent := matches[len(matches)-1]

		_, err := repo.Add(newLine, nil, oldLen)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true, "", 0, 0)
		matches = repo.matchListItems
		newItem := matches[len(matches)-1]

		expectedLen := oldLen + 1
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if newItem != oldParent.parent {
			t.Errorf("Returned item should be new bottom item")
		}

		expectedIdx := expectedLen - 1
		if matches[expectedIdx].Line != newLine {
			t.Errorf("Expected %s but got %s", newLine, matches[expectedIdx].Line)
		}

		if matches[expectedIdx].parent != nil {
			t.Errorf("Newly generated listItem should have a nil parent")
		}

		if matches[expectedIdx].child != oldParent {
			t.Errorf("Newly generated listItem has incorrect child")
		}

		if item2.parent != matches[expectedIdx] {
			t.Errorf("Original youngest listItem has incorrect parent")
		}
	})

	t.Run("Add item in middle of list", func(t *testing.T) {
		newLine := "I'm somewhere in the middle"

		oldParent := item1.parent

		newIdx := 2
		_, err := repo.Add(newLine, nil, newIdx)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true, "", 0, 0)
		matches := repo.matchListItems

		expectedItem := matches[newIdx]
		if expectedItem.Line != newLine {
			t.Errorf("Expected %s but got %s", newLine, expectedItem.Line)
		}

		if expectedItem.parent != oldParent {
			t.Errorf("New item should have inherit old child's parent")
		}

		if item1.parent != expectedItem {
			t.Errorf("Original youngest listItem has incorrect parent")
		}

		if oldParent.child != expectedItem {
			t.Errorf("Original old parent has incorrect child")
		}
	})

	t.Run("Add new item to empty list", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
		repo.Start(newTestClient(), testWalChan, inputEvtChan)

		newLine := "First item in list"
		repo.Add(newLine, nil, 0)

		repo.Match([][]rune{}, true, "", 0, 0)
		matches := repo.matchListItems

		if len(matches) != 1 {
			t.Errorf("Matches should only have 1 item")
		}

		expectedItem := matches[0]
		if expectedItem.Line != newLine {
			t.Errorf("Expected %s but got %s", newLine, expectedItem.Line)
		}

		if expectedItem.child != nil {
			t.Errorf("New item should have no child")
		}

		if expectedItem.parent != nil {
			t.Errorf("New item should have no parent")
		}

		if repo.Root != expectedItem {
			t.Errorf("New item should be new root")
		}
	})

}

func TestServiceDelete(t *testing.T) {
	testWalChan := generateProcessingWalChan()
	inputEvtChan := make(chan interface{})
	t.Run("Delete item from head of list", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
		repo.Start(newTestClient(), testWalChan, inputEvtChan)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item2 := repo.Root.parent

		repo.Match([][]rune{}, true, "", 0, 0)

		_, err := repo.Delete(0)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true, "", 0, 0)
		matches := repo.matchListItems

		if matches[0] != item2 {
			t.Errorf("item2 should be new root")
		}

		if repo.Root != item2 {
			t.Errorf("item2 should be new root")
		}

		expectedLen := 2
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		expectedLine := "Second"
		if matches[0].Line != expectedLine {
			t.Errorf("Expected %s but got %s", expectedLine, matches[0].Line)
		}

		if matches[0].child != nil {
			t.Errorf("First item should have no child")
		}
	})
	t.Run("Delete item from end of list", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
		repo.Start(newTestClient(), testWalChan, inputEvtChan)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item2 := repo.Root.parent

		repo.Match([][]rune{}, true, "", 0, 0)

		_, err := repo.Delete(2)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true, "", 0, 0)
		matches := repo.matchListItems

		expectedLen := 2
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if matches[expectedLen-1] != item2 {
			t.Errorf("Last item should be item2")
		}

		expectedLine := "Second"
		if matches[expectedLen-1].Line != expectedLine {
			t.Errorf("Expected %s but got %s", expectedLine, matches[expectedLen-1].Line)
		}

		if matches[expectedLen-1].parent != nil {
			t.Errorf("Third item should have been deleted")
		}
	})
	t.Run("Delete item from middle of list", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
		repo.Start(newTestClient(), testWalChan, inputEvtChan)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item3 := repo.Root.parent.parent

		repo.Match([][]rune{}, true, "", 0, 0)

		_, err := repo.Delete(1)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true, "", 0, 0)
		matches := repo.matchListItems

		if matches[0] != item1 {
			t.Errorf("First item should be previous first item")
		}

		if matches[1] != item3 {
			t.Errorf("Second item should be previous last item")
		}

		expectedLen := 2
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if matches[0].parent != item3 {
			t.Errorf("First item parent should be third item")
		}

		if matches[1].child != item1 {
			t.Errorf("Third item child should be first item")
		}
	})
}

func TestServiceMove(t *testing.T) {
	testWalChan := generateProcessingWalChan()
	inputEvtChan := make(chan interface{})
	t.Run("Move item up from bottom", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
		repo.Start(newTestClient(), testWalChan, inputEvtChan)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)
		matches := repo.matchListItems

		err := repo.MoveUp(len(matches) - 1)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true, "", 0, 0)
		matches = repo.matchListItems

		if repo.Root.Key() != item1.Key() {
			t.Errorf("item1 should still be root")
		}
		if matches[0].Key() != item1.Key() {
			t.Errorf("Root should have remained the same")
		}
		if matches[1].Key() != item3.Key() {
			t.Errorf("item3 should have moved up one")
		}
		if matches[2].Key() != item2.Key() {
			t.Errorf("item2 should have moved down one")
		}

		if matches[1].child.Key() != item1.Key() {
			t.Errorf("Moved item child should now be root")
		}
		if matches[1].parent.Key() != item2.Key() {
			t.Errorf("Moved item parent should be previous child")
		}

		if repo.Root.parent.Key() != item3.Key() {
			t.Errorf("Root parent should be newly moved item")
		}
		if matches[2].child.Key() != item3.Key() {
			t.Errorf("New lowest parent should be newly moved item")
		}
		if matches[2].parent != nil {
			t.Errorf("New lowest parent should have no parent")
		}
	})

	t.Run("Move item up from middle", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
		repo.Start(newTestClient(), testWalChan, inputEvtChan)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		err := repo.MoveUp(1)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true, "", 0, 0)
		matches := repo.matchListItems

		if repo.Root.Key() != item2.Key() {
			t.Errorf("item2 should have become root")
		}
		if matches[0].Key() != item2.Key() {
			t.Errorf("item2 should have become root")
		}
		if matches[1].Key() != item1.Key() {
			t.Errorf("previous root should have moved up one")
		}
		if matches[2].Key() != item3.Key() {
			t.Errorf("previous oldest should have stayed the same")
		}

		if matches[0].child != nil {
			t.Errorf("Moved item child should be null")
		}
		if matches[0].parent != item1 {
			t.Errorf("Moved item parent should be previous root")
		}

		if matches[1].parent.Key() != item3.Key() {
			t.Errorf("Old root parent should be unchanged oldest item")
		}
		if matches[1].child.Key() != item2.Key() {
			t.Errorf("Old root child should be new root item")
		}
		if matches[2].child.Key() != item1.Key() {
			t.Errorf("Lowest parent's child should be old root")
		}
	})

	t.Run("Move item up from top", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
		repo.Start(newTestClient(), testWalChan, inputEvtChan)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		err := repo.MoveUp(0)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true, "", 0, 0)
		matches := repo.matchListItems

		if repo.Root.Key() != item1.Key() {
			t.Errorf("All items should remain unchanged")
		}
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

	t.Run("Move item down from top", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
		repo.Start(newTestClient(), testWalChan, inputEvtChan)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		err := repo.MoveDown(0)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true, "", 0, 0)
		matches := repo.matchListItems

		if repo.Root.Key() != item2.Key() {
			t.Errorf("item2 should now be root")
		}
		if matches[0].Key() != item2.Key() {
			t.Errorf("item2 should now be root")
		}
		if matches[1].Key() != item1.Key() {
			t.Errorf("item1 should have moved down one")
		}
		if matches[2].Key() != item3.Key() {
			t.Errorf("item3 should still be at the bottom")
		}

		if matches[1].child.Key() != item2.Key() {
			t.Errorf("Moved item child should now be root")
		}
		if matches[2].child.Key() != item1.Key() {
			t.Errorf("Oldest item's child should be previous child")
		}

		if repo.Root.parent.Key() != item1.Key() {
			t.Errorf("Root parent should be newly moved item")
		}
		if matches[2].child.Key() != item1.Key() {
			t.Errorf("Lowest parent should be newly moved item")
		}
		if matches[2].parent != nil {
			t.Errorf("New lowest parent should have no parent")
		}
	})

	t.Run("Move item down from middle", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
		repo.Start(newTestClient(), testWalChan, inputEvtChan)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		err := repo.MoveDown(1)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true, "", 0, 0)
		matches := repo.matchListItems

		if repo.Root.Key() != item1.Key() {
			t.Errorf("Root should have remained the same")
		}
		if matches[0].Key() != item1.Key() {
			t.Errorf("Root should have remained the same")
		}
		if matches[1].Key() != item3.Key() {
			t.Errorf("previous oldest should have moved up one")
		}
		if matches[2].Key() != item2.Key() {
			t.Errorf("moved item should now be oldest")
		}

		if matches[2].child.Key() != item3.Key() {
			t.Errorf("Moved item child should be previous oldest")
		}
		if matches[2].parent != nil {
			t.Errorf("Moved item child should be null")
		}

		if matches[1].parent.Key() != item2.Key() {
			t.Errorf("Previous oldest parent should be new oldest item")
		}
		if matches[1].child.Key() != item1.Key() {
			t.Errorf("Previous oldest child should be unchanged root item")
		}
		if matches[0].parent.Key() != item3.Key() {
			t.Errorf("Root's parent should be moved item")
		}
	})

	t.Run("Move item down from bottom", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
		repo.Start(newTestClient(), testWalChan, inputEvtChan)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)
		matches := repo.matchListItems

		err := repo.MoveDown(len(matches) - 1)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true, "", 0, 0)
		matches = repo.matchListItems

		if repo.Root.Key() != item1.Key() {
			t.Errorf("All items should remain unchanged")
		}
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

	t.Run("Move item down from top to bottom", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
		repo.Start(newTestClient(), testWalChan, inputEvtChan)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true, "", 0, 0)

		err := repo.MoveDown(0)
		if err != nil {
			t.Fatal(err)
		}

		// We need to call Match again to reset match pointers prior to move, to avoid infinite loops
		repo.Match([][]rune{}, true, "", 0, 0)
		err = repo.MoveDown(1)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true, "", 0, 0)
		matches := repo.matchListItems

		if repo.Root.Key() != item2.Key() {
			t.Errorf("Root should be previous middle")
		}
		if matches[0].Key() != item2.Key() {
			t.Errorf("Root should be previous middle")
		}
		if matches[1].Key() != item3.Key() {
			t.Errorf("Previous oldest should have moved up one")
		}
		if matches[2].Key() != item1.Key() {
			t.Errorf("Preview root should have moved to the bottom")
		}

		if matches[0].child != nil {
			t.Errorf("New root should have nil child")
		}
		if matches[0].parent.Key() != item3.Key() {
			t.Errorf("New root parent should remain unchanged after two moves")
		}

		if matches[1].parent.Key() != item1.Key() {
			t.Errorf("Previous oldest's parent should be old root")
		}
		if matches[1].child.Key() != item2.Key() {
			t.Errorf("Previous oldest's child should have unchanged child")
		}
		if matches[2].child.Key() != item3.Key() {
			t.Errorf("New oldest child should be old oldest")
		}
		if matches[2].parent != nil {
			t.Errorf("New oldest should have no parent")
		}
	})
}

func TestServiceUpdate(t *testing.T) {
	localWalFile := NewLocalFileWalFile(rootDir)
	webTokenStore := NewFileWebTokenStore(rootDir)
	os.Mkdir(rootDir, os.ModePerm)
	defer clearUp()
	repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)

	testWalChan := generateProcessingWalChan()
	inputEvtChan := make(chan interface{})
	repo.Start(newTestClient(), testWalChan, inputEvtChan)

	repo.Add("Third", nil, 0)
	repo.Add("Second", nil, 0)
	repo.Add("First", nil, 0)

	// Call matches to trigger matchListItems creation
	repo.Match([][]rune{}, true, "", 0, 0)

	expectedLine := "Oooo I'm new"
	err := repo.Update(expectedLine, nil, 1)
	if err != nil {
		t.Fatal(err)
	}

	repo.Match([][]rune{}, true, "", 0, 0)
	matches := repo.matchListItems

	expectedLen := 3
	if len(matches) != expectedLen {
		t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
	}

	if matches[1].Line != expectedLine {
		t.Errorf("Expected %s but got %s", expectedLine, matches[1].Line)
	}
}

func TestServiceMatch(t *testing.T) {
	testWalChan := generateProcessingWalChan()
	inputEvtChan := make(chan interface{})
	t.Run("Full match items in list", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
		repo.Start(newTestClient(), testWalChan, inputEvtChan)
		repo.Add("Also not second", nil, 0)
		repo.Add("Not second", nil, 0)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item2 := repo.Root.parent
		item4 := repo.Root.parent.parent.parent
		item5 := repo.Root.parent.parent.parent.parent

		search := [][]rune{
			[]rune{'=', 's', 'e', 'c', 'o', 'n', 'd'},
		}
		_, _, err := repo.Match(search, true, "", 0, 0)
		matches := repo.matchListItems
		if err != nil {
			t.Fatal(err)
		}

		if matches[0] != item2 {
			t.Errorf("First match is incorrect")
		}

		if matches[1] != item4 {
			t.Errorf("Second match is incorrect")
		}

		if matches[2] != item5 {
			t.Errorf("Third match is incorrect")
		}

		expectedLen := 3
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if matches[0].matchChild != nil {
			t.Errorf("New root matchChild should be null")
		}
		if matches[0].matchParent != matches[1] {
			t.Errorf("New root matchParent should be second match")
		}
		if matches[1].matchChild != matches[0] {
			t.Errorf("Second item matchChild should be new root")
		}
		if matches[1].matchParent != matches[2] {
			t.Errorf("Second item matchParent should be third match")
		}
		if matches[2].matchChild != matches[1] {
			t.Errorf("Third item matchChild should be second match")
		}
		if matches[2].matchParent != nil {
			t.Errorf("Third item matchParent should be null")
		}

		expectedLine := "Second"
		if matches[0].Line != expectedLine {
			t.Errorf("Expected line %s but got %s", expectedLine, matches[0].Line)
		}

		expectedLine = "Not second"
		if matches[1].Line != expectedLine {
			t.Errorf("Expected line %s but got %s", expectedLine, matches[1].Line)
		}

		expectedLine = "Also not second"
		if matches[2].Line != expectedLine {
			t.Errorf("Expected line %s but got %s", expectedLine, matches[2].Line)
		}
	})

	t.Run("Fuzzy match items in list", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
		repo.Start(newTestClient(), testWalChan, inputEvtChan)
		repo.Add("Also not second", nil, 0)
		repo.Add("Not second", nil, 0)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item2 := repo.Root.parent
		item4 := repo.Root.parent.parent.parent
		item5 := repo.Root.parent.parent.parent.parent

		search := [][]rune{
			[]rune{'s', 'c', 'o', 'n', 'd'},
		}
		_, _, err := repo.Match(search, true, "", 0, 0)
		matches := repo.matchListItems
		if err != nil {
			t.Fatal(err)
		}

		if matches[0] != item2 {
			t.Errorf("First match is incorrect")
		}

		if matches[1] != item4 {
			t.Errorf("Second match is incorrect")
		}

		if matches[2] != item5 {
			t.Errorf("Third match is incorrect")
		}

		expectedLen := 3
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if matches[0].matchChild != nil {
			t.Errorf("New root matchChild should be null")
		}
		if matches[0].matchParent != matches[1] {
			t.Errorf("New root matchParent should be second match")
		}
		if matches[1].matchChild != matches[0] {
			t.Errorf("Second item matchChild should be new root")
		}
		if matches[1].matchParent != matches[2] {
			t.Errorf("Second item matchParent should be third match")
		}
		if matches[2].matchChild != matches[1] {
			t.Errorf("Third item matchChild should be second match")
		}
		if matches[2].matchParent != nil {
			t.Errorf("Third item matchParent should be null")
		}

		expectedLine := "Second"
		if matches[0].Line != expectedLine {
			t.Errorf("Expected line %s but got %s", expectedLine, matches[0].Line)
		}

		expectedLine = "Not second"
		if matches[1].Line != expectedLine {
			t.Errorf("Expected line %s but got %s", expectedLine, matches[1].Line)
		}

		expectedLine = "Also not second"
		if matches[2].Line != expectedLine {
			t.Errorf("Expected line %s but got %s", expectedLine, matches[2].Line)
		}
	})

	t.Run("Inverse match items in list", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
		repo.Start(newTestClient(), testWalChan, inputEvtChan)
		repo.Add("Also not second", nil, 0)
		repo.Add("Not second", nil, 0)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item3 := repo.Root.parent.parent

		search := [][]rune{
			[]rune{'=', '!', 's', 'e', 'c', 'o', 'n', 'd'},
		}
		_, _, err := repo.Match(search, true, "", 0, 0)
		matches := repo.matchListItems
		if err != nil {
			t.Fatal(err)
		}

		if matches[0] != item1 {
			t.Errorf("First match is incorrect")
		}

		if matches[1] != item3 {
			t.Errorf("Active item should be returned even with no string match")
		}

		expectedLen := 2
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}
	})

	t.Run("Full match items in list with offset and limit", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
		repo.Start(newTestClient(), testWalChan, inputEvtChan)
		repo.Add("Fifth", nil, 0)
		repo.Add("Fourth", nil, 0)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		// Partial response
		matches, _, _ := repo.Match([][]rune{}, true, "", 2, 0)
		expectedLen := 3
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}
		if matches[0].Line != "Third" {
			t.Error()
		}
		if matches[1].Line != "Fourth" {
			t.Error()
		}
		if matches[2].Line != "Fifth" {
			t.Error()
		}

		// Partial response with limit
		matches, _, _ = repo.Match([][]rune{}, true, "", 2, 2)
		expectedLen = 2
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}
		if matches[0].Line != "Third" {
			t.Error()
		}
		if matches[1].Line != "Fourth" {
			t.Error()
		}

		// Partial response with out-of-range limit
		matches, _, _ = repo.Match([][]rune{}, true, "", 2, 5)
		expectedLen = 3
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}
		if matches[0].Line != "Third" {
			t.Error()
		}
		if matches[1].Line != "Fourth" {
			t.Error()
		}
		if matches[2].Line != "Fifth" {
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

	t.Run("Move item up from bottom hidden middle", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
		repo.Start(newTestClient(), testWalChan, inputEvtChan)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		repo.Match([][]rune{}, false, "", 0, 0)

		// Hide middle item
		repo.ToggleVisibility(1)

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, false, "", 0, 0)
		matches := repo.matchListItems

		err := repo.MoveUp(len(matches) - 1)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, false, "", 0, 0)
		matches = repo.matchListItems

		if repo.Root.Key() != item3.Key() {
			t.Errorf("item3 should now be root")
		}
		if matches[0].Key() != item3.Key() {
			t.Errorf("item3 should now be root")
		}
		if matches[1].Key() != item1.Key() {
			t.Errorf("item1 should now be the lowest item")
		}

		if matches[0].child != nil {
			t.Errorf("Moved item child should now be nil")
		}
		if matches[0].matchChild != nil {
			t.Errorf("Moved item matchChild should now be nil")
		}
		if matches[0].parent.Key() != item1.Key() {
			t.Errorf("Moved item parent should be previous root")
		}
		if matches[0].matchParent.Key() != item1.Key() {
			t.Errorf("Moved item matchParent should be previous root")
		}

		if item1.child.Key() != matches[0].Key() {
			t.Errorf("Previous root child should be moved item")
		}
		if item1.matchChild.Key() != matches[0].Key() {
			t.Errorf("Previous root matchChild should be moved item")
		}
		if item1.parent.Key() != item2.Key() {
			t.Errorf("Previous root parent should be unchanged")
		}
		if item1.matchParent != nil {
			t.Errorf("Previous root matchParent should be nil")
		}

		if item2.child.Key() != item1.Key() {
			t.Errorf("Should be item1")
		}
		if item2.matchChild != nil {
			t.Errorf("Should be nil")
		}
		if item2.parent != nil {
			t.Errorf("Should be nil")
		}
		if item2.matchParent != nil {
			t.Errorf("Should be nil")
		}
	})

	t.Run("Move item up persist between Save Load with hidden middle", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
		repo.Start(newTestClient(), testWalChan, inputEvtChan)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		repo.Match([][]rune{}, false, "", 0, 0)

		// Hide middle item
		repo.ToggleVisibility(1)

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, false, "", 0, 0)
		matches := repo.matchListItems

		err := repo.MoveUp(len(matches) - 1)
		if err != nil {
			t.Fatal(err)
		}

		repo.Start(newTestClient(), testWalChan, inputEvtChan)
		repo.Stop()
		repo.Match([][]rune{}, false, "", 0, 0)
		matches = repo.matchListItems

		if repo.Root.Line != item3.Line {
			t.Errorf("item3 should now be root")
		}
		if matches[0].Line != item3.Line {
			t.Errorf("item3 should now be root")
		}
		if matches[1].Line != item1.Line {
			t.Errorf("item1 should now be the lowest item")
		}

		if matches[0].child != nil {
			t.Errorf("Moved item child should now be nil")
		}
		if matches[0].matchChild != nil {
			t.Errorf("Moved item matchChild should now be nil")
		}
		if matches[0].parent.Line != item1.Line {
			t.Errorf("Moved item parent should be previous root")
		}
		if matches[0].matchParent.Line != item1.Line {
			t.Errorf("Moved item matchParent should be previous root")
		}

		if matches[1].child.Line != item3.Line {
			t.Errorf("Previous root child should be moved item")
		}
		if matches[1].matchChild.Line != item3.Line {
			t.Errorf("Previous root matchChild should be moved item")
		}
		if matches[1].parent.Line != item2.Line {
			t.Errorf("Previous root parent should be unchanged")
		}
		if matches[1].matchParent != nil {
			t.Errorf("Previous root matchParent should be nil")
		}

		if item2.child != item1 {
			t.Errorf("Should be item1")
		}
		if item2.matchChild != nil {
			t.Errorf("Should be nil")
		}
		if item2.parent != nil {
			t.Errorf("Should be nil")
		}
		if item2.matchParent != nil {
			t.Errorf("Should be nil")
		}
	})

	t.Run("Move item down persist between Save Load with hidden middle", func(t *testing.T) {
		localWalFile := NewLocalFileWalFile(rootDir)
		webTokenStore := NewFileWebTokenStore(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp()
		repo := NewDBListRepo(localWalFile, webTokenStore, testPushFrequency, testPushFrequency)
		repo.Start(newTestClient(), testWalChan, inputEvtChan)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		repo.Match([][]rune{}, false, "", 0, 0)

		// Hide middle item
		repo.ToggleVisibility(1)

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, false, "", 0, 0)
		matches := repo.matchListItems

		err := repo.MoveDown(0)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, false, "", 0, 0)
		matches = repo.matchListItems

		if repo.Root.Line != item2.Line {
			t.Errorf("item2 (hidden) should now be root")
		}
		if matches[0].Line != item3.Line {
			t.Errorf("item3 should now be top match")
		}
		if matches[1].Line != item1.Line {
			t.Errorf("item1 should now be the lowest item")
		}

		if matches[0].child == nil || matches[0].child.Line != item2.Line {
			t.Errorf("Top match should still have child to hidden item")
		}
		if matches[0].matchChild != nil {
			t.Errorf("Moved item matchChild should now be nil")
		}
		if matches[0].parent.Line != item1.Line {
			t.Errorf("Moved item parent should be previous root")
		}
		if matches[0].matchParent.Line != item1.Line {
			t.Errorf("Moved item matchParent should be previous root")
		}

		if matches[1].child.Line != item3.Line {
			t.Errorf("Previous root child should be moved item")
		}
		if matches[1].matchChild.Line != item3.Line {
			t.Errorf("Previous root matchChild should be moved item")
		}
		if matches[1].parent != nil {
			t.Errorf("Previous root parent should be nil")
		}
		if matches[1].matchParent != nil {
			t.Errorf("Previous root matchParent should be nil")
		}

		if item2.child != nil {
			t.Errorf("Should be nil")
		}
		if item2.matchChild != nil {
			t.Errorf("Should be nil")
		}
		if item2.parent != item3 {
			t.Errorf("Should be item3")
		}
		if item2.matchParent != nil {
			t.Errorf("Should be nil")
		}
	})
}
