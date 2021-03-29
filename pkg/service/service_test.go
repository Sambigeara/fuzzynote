package service

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"path/filepath"
	//"runtime"
	"testing"
)

const rootDir string = "folder_to_delete"

var rootPath = path.Join(rootDir, rootFileName)

func clearUp(r *DBListRepo) {
	os.Remove(rootPath)

	walPathPattern := fmt.Sprintf(path.Join(rootDir, walFilePattern), "*")
	wals, _ := filepath.Glob(walPathPattern)
	for _, wal := range wals {
		os.Remove(wal)
	}
	os.Remove(path.Join(rootDir, syncFile))
	os.Remove(rootDir)

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

func TestServiceStoreLoad(t *testing.T) {
	t.Run("Stores to file and loads back", func(t *testing.T) {
		repo := NewDBListRepo(rootDir, []WalFile{})

		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		oldItem := ListItem{
			Line:         "Old newly created line",
			creationTime: int64(1),
		}
		newItem := ListItem{
			Line:         "New newly created line",
			parent:       &oldItem,
			creationTime: int64(2),
		}
		oldItem.child = &newItem

		repo.Root = &newItem
		repo.NextID = 3
		err := repo.Save([]WalFile{})
		if err != nil {
			t.Fatal(err)
		}

		// Check file schema version has been written correctly
		f, _ := os.OpenFile(repo.rootPath, os.O_CREATE, 0644)
		var fileSchema uint16
		binary.Read(f, binary.LittleEndian, &fileSchema)
		expectedSchemaID := uint16(3)
		if fileSchema != expectedSchemaID {
			t.Errorf("Incorrect set file schema. Expected %d but got %d", expectedSchemaID, fileSchema)
		}
		f.Close()

		err = repo.Load([]WalFile{})
		if err != nil {
			t.Fatal(err)
		}

		root := repo.Root

		if root.Line != newItem.Line {
			t.Errorf("File schema %d: Expected %s but got %s", repo.latestFileSchemaID, newItem.Line, root.Line)
		}

		expectedID := int64(2)
		if root.creationTime != expectedID {
			t.Errorf("File schema %d: Expected %d but got %d", repo.latestFileSchemaID, expectedID, root.creationTime)
		}

		if root.parent.Line != oldItem.Line {
			t.Errorf("File schema %d: Expected %s but got %s", repo.latestFileSchemaID, root.parent.Line, oldItem.Line)
		}

		expectedID = int64(1)
		if root.parent.creationTime != expectedID {
			t.Errorf("File schema %d: Expected %d but got %d", repo.latestFileSchemaID, expectedID, root.parent.creationTime)
		}
	})
}

func TestServiceAdd(t *testing.T) {
	repo := NewDBListRepo(rootDir, []WalFile{})
	repo.Add("Old existing created line", nil, 0)
	repo.Add("New existing created line", nil, 0)

	item1 := repo.Root
	item2 := repo.Root.parent

	os.Mkdir(rootDir, os.ModePerm)
	defer clearUp(repo)

	t.Run("Add item at head of list", func(t *testing.T) {
		newLine := "Now I'm first"
		err := repo.Add(newLine, nil, 0)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true)
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

		repo.Match([][]rune{}, true)
		matches := repo.matchListItems
		oldLen := len(matches)
		oldParent := matches[len(matches)-1]

		err := repo.Add(newLine, nil, oldLen)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true)
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
		err := repo.Add(newLine, nil, newIdx)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true)
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
		repo := NewDBListRepo("test", []WalFile{})

		newLine := "First item in list"
		repo.Add(newLine, nil, 0)

		repo.Match([][]rune{}, true)
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
	t.Run("Delete item from head of list", func(t *testing.T) {
		repo := NewDBListRepo(rootDir, []WalFile{})
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item2 := repo.Root.parent

		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		repo.Match([][]rune{}, true)

		err := repo.Delete(0)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true)
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
		repo := NewDBListRepo(rootDir, []WalFile{})
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item2 := repo.Root.parent

		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		repo.Match([][]rune{}, true)

		err := repo.Delete(2)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true)
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
		repo := NewDBListRepo(rootDir, []WalFile{})
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item3 := repo.Root.parent.parent

		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		repo.Match([][]rune{}, true)

		err := repo.Delete(1)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true)
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
	t.Run("Move item up from bottom", func(t *testing.T) {
		repo := NewDBListRepo(rootDir, []WalFile{})
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true)
		matches := repo.matchListItems

		_, err := repo.MoveUp(len(matches) - 1)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true)
		matches = repo.matchListItems

		if repo.Root != item1 {
			t.Errorf("item1 should still be root")
		}
		if matches[0] != item1 {
			t.Errorf("Root should have remained the same")
		}
		if matches[1] != item3 {
			t.Errorf("item3 should have moved up one")
		}
		if matches[2] != item2 {
			t.Errorf("item2 should have moved down one")
		}

		if item3.child != item1 {
			t.Errorf("Moved item child should now be root")
		}
		if item3.parent != item2 {
			t.Errorf("Moved item parent should be previous child")
		}

		if repo.Root.parent != item3 {
			t.Errorf("Root parent should be newly moved item")
		}
		if item2.child != item3 {
			t.Errorf("New lowest parent should be newly moved item")
		}
		if item2.parent != nil {
			t.Errorf("New lowest parent should have no parent")
		}
	})

	t.Run("Move item up from middle", func(t *testing.T) {
		repo := NewDBListRepo(rootDir, []WalFile{})
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true)

		_, err := repo.MoveUp(1)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true)
		matches := repo.matchListItems

		if repo.Root != item2 {
			t.Errorf("item2 should have become root")
		}
		if matches[0] != item2 {
			t.Errorf("item2 should have become root")
		}
		if matches[1] != item1 {
			t.Errorf("previous root should have moved up one")
		}
		if matches[2] != item3 {
			t.Errorf("previous oldest should have stayed the same")
		}

		if item2.child != nil {
			t.Errorf("Moved item child should be null")
		}
		if item2.parent != item1 {
			t.Errorf("Moved item parent should be previous root")
		}

		if item1.parent != item3 {
			t.Errorf("Old root parent should be unchanged oldest item")
		}
		if item1.child != item2 {
			t.Errorf("Old root child should be new root item")
		}
		if item3.child != item1 {
			t.Errorf("Lowest parent's child should be old root")
		}
	})

	t.Run("Move item up from top", func(t *testing.T) {
		repo := NewDBListRepo(rootDir, []WalFile{})
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true)

		_, err := repo.MoveUp(0)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true)
		matches := repo.matchListItems

		if repo.Root != item1 {
			t.Errorf("All items should remain unchanged")
		}
		if matches[0] != item1 {
			t.Errorf("All items should remain unchanged")
		}
		if matches[1] != item2 {
			t.Errorf("All items should remain unchanged")
		}
		if matches[2] != item3 {
			t.Errorf("All items should remain unchanged")
		}
	})

	t.Run("Move item down from top", func(t *testing.T) {
		repo := NewDBListRepo(rootDir, []WalFile{})
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true)

		_, err := repo.MoveDown(0)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true)
		matches := repo.matchListItems

		if repo.Root != item2 {
			t.Errorf("item2 should now be root")
		}
		if matches[0] != item2 {
			t.Errorf("item2 should now be root")
		}
		if matches[1] != item1 {
			t.Errorf("item1 should have moved down one")
		}
		if matches[2] != item3 {
			t.Errorf("item3 should still be at the bottom")
		}

		if item1.child != item2 {
			t.Errorf("Moved item child should now be root")
		}
		if item3.child != item1 {
			t.Errorf("Oldest item's child should be previous child")
		}

		if repo.Root.parent != item1 {
			t.Errorf("Root parent should be newly moved item")
		}
		if item3.child != item1 {
			t.Errorf("Lowest parent should be newly moved item")
		}
		if item3.parent != nil {
			t.Errorf("New lowest parent should have no parent")
		}
	})

	t.Run("Move item down from middle", func(t *testing.T) {
		repo := NewDBListRepo(rootDir, []WalFile{})
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true)

		_, err := repo.MoveDown(1)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true)
		matches := repo.matchListItems

		if repo.Root != item1 {
			t.Errorf("Root should have remained the same")
		}
		if matches[0] != item1 {
			t.Errorf("Root should have remained the same")
		}
		if matches[1] != item3 {
			t.Errorf("previous oldest should have moved up one")
		}
		if matches[2] != item2 {
			t.Errorf("moved item should now be oldest")
		}

		if item2.child != item3 {
			t.Errorf("Moved item child should be previous oldest")
		}
		if item2.parent != nil {
			t.Errorf("Moved item child should be null")
		}

		if item3.parent != item2 {
			t.Errorf("Previous oldest parent should be new oldest item")
		}
		if item3.child != item1 {
			t.Errorf("Previous oldest child should be unchanged root item")
		}
		if item1.parent != item3 {
			t.Errorf("Root's parent should be moved item")
		}
	})

	t.Run("Move item down from bottom", func(t *testing.T) {
		repo := NewDBListRepo(rootDir, []WalFile{})
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true)
		matches := repo.matchListItems

		_, err := repo.MoveDown(len(matches) - 1)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true)
		matches = repo.matchListItems

		if repo.Root != item1 {
			t.Errorf("All items should remain unchanged")
		}
		if matches[0] != item1 {
			t.Errorf("All items should remain unchanged")
		}
		if matches[1] != item2 {
			t.Errorf("All items should remain unchanged")
		}
		if matches[2] != item3 {
			t.Errorf("All items should remain unchanged")
		}
	})

	t.Run("Move item down from top to bottom", func(t *testing.T) {
		repo := NewDBListRepo(rootDir, []WalFile{})
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, true)

		_, err := repo.MoveDown(0)
		if err != nil {
			t.Fatal(err)
		}

		// We need to call Match again to reset match pointers prior to move, to avoid infinite loops
		repo.Match([][]rune{}, true)
		_, err = repo.MoveDown(1)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true)
		matches := repo.matchListItems

		if repo.Root != item2 {
			t.Errorf("Root should be previous middle")
		}
		if matches[0] != item2 {
			t.Errorf("Root should be previous middle")
		}
		if matches[1] != item3 {
			t.Errorf("Previous oldest should have moved up one")
		}
		if matches[2] != item1 {
			t.Errorf("Preview root should have moved to the bottom")
		}

		if item2.child != nil {
			t.Errorf("New root should have nil child")
		}
		if item2.parent != item3 {
			t.Errorf("New root parent should remain unchanged after two moves")
		}

		if item3.parent != item1 {
			t.Errorf("Previous oldest's parent should be old root")
		}
		if item3.child != item2 {
			t.Errorf("Previous oldest's child should have unchanged child")
		}
		if item1.child != item3 {
			t.Errorf("New oldest child should be old oldest")
		}
		if item1.parent != nil {
			t.Errorf("New oldest should have no parent")
		}
	})
}

func TestServiceUpdate(t *testing.T) {
	repo := NewDBListRepo(rootDir, []WalFile{})
	repo.Add("Third", nil, 0)
	repo.Add("Second", nil, 0)
	repo.Add("First", nil, 0)

	item2 := repo.Root.parent

	os.Mkdir(rootDir, os.ModePerm)
	defer clearUp(repo)

	// Call matches to trigger matchListItems creation
	repo.Match([][]rune{}, true)

	expectedLine := "Oooo I'm new"
	err := repo.Update(expectedLine, &[]byte{}, 1)
	if err != nil {
		t.Fatal(err)
	}

	repo.Match([][]rune{}, true)
	matches := repo.matchListItems

	expectedLen := 3
	if len(matches) != expectedLen {
		t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
	}

	if item2.Line != expectedLine {
		t.Errorf("Expected %s but got %s", expectedLine, item2.Line)
	}
}

func TestServiceMatch(t *testing.T) {
	t.Run("Full match items in list", func(t *testing.T) {
		repo := NewDBListRepo(rootDir, []WalFile{})
		repo.Add("Also not second", nil, 0)
		repo.Add("Not second", nil, 0)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item2 := repo.Root.parent
		item4 := repo.Root.parent.parent.parent
		item5 := repo.Root.parent.parent.parent.parent

		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		search := [][]rune{
			[]rune{'=', 's', 'e', 'c', 'o', 'n', 'd'},
		}
		_, err := repo.Match(search, true)
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
		repo := NewDBListRepo(rootDir, []WalFile{})
		repo.Add("Also not second", nil, 0)
		repo.Add("Not second", nil, 0)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item2 := repo.Root.parent
		item4 := repo.Root.parent.parent.parent
		item5 := repo.Root.parent.parent.parent.parent

		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		search := [][]rune{
			[]rune{'s', 'c', 'o', 'n', 'd'},
		}
		_, err := repo.Match(search, true)
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
		repo := NewDBListRepo(rootDir, []WalFile{})
		repo.Add("Also not second", nil, 0)
		repo.Add("Not second", nil, 0)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item3 := repo.Root.parent.parent

		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		search := [][]rune{
			[]rune{'=', '!', 's', 'e', 'c', 'o', 'n', 'd'},
		}
		_, err := repo.Match(search, true)
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

	t.Run("Move item up from bottom hidden middle", func(t *testing.T) {
		repo := NewDBListRepo(rootDir, []WalFile{})
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		repo.Match([][]rune{}, false)

		// Hide middle item
		repo.ToggleVisibility(1)

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, false)
		matches := repo.matchListItems

		_, err := repo.MoveUp(len(matches) - 1)
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, false)
		matches = repo.matchListItems

		if repo.Root != item3 {
			t.Errorf("item3 should now be root")
		}
		if matches[0] != item3 {
			t.Errorf("item3 should now be root")
		}
		if matches[1] != item1 {
			t.Errorf("item1 should now be the lowest item")
		}

		if item3.child != nil {
			t.Errorf("Moved item child should now be nil")
		}
		if item3.matchChild != nil {
			t.Errorf("Moved item matchChild should now be nil")
		}
		if item3.parent != item1 {
			t.Errorf("Moved item parent should be previous root")
		}
		if item3.matchParent != item1 {
			t.Errorf("Moved item matchParent should be previous root")
		}

		if item1.child != item3 {
			t.Errorf("Previous root child should be moved item")
		}
		if item1.matchChild != item3 {
			t.Errorf("Previous root matchChild should be moved item")
		}
		if item1.parent != item2 {
			t.Errorf("Previous root parent should be unchanged")
		}
		if item1.matchParent != nil {
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

	t.Run("Move item up persist between Save Load with hidden middle", func(t *testing.T) {
		walFiles := []WalFile{NewLocalWalFile(rootDir)}
		repo := NewDBListRepo(rootDir, walFiles)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		repo.Match([][]rune{}, false)

		// Hide middle item
		repo.ToggleVisibility(1)

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, false)
		matches := repo.matchListItems

		_, err := repo.MoveUp(len(matches) - 1)
		if err != nil {
			t.Fatal(err)
		}

		//runtime.Breakpoint()
		repo.Save(walFiles)
		repo = NewDBListRepo(rootDir, walFiles)
		repo.Load(walFiles)
		repo.Match([][]rune{}, false)
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
		walFiles := []WalFile{NewLocalWalFile(rootDir)}
		repo := NewDBListRepo(rootDir, walFiles)
		repo.Add("Third", nil, 0)
		repo.Add("Second", nil, 0)
		repo.Add("First", nil, 0)

		item1 := repo.Root
		item2 := repo.Root.parent
		item3 := repo.Root.parent.parent

		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		repo.Match([][]rune{}, false)

		// Hide middle item
		repo.ToggleVisibility(1)

		// Preset Match pointers with Match call
		repo.Match([][]rune{}, false)
		matches := repo.matchListItems

		_, err := repo.MoveDown(0)
		if err != nil {
			t.Fatal(err)
		}

		repo.Save(walFiles)
		repo = NewDBListRepo(rootDir, walFiles)
		repo.Load(walFiles)
		repo.Match([][]rune{}, false)
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
