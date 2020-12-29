package service

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestServiceStoreLoad(t *testing.T) {
	t.Run("Loads from file schema 0", func(t *testing.T) {
		// When loading from file schema 0 for the first time, it reverses the order
		// of the list items for historical reasons

		// Run for both schema type
		rootPath := "file_to_delete"

		fileDS0 := NewFileDataStore(rootPath, "", nil)
		fileDS0.latestFileSchemaID = 0

		fileDS1 := NewFileDataStore(rootPath, "", nil)

		f, err := os.Create(rootPath)
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(rootPath)

		expectedLines := make([]string, 2)
		expectedLines[0] = "Test ListItem"
		expectedLines[1] = "Another test ListItem"

		data := []interface{}{
			// No file schema defined
			listItemSchema0{
				1,
				0,
				1,
				uint64(len([]byte(expectedLines[0]))),
			},
			[]byte(expectedLines[0]),
			listItemSchema0{
				2,
				0,
				2,
				uint64(len([]byte(expectedLines[1]))),
			},
			[]byte(expectedLines[1]),
		}
		for _, v := range data {
			err = binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}

		f.Close()

		root, nextID, err := fileDS0.Load()
		if err != nil {
			t.Fatalf("Load failed when loading to file schema %d: %s", fileDS0.latestFileSchemaID, err)
		}
		repo0 := NewDBListRepo(root, nextID, nil)

		root, nextID, err = fileDS1.Load()
		if err != nil {
			t.Fatalf("Load failed when loading to file schema %d: %s", fileDS1.latestFileSchemaID, err)
		}
		repo1 := NewDBListRepo(root, nextID, nil)

		repos := []*DBListRepo{repo0, repo1}

		for i, repo := range repos {
			if repo.Root.Line != expectedLines[1] {
				t.Errorf("Repo file schema %d: Expected %s but got %s", i, expectedLines[1], repo.Root.Line)
			}

			expectedID := uint64(2)
			if repo.Root.id != expectedID {
				t.Errorf("Repo file schema %d: Expected %d but got %d", i, expectedID, repo.Root.id)
			}

			if repo.Root.parent.Line != expectedLines[0] {
				t.Errorf("Repo file schema %d: Expected %s but got %s", i, expectedLines[0], repo.Root.Line)
			}

			expectedID = 1
			if repo.Root.parent.id != expectedID {
				t.Errorf("Repo file schema %d: Expected %d but got %d", i, expectedID, repo.Root.parent.id)
			}
		}
	})
	t.Run("Loads from file schema 1", func(t *testing.T) {
		// Run for both schema type
		rootPath := "file_to_delete"

		fileDS0 := NewFileDataStore(rootPath, "", nil)
		fileDS0.latestFileSchemaID = 0

		fileDS1 := NewFileDataStore(rootPath, "", nil)

		f, err := os.Create(rootPath)
		if err != nil {
			t.Fatal(err)
		}
		defer os.Remove(rootPath)

		expectedLines := make([]string, 2)
		expectedLines[0] = "Test ListItem"
		expectedLines[1] = "Another test ListItem"

		data := []interface{}{
			uint16(1), // Schema type 1
			listItemSchema1{
				1,
				0,
				uint64(len([]byte(expectedLines[0]))),
				0,
			},
			[]byte(expectedLines[0]),
			listItemSchema1{
				2,
				0,
				uint64(len([]byte(expectedLines[1]))),
				0,
			},
			[]byte(expectedLines[1]),
		}
		for _, v := range data {
			err = binary.Write(f, binary.LittleEndian, v)
			if err != nil {
				t.Fatal(err)
			}
		}

		f.Close()

		root, nextID, err := fileDS0.Load()
		if err != nil {
			t.Fatalf("Load failed when loading to file schema %d: %s", fileDS0.latestFileSchemaID, err)
		}
		repo0 := NewDBListRepo(root, nextID, nil)

		root, nextID, err = fileDS1.Load()
		if err != nil {
			t.Fatalf("Load failed when loading to file schema %d: %s", fileDS1.latestFileSchemaID, err)
		}
		repo1 := NewDBListRepo(root, nextID, nil)

		repos := []*DBListRepo{repo0, repo1}

		for i, repo := range repos {
			if repo.Root.Line != expectedLines[0] {
				t.Errorf("Repo file schema %d: Expected %s but got %s", i, expectedLines[0], repo.Root.Line)
			}

			expectedID := uint64(1)
			if repo.Root.id != expectedID {
				t.Errorf("Repo file schema %d: Expected %d but got %d", i, expectedID, repo.Root.id)
			}

			if repo.Root.parent.Line != expectedLines[1] {
				t.Errorf("Repo file schema %d: Expected %s but got %s", i, expectedLines[1], repo.Root.Line)
			}

			expectedID = 2
			if repo.Root.parent.id != expectedID {
				t.Errorf("Repo file schema %d: Expected %d but got %d", i, expectedID, repo.Root.parent.id)
			}
		}
	})
	t.Run("Stores to file and loads back", func(t *testing.T) {
		// Run for both schema type
		rootPath0 := "temp0"
		rootPath1 := "temp1"
		walDirPattern := "wal_temp_%d.db"
		defer os.Remove(rootPath0)
		defer os.Remove(rootPath1)
		defer func() {
			files, err := filepath.Glob("wal_temp*")
			if err != nil {
				panic(err)
			}
			for _, f := range files {
				if err := os.Remove(f); err != nil {
					panic(err)
				}
			}

		}()

		walFile0 := NewWalFile(rootPath0, walDirPattern)
		walFile1 := NewWalFile(rootPath0, walDirPattern)

		fileDS0 := NewFileDataStore(rootPath0, "", walFile0)
		fileDS0.latestFileSchemaID = 0
		fileDS1 := NewFileDataStore(rootPath1, "", walFile1)

		fileDSs := []*FileDataStore{fileDS0, fileDS1}

		schemaIDMap := make(map[int]uint16)
		schemaIDMap[0] = 0
		schemaIDMap[1] = 3

		for i, fileDS := range fileDSs {
			oldItem := ListItem{
				Line: "Old newly created line",
				id:   uint64(1),
			}
			newItem := ListItem{
				Line:   "New newly created line",
				parent: &oldItem,
				id:     uint64(2),
			}
			oldItem.child = &newItem

			err := fileDS.Save(&newItem, []*ListItem{}, 3)
			if err != nil {
				t.Fatal(err)
			}

			// Check file schema version has been written correctly
			f, _ := os.OpenFile(fileDS.rootPath, os.O_CREATE, 0644)
			var fileSchema uint16
			binary.Read(f, binary.LittleEndian, &fileSchema)
			if fileSchema != schemaIDMap[i] {
				t.Errorf("Incorrect set file schema. Expected %d but got %d", i, fileSchema)
			}
			f.Close()

			root, _, err := fileDS.Load()
			if err != nil {
				t.Fatal(err)
			}

			if root.Line != newItem.Line {
				t.Errorf("File schema %d: Expected %s but got %s", i, newItem.Line, root.Line)
			}

			expectedID := uint64(2)
			if root.id != expectedID {
				t.Errorf("File schema %d: Expected %d but got %d", i, expectedID, root.id)
			}

			if root.parent.Line != oldItem.Line {
				t.Errorf("File schema %d: Expected %s but got %s", i, root.parent.Line, oldItem.Line)
			}

			expectedID = uint64(1)
			if root.parent.id != expectedID {
				t.Errorf("File schema %d: Expected %d but got %d", i, expectedID, root.parent.id)
			}
		}
	})
}

func TestServiceAdd(t *testing.T) {
	item2 := ListItem{
		Line: "Old existing created line",
		id:   2,
	}
	item1 := ListItem{
		Line:   "New existing created line",
		parent: &item2,
		id:     1,
	}
	item2.child = &item1

	mockListRepo := NewDBListRepo(&item1, 3, NewDbEventLogger())

	t.Run("Add item at head of list", func(t *testing.T) {
		newLine := "Now I'm first"
		err := mockListRepo.Add(newLine, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)
		newItem := matches[0]

		expectedLen := 3
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if newItem != item1.child {
			t.Errorf("New item should be original root's child")
		}

		expectedID := uint64(3)
		if newItem.id != expectedID {
			t.Errorf("Expected id %d but got %d", expectedID, newItem.id)
		}

		if mockListRepo.Root != matches[0] {
			t.Errorf("item2 should be new root")
		}

		if matches[0].Line != newLine {
			t.Errorf("Expected %s but got %s", newLine, matches[0].Line)
		}

		if matches[0].child != nil {
			t.Errorf("Newly generated listItem should have a nil child")
		}

		if matches[0].parent != &item1 {
			t.Errorf("Newly generated listItem has incorrect parent")
		}

		if item1.child != matches[0] {
			t.Errorf("Original young listItem has incorrect child")
		}
	})

	t.Run("Add item at end of list", func(t *testing.T) {
		newLine := "I should be last"

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)
		oldLen := len(matches)

		err := mockListRepo.Add(newLine, nil, &item2, nil)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ = mockListRepo.Match([][]rune{}, nil, true)
		newItem := matches[len(matches)-1]

		expectedLen := oldLen + 1
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if newItem != item2.parent {
			t.Errorf("Returned item should be new bottom item")
		}

		expectedIdx := expectedLen - 1
		if matches[expectedIdx].Line != newLine {
			t.Errorf("Expected %s but got %s", newLine, matches[expectedIdx].Line)
		}

		if matches[expectedIdx].parent != nil {
			t.Errorf("Newly generated listItem should have a nil parent")
		}

		if matches[expectedIdx].child != &item2 {
			t.Errorf("Newly generated listItem has incorrect child")
		}

		if item2.parent != matches[expectedIdx] {
			t.Errorf("Original youngest listItem has incorrect parent")
		}
	})

	t.Run("Add item in middle of list", func(t *testing.T) {
		newLine := "I'm somewhere in the middle"

		oldparent := item1.parent

		err := mockListRepo.Add(newLine, nil, &item1, nil)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)

		expectedItem := matches[2]
		if expectedItem.Line != newLine {
			t.Errorf("Expected %s but got %s", newLine, expectedItem.Line)
		}

		if expectedItem.parent != oldparent {
			t.Errorf("New item should have inherit old child's parent")
		}

		if item1.parent != expectedItem {
			t.Errorf("Original youngest listItem has incorrect parent")
		}

		if oldparent.child != expectedItem {
			t.Errorf("Original old parent has incorrect child")
		}
	})
}

func TestServiceDelete(t *testing.T) {
	t.Run("Delete item from head of list", func(t *testing.T) {
		item3 := ListItem{
			Line: "Third",
		}
		item2 := ListItem{
			Line:   "Second",
			parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			parent: &item2,
		}
		item3.child = &item2
		item2.child = &item1

		mockListRepo := NewDBListRepo(&item1, 1, NewDbEventLogger())

		err := mockListRepo.Delete(&item1)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)

		if matches[0] != &item2 {
			t.Errorf("item2 should be new root")
		}

		if mockListRepo.Root != &item2 {
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
		item3 := ListItem{
			Line: "Third",
		}
		item2 := ListItem{
			Line:   "Second",
			parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			parent: &item2,
		}
		item3.child = &item2
		item2.child = &item1
		mockListRepo := NewDBListRepo(&item1, 1, NewDbEventLogger())

		err := mockListRepo.Delete(&item3)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)

		expectedLen := 2
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if matches[expectedLen-1] != &item2 {
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
		item3 := ListItem{
			Line: "Third",
		}
		item2 := ListItem{
			Line:   "Second",
			parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			parent: &item2,
		}
		item3.child = &item2
		item2.child = &item1
		mockListRepo := NewDBListRepo(&item1, 1, NewDbEventLogger())

		err := mockListRepo.Delete(&item2)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)

		if matches[0] != &item1 {
			t.Errorf("First item should be previous first item")
		}

		if matches[1] != &item3 {
			t.Errorf("Second item should be previous last item")
		}

		expectedLen := 2
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if matches[0].parent != &item3 {
			t.Errorf("First item parent should be third item")
		}

		if matches[1].child != &item1 {
			t.Errorf("Third item child should be first item")
		}
	})
}

func TestServiceMove(t *testing.T) {
	t.Run("Move item up from bottom", func(t *testing.T) {
		item3 := ListItem{
			Line: "Third",
		}
		item2 := ListItem{
			Line:   "Second",
			parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			parent: &item2,
		}
		item3.child = &item2
		item2.child = &item1
		mockListRepo := NewDBListRepo(&item1, 1, NewDbEventLogger())

		// Preset Match pointers with Match call
		mockListRepo.Match([][]rune{}, nil, true)

		_, err := mockListRepo.MoveUp(&item3)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)

		if mockListRepo.Root != &item1 {
			t.Errorf("item1 should still be root")
		}
		if matches[0] != &item1 {
			t.Errorf("Root should have remained the same")
		}
		if matches[1] != &item3 {
			t.Errorf("item3 should have moved up one")
		}
		if matches[2] != &item2 {
			t.Errorf("item2 should have moved down one")
		}

		if item3.child != &item1 {
			t.Errorf("Moved item child should now be root")
		}
		if item3.parent != &item2 {
			t.Errorf("Moved item parent should be previous child")
		}

		if mockListRepo.Root.parent != &item3 {
			t.Errorf("Root parent should be newly moved item")
		}
		if item2.child != &item3 {
			t.Errorf("New lowest parent should be newly moved item")
		}
		if item2.parent != nil {
			t.Errorf("New lowest parent should have no parent")
		}
	})

	t.Run("Move item up from middle", func(t *testing.T) {
		item3 := ListItem{
			Line: "Third",
		}
		item2 := ListItem{
			Line:   "Second",
			parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			parent: &item2,
		}
		item3.child = &item2
		item2.child = &item1
		mockListRepo := NewDBListRepo(&item1, 1, NewDbEventLogger())

		// Preset Match pointers with Match call
		mockListRepo.Match([][]rune{}, nil, true)

		_, err := mockListRepo.MoveUp(&item2)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)

		if mockListRepo.Root != &item2 {
			t.Errorf("item2 should have become root")
		}
		if matches[0] != &item2 {
			t.Errorf("item2 should have become root")
		}
		if matches[1] != &item1 {
			t.Errorf("previous root should have moved up one")
		}
		if matches[2] != &item3 {
			t.Errorf("previous oldest should have stayed the same")
		}

		if item2.child != nil {
			t.Errorf("Moved item child should be null")
		}
		if item2.parent != &item1 {
			t.Errorf("Moved item parent should be previous root")
		}

		if item1.parent != &item3 {
			t.Errorf("Old root parent should be unchanged oldest item")
		}
		if item1.child != &item2 {
			t.Errorf("Old root child should be new root item")
		}
		if item3.child != &item1 {
			t.Errorf("Lowest parent's child should be old root")
		}
	})

	t.Run("Move item up from top", func(t *testing.T) {
		item3 := ListItem{
			Line: "Third",
		}
		item2 := ListItem{
			Line:   "Second",
			parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			parent: &item2,
		}
		item3.child = &item2
		item2.child = &item1
		mockListRepo := NewDBListRepo(&item1, 1, NewDbEventLogger())

		// Preset Match pointers with Match call
		mockListRepo.Match([][]rune{}, nil, true)

		_, err := mockListRepo.MoveUp(&item1)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)

		if mockListRepo.Root != &item1 {
			t.Errorf("All items should remain unchanged")
		}
		if matches[0] != &item1 {
			t.Errorf("All items should remain unchanged")
		}
		if matches[1] != &item2 {
			t.Errorf("All items should remain unchanged")
		}
		if matches[2] != &item3 {
			t.Errorf("All items should remain unchanged")
		}
	})

	t.Run("Move item down from top", func(t *testing.T) {
		item3 := ListItem{
			Line: "Third",
		}
		item2 := ListItem{
			Line:   "Second",
			parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			parent: &item2,
		}
		item3.child = &item2
		item2.child = &item1
		mockListRepo := NewDBListRepo(&item1, 1, NewDbEventLogger())

		// Preset Match pointers with Match call
		mockListRepo.Match([][]rune{}, nil, true)

		_, err := mockListRepo.MoveDown(&item1)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)

		if mockListRepo.Root != &item2 {
			t.Errorf("item2 should now be root")
		}
		if matches[0] != &item2 {
			t.Errorf("item2 should now be root")
		}
		if matches[1] != &item1 {
			t.Errorf("item1 should have moved down one")
		}
		if matches[2] != &item3 {
			t.Errorf("item3 should still be at the bottom")
		}

		if item1.child != &item2 {
			t.Errorf("Moved item child should now be root")
		}
		if item3.child != &item1 {
			t.Errorf("Oldest item's child should be previous child")
		}

		if mockListRepo.Root.parent != &item1 {
			t.Errorf("Root parent should be newly moved item")
		}
		if item3.child != &item1 {
			t.Errorf("Lowest parent should be newly moved item")
		}
		if item3.parent != nil {
			t.Errorf("New lowest parent should have no parent")
		}
	})

	t.Run("Move item down from middle", func(t *testing.T) {
		item3 := ListItem{
			Line: "Third",
		}
		item2 := ListItem{
			Line:   "Second",
			parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			parent: &item2,
		}
		item3.child = &item2
		item2.child = &item1
		mockListRepo := NewDBListRepo(&item1, 1, NewDbEventLogger())

		// Preset Match pointers with Match call
		mockListRepo.Match([][]rune{}, nil, true)

		_, err := mockListRepo.MoveDown(&item2)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)

		if mockListRepo.Root != &item1 {
			t.Errorf("Root should have remained the same")
		}
		if matches[0] != &item1 {
			t.Errorf("Root should have remained the same")
		}
		if matches[1] != &item3 {
			t.Errorf("previous oldest should have moved up one")
		}
		if matches[2] != &item2 {
			t.Errorf("moved item should now be oldest")
		}

		if item2.child != &item3 {
			t.Errorf("Moved item child should be previous oldest")
		}
		if item2.parent != nil {
			t.Errorf("Moved item child should be null")
		}

		if item3.parent != &item2 {
			t.Errorf("Previous oldest parent should be new oldest item")
		}
		if item3.child != &item1 {
			t.Errorf("Previous oldest child should be unchanged root item")
		}
		if item1.parent != &item3 {
			t.Errorf("Root's parent should be moved item")
		}
	})

	t.Run("Move item down from bottom", func(t *testing.T) {
		item3 := ListItem{
			Line: "Third",
		}
		item2 := ListItem{
			Line:   "Second",
			parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			parent: &item2,
		}
		item3.child = &item2
		item2.child = &item1
		mockListRepo := NewDBListRepo(&item1, 1, NewDbEventLogger())

		// Preset Match pointers with Match call
		mockListRepo.Match([][]rune{}, nil, true)

		_, err := mockListRepo.MoveDown(&item3)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)

		if mockListRepo.Root != &item1 {
			t.Errorf("All items should remain unchanged")
		}
		if matches[0] != &item1 {
			t.Errorf("All items should remain unchanged")
		}
		if matches[1] != &item2 {
			t.Errorf("All items should remain unchanged")
		}
		if matches[2] != &item3 {
			t.Errorf("All items should remain unchanged")
		}
	})

	t.Run("Move item down from top to bottom", func(t *testing.T) {
		item3 := ListItem{
			Line: "Third",
		}
		item2 := ListItem{
			Line:   "Second",
			parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			parent: &item2,
		}
		item3.child = &item2
		item2.child = &item1
		mockListRepo := NewDBListRepo(&item1, 1, NewDbEventLogger())

		// Preset Match pointers with Match call
		mockListRepo.Match([][]rune{}, nil, true)

		_, err := mockListRepo.MoveDown(&item1)
		if err != nil {
			t.Fatal(err)
		}

		// We need to call Match again to reset match pointers prior to move, to avoid infinite loops
		mockListRepo.Match([][]rune{}, nil, true)
		_, err = mockListRepo.MoveDown(&item1)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)

		if mockListRepo.Root != &item2 {
			t.Errorf("Root should be previous middle")
		}
		if matches[0] != &item2 {
			t.Errorf("Root should be previous middle")
		}
		if matches[1] != &item3 {
			t.Errorf("Previous oldest should have moved up one")
		}
		if matches[2] != &item1 {
			t.Errorf("Preview root should have moved to the bottom")
		}

		if item2.child != nil {
			t.Errorf("New root should have nil child")
		}
		if item2.parent != &item3 {
			t.Errorf("New root parent should remain unchanged after two moves")
		}

		if item3.parent != &item1 {
			t.Errorf("Previous oldest's parent should be old root")
		}
		if item3.child != &item2 {
			t.Errorf("Previous oldest's child should have unchanged child")
		}
		if item1.child != &item3 {
			t.Errorf("New oldest child should be old oldest")
		}
		if item1.parent != nil {
			t.Errorf("New oldest should have no parent")
		}
	})
}

func TestServiceUpdate(t *testing.T) {
	item3 := ListItem{
		Line: "Third",
	}
	item2 := ListItem{
		Line:   "Second",
		parent: &item3,
	}
	item1 := ListItem{
		Line:   "First",
		parent: &item2,
	}
	item3.child = &item2
	item2.child = &item1
	mockListRepo := NewDBListRepo(&item1, 1, NewDbEventLogger())

	expectedLine := "Oooo I'm new"
	err := mockListRepo.Update(expectedLine, &[]byte{}, &item2)
	if err != nil {
		t.Fatal(err)
	}

	matches, _ := mockListRepo.Match([][]rune{}, nil, true)

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
		item5 := ListItem{
			Line: "Also not second",
		}
		item4 := ListItem{
			Line:   "Not second",
			parent: &item5,
		}
		item3 := ListItem{
			Line:   "Third",
			parent: &item4,
		}
		item2 := ListItem{
			Line:   "Second",
			parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			parent: &item2,
		}
		item5.child = &item4
		item4.child = &item3
		item3.child = &item2
		item2.child = &item1
		mockListRepo := NewDBListRepo(&item1, 1, NewDbEventLogger())

		search := [][]rune{
			[]rune{'#', 's', 'e', 'c', 'o', 'n', 'd'},
		}
		matches, err := mockListRepo.Match(search, nil, true)
		if err != nil {
			t.Fatal(err)
		}

		if matches[0] != &item2 {
			t.Errorf("First match is incorrect")
		}

		if matches[1] != &item4 {
			t.Errorf("Second match is incorrect")
		}

		if matches[2] != &item5 {
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
		item5 := ListItem{
			Line: "Also not second",
		}
		item4 := ListItem{
			Line:   "Not second",
			parent: &item5,
		}
		item3 := ListItem{
			Line:   "Third",
			parent: &item4,
		}
		item2 := ListItem{
			Line:   "Second",
			parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			parent: &item2,
		}
		item5.child = &item4
		item4.child = &item3
		item3.child = &item2
		item2.child = &item1
		mockListRepo := NewDBListRepo(&item1, 1, nil)

		search := [][]rune{
			[]rune{'s', 'c', 'o', 'n', 'd'},
		}
		matches, err := mockListRepo.Match(search, nil, true)
		if err != nil {
			t.Fatal(err)
		}

		if matches[0] != &item2 {
			t.Errorf("First match is incorrect")
		}

		if matches[1] != &item4 {
			t.Errorf("Second match is incorrect")
		}

		if matches[2] != &item5 {
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

	t.Run("Match items in list with active", func(t *testing.T) {
		item5 := ListItem{
			Line: "Also not second",
		}
		item4 := ListItem{
			Line:   "Not second",
			parent: &item5,
		}
		item3 := ListItem{
			Line:   "Third",
			parent: &item4,
		}
		item2 := ListItem{
			Line:   "Second",
			parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			parent: &item2,
		}
		item5.child = &item4
		item4.child = &item3
		item3.child = &item2
		item2.child = &item1
		mockListRepo := NewDBListRepo(&item1, 1, nil)

		search := [][]rune{
			[]rune{'s', 'e', 'c', 'o', 'n', 'd'},
		}
		matches, err := mockListRepo.Match(search, &item3, true)
		if err != nil {
			t.Fatal(err)
		}

		if matches[0] != &item2 {
			t.Errorf("First match is incorrect")
		}

		if matches[1] != &item3 {
			t.Errorf("Active item should be returned even with no string match")
		}

		if matches[2] != &item4 {
			t.Errorf("Third match is incorrect")
		}

		if matches[3] != &item5 {
			t.Errorf("Fourth match is incorrect")
		}

		expectedLen := 4
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}
	})

	t.Run("Inverse match items in list", func(t *testing.T) {
		item5 := ListItem{
			Line: "Also not second",
		}
		item4 := ListItem{
			Line:   "Not second",
			parent: &item5,
		}
		item3 := ListItem{
			Line:   "Third",
			parent: &item4,
		}
		item2 := ListItem{
			Line:   "Second",
			parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			parent: &item2,
		}
		item5.child = &item4
		item4.child = &item3
		item3.child = &item2
		item2.child = &item1
		mockListRepo := NewDBListRepo(&item1, 1, nil)

		search := [][]rune{
			[]rune{'#', '!', 's', 'e', 'c', 'o', 'n', 'd'},
		}
		matches, err := mockListRepo.Match(search, nil, true)
		if err != nil {
			t.Fatal(err)
		}

		if matches[0] != &item1 {
			t.Errorf("First match is incorrect")
		}

		if matches[1] != &item3 {
			t.Errorf("Active item should be returned even with no string match")
		}

		expectedLen := 2
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}
	})
}
