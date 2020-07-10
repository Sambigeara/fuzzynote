package service

import (
	"fmt"
	"log"
	"os"
	"path"
	"testing"
)

func TestServiceStoreLoad(t *testing.T) {
	t.Run("Loads from file", func(t *testing.T) {
		rootPath := "tests/test_file"
		mockListRepo := NewDBListRepo(rootPath, "")

		expectedLines := make([]string, 2)
		expectedLines[0] = "Test ListItem"
		expectedLines[1] = "Another test ListItem"

		err := mockListRepo.Load()
		if err != nil {
			t.Fatal(err)
		}

		if mockListRepo.root.Line != expectedLines[0] {
			t.Errorf("Expected %s but got %s", expectedLines[0], mockListRepo.root.Line)
		}

		expectedID := uint32(2)
		if mockListRepo.root.ID != expectedID {
			t.Errorf("Expected %d but got %d", expectedID, mockListRepo.root.ID)
		}

		if mockListRepo.root.Parent.Line != expectedLines[1] {
			t.Errorf("Expected %s but got %s", expectedLines[1], mockListRepo.root.Line)
		}

		expectedID = 1
		if mockListRepo.root.Parent.ID != expectedID {
			t.Errorf("Expected %d but got %d", expectedID, mockListRepo.root.Parent.ID)
		}
	})
	t.Run("Stores to new file and loads back", func(t *testing.T) {
		rootPath := "file_to_delete"
		mockListRepo := NewDBListRepo(rootPath, "")

		// Instantiate NextID index with initial empty load
		mockListRepo.Load()

		oldItem := ListItem{
			Line: "Old newly created line",
			ID:   uint32(1),
		}
		newItem := ListItem{
			Line:   "New newly created line",
			Parent: &oldItem,
			ID:     uint32(2),
		}
		oldItem.Child = &newItem

		mockListRepo.root = &newItem
		err := mockListRepo.Save()
		if err != nil {
			t.Fatal(err)
		}

		mockListRepo.Load()

		if mockListRepo.root.Line != newItem.Line {
			t.Errorf("Expected %s but got %s", newItem.Line, mockListRepo.root.Line)
		}

		expectedID := uint32(2)
		if mockListRepo.root.ID != expectedID {
			t.Errorf("Expected %d but got %d", expectedID, mockListRepo.root.ID)
		}

		if mockListRepo.root.Parent.Line != oldItem.Line {
			t.Errorf("Expected %s but got %s", mockListRepo.root.Parent.Line, oldItem.Line)
		}

		expectedID = uint32(1)
		if mockListRepo.root.Parent.ID != expectedID {
			t.Errorf("Expected %d but got %d", expectedID, mockListRepo.root.Parent.ID)
		}

		err = os.Remove(rootPath)
		if err != nil {
			log.Fatal(err)
		}
	})
	t.Run("Stores to new file and loads back", func(t *testing.T) {
		rootPath := "file_to_delete"
		mockListRepo := NewDBListRepo(rootPath, "")

		// Instantiate NextID index with initial empty load
		mockListRepo.Load()

		oldItem := ListItem{
			Line: "Old newly created line",
		}
		newItem := ListItem{
			Line:   "New newly created line",
			Parent: &oldItem,
		}
		oldItem.Child = &newItem

		mockListRepo.root = &newItem
		err := mockListRepo.Save()
		if err != nil {
			t.Fatal(err)
		}

		mockListRepo.Load()

		if mockListRepo.root.Line != newItem.Line {
			t.Errorf("Expected %s but got %s", newItem.Line, mockListRepo.root.Line)
		}

		expectedID := uint32(2)
		if mockListRepo.root.ID != expectedID {
			t.Errorf("Expected %d but got %d", expectedID, mockListRepo.root.ID)
		}

		if mockListRepo.root.Parent.Line != oldItem.Line {
			t.Errorf("Expected %s but got %s", mockListRepo.root.Parent.Line, oldItem.Line)
		}

		expectedID = uint32(1)
		if mockListRepo.root.Parent.ID != expectedID {
			t.Errorf("Expected %d but got %d", expectedID, mockListRepo.root.Parent.ID)
		}

		err = os.Remove(rootPath)
		if err != nil {
			log.Fatal(err)
		}
	})
}

func TestServiceAdd(t *testing.T) {
	rootPath := "file_to_delete"
	mockListRepo := NewDBListRepo(rootPath, "")

	item2 := ListItem{
		Line: "Old existing created line",
	}
	item1 := ListItem{
		Line:   "New existing created line",
		Parent: &item2,
	}
	item2.Child = &item1
	mockListRepo.root = &item1
	err := mockListRepo.Save()
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Add item at head of list", func(t *testing.T) {
		newLine := "Now I'm first"
		err := mockListRepo.Add(newLine, nil, nil)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)
		newItem := matches[0]

		expectedLen := 3
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if newItem != item1.Child {
			t.Errorf("New item should be original root's Child")
		}

		expectedID := uint32(3)
		if newItem.ID != expectedID {
			t.Errorf("Expected ID %d but got %d", expectedID, newItem.ID)
		}

		if mockListRepo.root != matches[0] {
			t.Errorf("item2 should be new root")
		}

		if matches[0].Line != newLine {
			t.Errorf("Expected %s but got %s", newLine, matches[0].Line)
		}

		if matches[0].Child != nil {
			t.Errorf("Newly generated listItem should have a nil Child")
		}

		if matches[0].Parent != &item1 {
			t.Errorf("Newly generated listItem has incorrect Parent")
		}

		if item1.Child != matches[0] {
			t.Errorf("Original young listItem has incorrect Child")
		}
	})

	t.Run("Add item at end of list", func(t *testing.T) {
		newLine := "I should be last"

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)
		oldLen := len(matches)

		err := mockListRepo.Add(newLine, nil, &item2)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ = mockListRepo.Match([][]rune{}, nil, true)
		newItem := matches[len(matches)-1]

		expectedLen := oldLen + 1
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if newItem != item2.Parent {
			t.Errorf("Returned item should be new bottom item")
		}

		expectedIdx := expectedLen - 1
		if matches[expectedIdx].Line != newLine {
			t.Errorf("Expected %s but got %s", newLine, matches[expectedIdx].Line)
		}

		if matches[expectedIdx].Parent != nil {
			t.Errorf("Newly generated listItem should have a nil Parent")
		}

		if matches[expectedIdx].Child != &item2 {
			t.Errorf("Newly generated listItem has incorrect Child")
		}

		if item2.Parent != matches[expectedIdx] {
			t.Errorf("Original youngest listItem has incorrect Parent")
		}
	})

	t.Run("Add item in middle of list", func(t *testing.T) {
		newLine := "I'm somewhere in the middle"

		oldParent := item1.Parent

		err := mockListRepo.Add(newLine, nil, &item1)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)

		expectedItem := matches[2]
		if expectedItem.Line != newLine {
			t.Errorf("Expected %s but got %s", newLine, expectedItem.Line)
		}

		if expectedItem.Parent != oldParent {
			t.Errorf("New item should have inherit old child's parent")
		}

		if item1.Parent != expectedItem {
			t.Errorf("Original youngest listItem has incorrect Parent")
		}

		if oldParent.Child != expectedItem {
			t.Errorf("Original old parent has incorrect Child")
		}
	})

	err = os.Remove(rootPath)
	if err != nil {
		log.Fatal(err)
	}
}

func TestServiceDelete(t *testing.T) {
	rootPath := "file_to_delete"
	mockListRepo := NewDBListRepo(rootPath, "")

	t.Run("Delete item from head of list", func(t *testing.T) {
		item3 := ListItem{
			Line: "Third",
		}
		item2 := ListItem{
			Line:   "Second",
			Parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			Parent: &item2,
		}
		item3.Child = &item2
		item2.Child = &item1
		mockListRepo.root = &item1
		mockListRepo.Save()

		err := mockListRepo.Delete(&item1)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)

		if matches[0] != &item2 {
			t.Errorf("item2 should be new root")
		}

		if mockListRepo.root != &item2 {
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

		if matches[0].Child != nil {
			t.Errorf("First item should have no child")
		}

		err = os.Remove(rootPath)
		if err != nil {
			log.Fatal(err)
		}
	})
	t.Run("Delete item from end of list", func(t *testing.T) {
		item3 := ListItem{
			Line: "Third",
		}
		item2 := ListItem{
			Line:   "Second",
			Parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			Parent: &item2,
		}
		item3.Child = &item2
		item2.Child = &item1
		mockListRepo.root = &item1
		mockListRepo.Save()

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

		if matches[expectedLen-1].Parent != nil {
			t.Errorf("Third item should have been deleted")
		}

		err = os.Remove(rootPath)
		if err != nil {
			log.Fatal(err)
		}
	})
	t.Run("Delete item from middle of list", func(t *testing.T) {
		item3 := ListItem{
			Line: "Third",
		}
		item2 := ListItem{
			Line:   "Second",
			Parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			Parent: &item2,
		}
		item3.Child = &item2
		item2.Child = &item1
		mockListRepo.root = &item1
		mockListRepo.Save()

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

		if matches[0].Parent != &item3 {
			t.Errorf("First item parent should be third item")
		}

		if matches[1].Child != &item1 {
			t.Errorf("Third item child should be first item")
		}

		err = os.Remove(rootPath)
		if err != nil {
			log.Fatal(err)
		}
	})
}

func TestServiceUpdate(t *testing.T) {
	rootPath := "file_to_delete"
	mockListRepo := NewDBListRepo(rootPath, "")

	item3 := ListItem{
		Line: "Third",
	}
	item2 := ListItem{
		Line:   "Second",
		Parent: &item3,
	}
	item1 := ListItem{
		Line:   "First",
		Parent: &item2,
	}
	item3.Child = &item2
	item2.Child = &item1
	mockListRepo.root = &item1
	mockListRepo.Save()

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

	err = os.Remove(rootPath)
	if err != nil {
		log.Fatal(err)
	}
}

func TestServiceMatch(t *testing.T) {
	rootPath := "file_to_delete"
	mockListRepo := NewDBListRepo(rootPath, "")

	t.Run("Match items in list", func(t *testing.T) {
		item5 := ListItem{
			Line: "Also not second",
		}
		item4 := ListItem{
			Line:   "Not second",
			Parent: &item5,
		}
		item3 := ListItem{
			Line:   "Third",
			Parent: &item4,
		}
		item2 := ListItem{
			Line:   "Second",
			Parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			Parent: &item2,
		}
		item5.Child = &item4
		item4.Child = &item3
		item3.Child = &item2
		item2.Child = &item1
		mockListRepo.root = &item1
		mockListRepo.Save()

		search := [][]rune{
			[]rune{'s', 'e', 'c', 'o', 'n', 'd'},
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

		err = os.Remove(rootPath)
		if err != nil {
			log.Fatal(err)
		}
	})

	t.Run("Match items in list", func(t *testing.T) {
		// Instantiate NextID index with initial empty load
		mockListRepo.Load()

		item5 := ListItem{
			Line: "Also not second",
		}
		item4 := ListItem{
			Line:   "Not second",
			Parent: &item5,
		}
		item3 := ListItem{
			Line:   "Third",
			Parent: &item4,
		}
		item2 := ListItem{
			Line:   "Second",
			Parent: &item3,
		}
		item1 := ListItem{
			Line:   "First",
			Parent: &item2,
		}
		item5.Child = &item4
		item4.Child = &item3
		item3.Child = &item2
		item2.Child = &item1
		mockListRepo.root = &item1
		mockListRepo.Save()

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

		err = os.Remove(rootPath)
		if err != nil {
			log.Fatal(err)
		}
	})
}

func TestServiceEditPage(t *testing.T) {
	rootPath := "file_to_delete"
	notesDir := "notes"
	os.MkdirAll(notesDir, os.ModePerm)

	mockListRepo := NewDBListRepo(rootPath, notesDir)

	oldNote := []byte("I am an old note")
	item2 := ListItem{
		Line: "Second",
	}
	item1 := ListItem{
		Line:   "First",
		Parent: &item2,
		Note:   &oldNote,
	}
	item2.Child = &item1
	mockListRepo.root = &item1
	mockListRepo.Save()

	stringToWrite := "I am a new line"
	dataToWrite := []byte(stringToWrite)

	err := mockListRepo.Update(item2.Line, &dataToWrite, &item2)
	if err != nil {
		t.Fatal(err)
	}

	if string(*item2.Note) != stringToWrite {
		t.Errorf("Expected line %s but got %s", stringToWrite, string(*item1.Note))
	}

	// Assert that file for first note does already exist
	strID1 := fmt.Sprint(item1.ID)
	expectedNotePath1 := path.Join(notesDir, strID1)
	if _, err := os.Stat(expectedNotePath1); os.IsNotExist(err) {
		t.Errorf("New file %s should already exist", expectedNotePath1)
	}

	// Assert that file for newly added note doesn't yet exist
	strID2 := fmt.Sprint(item2.ID)
	expectedNotePath2 := path.Join(notesDir, strID2)
	if _, err := os.Stat(expectedNotePath2); !os.IsNotExist(err) {
		t.Errorf("New file %s should not yet have been generated", expectedNotePath2)
	}

	mockListRepo.Save()

	// Assert that file does now exist after save
	if _, err := os.Stat(expectedNotePath2); os.IsNotExist(err) {
		t.Errorf("New file %s should have been generated", expectedNotePath2)
	}

	// Delete item2
	mockListRepo.Delete(&item2)

	// Assert that file still exists
	if _, err := os.Stat(expectedNotePath2); os.IsNotExist(err) {
		t.Errorf("New file %s should still exist", expectedNotePath2)
	}

	mockListRepo.Save()

	// Assert that file for newly added note has now been deleted
	if _, err := os.Stat(expectedNotePath2); !os.IsNotExist(err) {
		t.Errorf("New file %s should now have been deleted", expectedNotePath2)
	}

	err = os.Remove(rootPath)
	if err != nil {
		log.Fatal(err)
	}
	err = os.RemoveAll(notesDir)
	if err != nil {
		log.Fatal(err)
	}
}
