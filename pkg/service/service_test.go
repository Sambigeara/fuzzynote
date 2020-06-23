package service

import (
	"log"
	"os"
	"testing"
)

func TestServiceStoreLoad(t *testing.T) {
	t.Run("Loads from file", func(t *testing.T) {
		rootPath := "test_file"
		mockListRepo := NewDBListRepo(rootPath)

		expectedLines := make([]string, 2)
		expectedLines[0] = "Test ListItem"
		expectedLines[1] = "Another test ListItem"

		listItems, err := mockListRepo.Load()
		if err != nil {
			t.Fatal(err)
		}

		if listItems[0].Line != expectedLines[0] {
			t.Errorf("Expected %s but got %s", expectedLines[0], listItems[0].Line)
		}

		if listItems[1].Line != expectedLines[1] {
			t.Errorf("Expected %s but got %s", expectedLines[1], listItems[1].Line)
		}
	})
	t.Run("Stores to new file and loads back", func(t *testing.T) {
		rootPath := "file_to_delete"
		mockListRepo := NewDBListRepo(rootPath)

		newListItems := make([]ListItem, 2)
		newListItems[0] = ListItem{
			Line: "I am a newly created line",
		}
		newListItems[1] = ListItem{
			Line: "I am a newly created line",
		}

		var err error
		err = mockListRepo.Save(newListItems)
		if err != nil {
			t.Fatal(err)
		}

		retLinkItems, _ := mockListRepo.Load()

		if retLinkItems[0].Line != newListItems[0].Line {
			t.Errorf("Expected %s but got %s", newListItems[0].Line, retLinkItems[0].Line)
		}

		if retLinkItems[1].Line != newListItems[1].Line {
			t.Errorf("Expected %s but got %s", newListItems[1].Line, retLinkItems[1].Line)
		}

		err = os.Remove(rootPath)
		if err != nil {
			log.Fatal(err)
		}
	})
}

func TestServiceAdd(t *testing.T) {
	rootPath := "file_to_delete"
	mockListRepo := NewDBListRepo(rootPath)
	newListItems := []ListItem{
		{"First"},
		{"Second"},
	}
	err := mockListRepo.Save(newListItems)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Add item at head of list", func(t *testing.T) {
		newLine := "Now I'm first"
		newListItems, err := mockListRepo.Add(newLine, 0, &newListItems)
		if err != nil {
			t.Fatal(err)
		}

		if newListItems[0].Line != newLine {
			t.Errorf("Expected %s but got %s", newLine, newListItems[0].Line)
		}
	})

	t.Run("Add item at end of list", func(t *testing.T) {
		newLine := "I should be last"

		retLinkItems, _ := mockListRepo.Load()
		idx := len(retLinkItems)

		newListItems, err := mockListRepo.Add(newLine, idx, &newListItems)
		if err != nil {
			t.Fatal(err)
		}

		if newListItems[idx].Line != newLine {
			t.Errorf("Expected %s but got %s", newLine, newListItems[idx].Line)
		}
	})

	t.Run("Add item in middle of list", func(t *testing.T) {
		newLine := "I'm somewhere in the middle"
		idx := 1

		newListItems, err := mockListRepo.Add(newLine, idx, &newListItems)
		if err != nil {
			t.Fatal(err)
		}

		if newListItems[idx].Line != newLine {
			t.Errorf("Expected %s but got %s", newLine, newListItems[idx].Line)
		}
	})

	err = os.Remove(rootPath)
	if err != nil {
		log.Fatal(err)
	}
}

func TestServiceDelete(t *testing.T) {
	rootPath := "file_to_delete"
	mockListRepo := NewDBListRepo(rootPath)

	t.Run("Delete item from head of list", func(t *testing.T) {
		listItems := []ListItem{
			{"First"},
			{"Second"},
			{"Third"},
		}

		err := mockListRepo.Save(listItems)
		if err != nil {
			t.Fatal(err)
		}

		listItems, err = mockListRepo.Delete(0, &listItems)
		if err != nil {
			t.Fatal(err)
		}

		expectedLen := 2
		if len(listItems) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(listItems))
		}

		expectedLine := "Second"
		if listItems[0].Line != expectedLine {
			t.Errorf("Expected %s but got %s", expectedLine, listItems[0].Line)
		}

		err = os.Remove(rootPath)
		if err != nil {
			log.Fatal(err)
		}
	})
	t.Run("Delete item from end of list", func(t *testing.T) {
		listItems := []ListItem{
			{"First"},
			{"Second"},
			{"Third"},
		}

		err := mockListRepo.Save(listItems)
		if err != nil {
			t.Fatal(err)
		}

		idx := len(listItems) - 1
		listItems, err = mockListRepo.Delete(idx, &listItems)
		if err != nil {
			t.Fatal(err)
		}

		expectedLen := 2
		if len(listItems) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(listItems))
		}

		expectedLine := "Second"
		if listItems[idx-1].Line != expectedLine {
			t.Errorf("Expected %s but got %s", expectedLine, listItems[idx-1].Line)
		}

		err = os.Remove(rootPath)
		if err != nil {
			log.Fatal(err)
		}
	})
	t.Run("Delete item from middle of list", func(t *testing.T) {
		listItems := []ListItem{
			{"First"},
			{"Second"},
			{"Third"},
		}

		err := mockListRepo.Save(listItems)
		if err != nil {
			t.Fatal(err)
		}

		idx := 1
		listItems, err = mockListRepo.Delete(idx, &listItems)
		if err != nil {
			t.Fatal(err)
		}

		expectedLen := 2
		if len(listItems) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(listItems))
		}

		expectedLine := "First"
		if listItems[0].Line != expectedLine {
			t.Errorf("Expected %s but got %s", expectedLine, listItems[0].Line)
		}

		expectedLine = "Third"
		if listItems[idx].Line != expectedLine {
			t.Errorf("Expected %s but got %s", expectedLine, listItems[idx].Line)
		}

		err = os.Remove(rootPath)
		if err != nil {
			log.Fatal(err)
		}
	})
}

func TestServiceUpdate(t *testing.T) {
	rootPath := "file_to_delete"
	mockListRepo := NewDBListRepo(rootPath)

	t.Run("Update item in list", func(t *testing.T) {
		listItems := []ListItem{
			{"First"},
			{"Second"},
			{"Third"},
		}

		err := mockListRepo.Save(listItems)
		if err != nil {
			t.Fatal(err)
		}

		idx := 1
		expectedLine := "Oooo I'm new"
		err = mockListRepo.Update(expectedLine, idx, &listItems)
		if err != nil {
			t.Fatal(err)
		}

		expectedLen := 3
		if len(listItems) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(listItems))
		}

		if listItems[idx].Line != expectedLine {
			t.Errorf("Expected %s but got %s", expectedLine, listItems[idx].Line)
		}

		err = os.Remove(rootPath)
		if err != nil {
			log.Fatal(err)
		}
	})
}

func TestServiceMatch(t *testing.T) {
	rootPath := "file_to_delete"
	mockListRepo := NewDBListRepo(rootPath)

	t.Run("Update item in list", func(t *testing.T) {
		listItems := []ListItem{
			{"First"},
			{"Second"},
			{"Third"},
		}

		err := mockListRepo.Save(listItems)
		if err != nil {
			t.Fatal(err)
		}

		search := [][]rune{
			[]rune{'s', 'e', 'c', 'o', 'n', 'd'},
		}
		listItems, err = mockListRepo.Match(search, &listItems)
		if err != nil {
			t.Fatal(err)
		}

		expectedLen := 1
		if len(listItems) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(listItems))
		}

		expectedLine := "Second"
		if listItems[0].Line != expectedLine {
			t.Errorf("Expected line %s but got %s", expectedLine, listItems[0].Line)
		}

		err = os.Remove(rootPath)
		if err != nil {
			log.Fatal(err)
		}
	})
}
