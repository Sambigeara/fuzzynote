package service

import (
	"log"
	"os"
	"testing"
)

func TestServiceStoreLoad(t *testing.T) {
	t.Run("Loads from file", func(t *testing.T) {
		rootPath := "tests/test_file"
		mockListRepo := NewDBListRepo(rootPath)

		expectedLines := make([]string, 2)
		expectedLines[0] = "Test ListItem"
		expectedLines[1] = "Another test ListItem"

		listItem, err := mockListRepo.Load()
		if err != nil {
			t.Fatal(err)
		}

		if listItem.Line != expectedLines[0] {
			t.Errorf("Expected %s but got %s", expectedLines[0], listItem.Line)
		}

		if (*listItem.Parent).Line != expectedLines[1] {
			t.Errorf("Expected %s but got %s", expectedLines[1], listItem.Line)
		}
	})
	t.Run("Stores to new file and loads back", func(t *testing.T) {
		rootPath := "file_to_delete"
		mockListRepo := NewDBListRepo(rootPath)

		item3 := ListItem{
			Line: "Old newly created line",
		}
		newItem := ListItem{
			Line:   "New newly created line",
			Parent: &item3,
		}
		item3.Child = &newItem

		err := mockListRepo.Save(&newItem)
		if err != nil {
			t.Fatal(err)
		}

		retLinkItem, _ := mockListRepo.Load()

		if retLinkItem.Line != newItem.Line {
			t.Errorf("Expected %s but got %s", newItem.Line, retLinkItem.Line)
		}

		if (*retLinkItem.Parent).Line != item3.Line {
			t.Errorf("Expected %s but got %s", (*retLinkItem.Parent).Line, item3.Line)
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

	item3 := ListItem{
		Line: "Old existing created line",
	}
	item2 := ListItem{
		Line:   "New existing created line",
		Parent: &item3,
	}
	item3.Child = &item2
	err := mockListRepo.Save(&item2)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Add item at head of list", func(t *testing.T) {
		newLine := "Now I'm first"
		newItem, err := mockListRepo.Add(newLine, nil, &item2)
		if err != nil {
			t.Fatal(err)
		}

		if (*newItem).Line != newLine {
			t.Errorf("Expected %s but got %s", newLine, (*newItem).Line)
		}

		if (*newItem).Child != nil {
			t.Errorf("Newly generated listItem should have a nil Child")
		}

		if (*newItem).Parent != &item2 {
			t.Errorf("Newly generated listItem has incorrect Parent")
		}

		if item2.Child != newItem {
			t.Errorf("Original young listItem has incorrect Child")
		}
	})

	t.Run("Add item at end of list", func(t *testing.T) {
		newLine := "I should be last"

		newItem, err := mockListRepo.Add(newLine, &item3, nil)
		if err != nil {
			t.Fatal(err)
		}

		if (*newItem).Line != newLine {
			t.Errorf("Expected %s but got %s", newLine, (*newItem).Line)
		}

		if (*newItem).Parent != nil {
			t.Errorf("Newly generated listItem should have a nil Parent")
		}

		if (*newItem).Child != &item3 {
			t.Errorf("Newly generated listItem has incorrect Child")
		}

		if item3.Parent != newItem {
			t.Errorf("Original youngest listItem has incorrect Parent")
		}
	})

	t.Run("Add item in middle of list", func(t *testing.T) {
		newLine := "I'm somewhere in the middle"

		newItem, err := mockListRepo.Add(newLine, &item2, nil)
		if err != nil {
			t.Fatal(err)
		}

		if (*newItem).Line != newLine {
			t.Errorf("Expected %s but got %s", newLine, (*newItem).Line)
		}

		if item3.Child != newItem {
			t.Errorf("Original oldest listItem has incorrect Child")
		}

		if item2.Parent != newItem {
			t.Errorf("Original youngest listItem has incorrect Parent")
		}
	})

	err = os.Remove(rootPath)
	if err != nil {
		log.Fatal(err)
	}
}

func lenListItems(cur *ListItem) int {
	cnt := 0
	for {
		cnt++
		if cur.Parent == nil {
			return cnt
		}
		cur = cur.Parent
	}
}

func TestServiceDelete(t *testing.T) {
	rootPath := "file_to_delete"
	mockListRepo := NewDBListRepo(rootPath)

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
		mockListRepo.Save(&item1)

		root, err := mockListRepo.Delete(&item1)
		if err != nil {
			t.Fatal(err)
		}

		if root != &item2 {
			t.Errorf("item2 should be new root")
		}

		expectedLen := 2
		if lenListItems(root) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, lenListItems(root))
		}

		expectedLine := "Second"
		if root.Line != expectedLine {
			t.Errorf("Expected %s but got %s", expectedLine, root.Line)
		}

		if item2.Child != nil {
			t.Errorf("New root item should have no child")
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
		mockListRepo.Save(&item1)

		root, err := mockListRepo.Delete(&item3)
		if err != nil {
			t.Fatal(err)
		}

		if root != &item1 {
			t.Errorf("item1 should have remained the root")
		}

		expectedLen := 2
		if lenListItems(root) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, lenListItems(root))
		}

		expectedLine := "First"
		if root.Line != expectedLine {
			t.Errorf("Expected %s but got %s", expectedLine, root.Line)
		}

		expectedLine = "Second"
		if root.Parent.Line != expectedLine {
			t.Errorf("Expected %s but got %s", expectedLine, root.Parent.Line)
		}

		if item2.Parent != nil {
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
		mockListRepo.Save(&item1)

		root, err := mockListRepo.Delete(&item2)
		if err != nil {
			t.Fatal(err)
		}

		if root != &item1 {
			t.Errorf("item1 should have remained the root")
		}

		expectedLen := 2
		if lenListItems(root) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, lenListItems(root))
		}

		expectedLine := "First"
		if root.Line != expectedLine {
			t.Errorf("Expected %s but got %s", expectedLine, root.Line)
		}

		expectedLine = "Third"
		if root.Parent.Line != expectedLine {
			t.Errorf("Expected %s but got %s", expectedLine, root.Parent.Line)
		}

		if item1.Parent != &item3 {
			t.Errorf("First item parent should be third item")
		}

		if item3.Child != &item1 {
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
	mockListRepo := NewDBListRepo(rootPath)

	t.Run("Update item in list", func(t *testing.T) {
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
		mockListRepo.Save(&item1)

		expectedLine := "Oooo I'm new"
		err := mockListRepo.Update(expectedLine, &item2)
		if err != nil {
			t.Fatal(err)
		}

		expectedLen := 3
		if lenListItems(&item1) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, lenListItems(&item1))
		}

		if item2.Line != expectedLine {
			t.Errorf("Expected %s but got %s", expectedLine, item2.Line)
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
		mockListRepo.Save(&item1)

		search := [][]rune{
			[]rune{'s', 'e', 'c', 'o', 'n', 'd'},
		}
		matchItem, err := mockListRepo.Match(search, &item1)
		if err != nil {
			t.Fatal(err)
		}

		expectedLen := 1
		if lenListItems(matchItem) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, lenListItems(matchItem))
		}

		expectedLine := "Second"
		if matchItem.Line != expectedLine {
			t.Errorf("Expected line %s but got %s", expectedLine, matchItem.Line)
		}

		if matchItem.Child != nil {
			t.Errorf("Returned item should have no child")
		}

		if matchItem.Parent != nil {
			t.Errorf("Returned item should have no parent")
		}

		err = os.Remove(rootPath)
		if err != nil {
			log.Fatal(err)
		}
	})
}
