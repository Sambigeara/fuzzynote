package service

import (
	"fmt"
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

		err := mockListRepo.Load()
		if err != nil {
			t.Fatal(err)
		}

		if mockListRepo.Root.Line != expectedLines[0] {
			t.Errorf("Expected %s but got %s", expectedLines[0], mockListRepo.Root.Line)
		}

		if mockListRepo.Root.Parent.Line != expectedLines[1] {
			t.Errorf("Expected %s but got %s", expectedLines[1], mockListRepo.Root.Line)
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

		mockListRepo.Load()

		if mockListRepo.Root.Line != newItem.Line {
			t.Errorf("Expected %s but got %s", newItem.Line, mockListRepo.Root.Line)
		}

		if mockListRepo.Root.Parent.Line != item3.Line {
			t.Errorf("Expected %s but got %s", mockListRepo.Root.Parent.Line, item3.Line)
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

	item2 := ListItem{
		Line: "Old existing created line",
	}
	item1 := ListItem{
		Line:   "New existing created line",
		Parent: &item2,
	}
	item2.Child = &item1
	err := mockListRepo.Save(&item1)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("Add item at head of list", func(t *testing.T) {
		newLine := "Now I'm first"
		newItem, err := mockListRepo.Add(newLine, &item1, true)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil)

		expectedLen := 3
		if len(matches) != expectedLen {
			t.Errorf("Expected len %d but got %d", expectedLen, len(matches))
		}

		if newItem != item1.Child {
			t.Errorf("New item should be original root's Child")
		}

		if mockListRepo.GetRoot() != matches[0] {
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

		matches, _ := mockListRepo.Match([][]rune{}, nil)
		oldLen := len(matches)
		fmt.Printf("HELLOOOO %v\n", matches)

		newItem, err := mockListRepo.Add(newLine, &item2, false)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ = mockListRepo.Match([][]rune{}, nil)

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

		_, err := mockListRepo.Add(newLine, &item1, false)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil)

		expectedIdx := 2
		if matches[expectedIdx].Line != newLine {
			t.Errorf("Expected %s but got %s", newLine, matches[expectedIdx].Line)
		}

		if item1.Parent != matches[expectedIdx] {
			t.Errorf("Original youngest listItem has incorrect Parent")
		}

		if oldParent.Child != matches[expectedIdx] {
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

		err := mockListRepo.Delete(&item1)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil)

		if matches[0] != &item2 {
			t.Errorf("item2 should be new root")
		}

		if mockListRepo.GetRoot() != &item2 {
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
		mockListRepo.Save(&item1)

		err := mockListRepo.Delete(&item3)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil)

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
		mockListRepo.Save(&item1)

		err := mockListRepo.Delete(&item2)
		if err != nil {
			t.Fatal(err)
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil)

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

		matches, _ := mockListRepo.Match([][]rune{}, nil)

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
	})
}

func TestServiceMatch(t *testing.T) {
	rootPath := "file_to_delete"
	mockListRepo := NewDBListRepo(rootPath)

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
		mockListRepo.Save(&item1)

		search := [][]rune{
			[]rune{'s', 'e', 'c', 'o', 'n', 'd'},
		}
		matches, err := mockListRepo.Match(search, nil)
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
		mockListRepo.Save(&item1)

		search := [][]rune{
			[]rune{'s', 'e', 'c', 'o', 'n', 'd'},
		}
		matches, err := mockListRepo.Match(search, &item3)
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
