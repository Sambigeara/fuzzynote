package service

import (
	"testing"
)

func TestTransactionUndo(t *testing.T) {
	t.Run("Undo on empty db", func(t *testing.T) {
		eventLogger := NewDbEventLogger()
		mockListRepo := NewDBListRepo(nil, 1, eventLogger)

		err := mockListRepo.Undo()
		if err != nil {
			t.Fatal(err)
		}

		if len(mockListRepo.eventLogger.log) != 1 {
			t.Errorf("Event log should instantiate with a null event log at idx zero")
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)

		if len(matches) != 0 {
			t.Errorf("Undo should have done nothing")
		}
	})
	t.Run("Undo single item Add", func(t *testing.T) {
		eventLogger := NewDbEventLogger()
		mockListRepo := NewDBListRepo(nil, 1, eventLogger)

		line := "New item"
		mockListRepo.Add(line, nil, nil, nil)

		if len(mockListRepo.eventLogger.log) != 2 {
			t.Errorf("Event log should have one null and one real event in it")
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)

		logItem := mockListRepo.eventLogger.log[1]
		if logItem.eventType != addEvent {
			t.Errorf("Event log entry should be of type AddEvent")
		}
		if logItem.ptr.id != matches[0].id {
			t.Errorf("Event log list item should have the same id")
		}
		if (mockListRepo.eventLogger.curIdx) != 1 {
			t.Errorf("The event logger index should increment to 1")
		}

		err := mockListRepo.Undo()
		if err != nil {
			t.Fatal(err)
		}

		matches, _ = mockListRepo.Match([][]rune{}, nil, true)

		if len(matches) != 0 {
			t.Errorf("Undo should have removed the only item")
		}
		if mockListRepo.Root != nil {
			t.Errorf("The root should have been cleared")
		}

		if len(mockListRepo.eventLogger.log) != 2 {
			t.Errorf("Event logger should persist the log")
		}
		if (mockListRepo.eventLogger.curIdx) != 0 {
			t.Errorf("The event logger index should decrement back to 0")
		}
	})
	t.Run("Undo single item Add and Update", func(t *testing.T) {
		eventLogger := NewDbEventLogger()
		mockListRepo := NewDBListRepo(nil, 1, eventLogger)

		line := "New item"
		mockListRepo.Add(line, nil, nil, nil)

		updatedLine := "Updated item"
		matches, _ := mockListRepo.Match([][]rune{}, nil, true)
		mockListRepo.Update(updatedLine, &[]byte{}, matches[0])

		if len(mockListRepo.eventLogger.log) != 3 {
			t.Errorf("Event log should have one null and two real events in it")
		}
		if mockListRepo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should be at position two")
		}

		newestLogItem := mockListRepo.eventLogger.log[2]
		if newestLogItem.eventType != updateEvent {
			t.Errorf("Newest event log entry should be of type UpdateEvent")
		}
		if newestLogItem.undoLine != line {
			t.Errorf("Newest event log list item should have the original line")
		}

		oldestLogItem := mockListRepo.eventLogger.log[1]
		if oldestLogItem.eventType != addEvent {
			t.Errorf("Oldest event log entry should be of type AddEvent")
		}

		matches, _ = mockListRepo.Match([][]rune{}, nil, true)
		if matches[0].Line != updatedLine {
			t.Errorf("List item should have the updated line")
		}

		err := mockListRepo.Undo()
		if err != nil {
			t.Fatal(err)
		}

		if len(mockListRepo.eventLogger.log) != 3 {
			t.Errorf("Event log should still have three events in it")
		}
		if mockListRepo.eventLogger.curIdx != 1 {
			t.Errorf("Event logger should have decremented to one")
		}

		matches, _ = mockListRepo.Match([][]rune{}, nil, true)
		if len(matches) != 1 {
			t.Errorf("Undo should have updated the item, not deleted it")
		}
		if matches[0].Line != line {
			t.Errorf("List item should now have the original line")
		}

		err = mockListRepo.Undo()
		if err != nil {
			t.Fatal(err)
		}

		if len(mockListRepo.eventLogger.log) != 3 {
			t.Errorf("Event log should still have three events in it")
		}
		if mockListRepo.eventLogger.curIdx != 0 {
			t.Errorf("Event logger should have decremented to zero")
		}

		matches, _ = mockListRepo.Match([][]rune{}, nil, true)
		if len(matches) != 0 {
			t.Errorf("Second undo should have deleted the item")
		}
		if mockListRepo.Root != nil {
			t.Errorf("Undo should have removed the root")
		}
	})
	t.Run("Add twice, Delete twice, Undo twice, Redo once", func(t *testing.T) {
		eventLogger := NewDbEventLogger()
		mockListRepo := NewDBListRepo(nil, 1, eventLogger)

		line := "New item"
		mockListRepo.Add(line, nil, nil, nil)

		if len(mockListRepo.eventLogger.log) != 2 {
			t.Errorf("Event log should have one null and one real event in it")
		}
		if mockListRepo.eventLogger.curIdx != 1 {
			t.Errorf("Event logger should have incremented to one")
		}

		logItem := mockListRepo.eventLogger.log[1]
		if logItem.eventType != addEvent {
			t.Errorf("Event log entry should be of type AddEvent")
		}
		if logItem.undoLine != line {
			t.Errorf("Event log list item should have the original line")
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)
		listItem := matches[0]
		if logItem.ptr != listItem {
			t.Errorf("The listItem ptr should be consistent with the original")
		}

		line2 := "Another item"
		mockListRepo.Add(line2, nil, listItem, nil)
		matches, _ = mockListRepo.Match([][]rune{}, nil, true)
		listItem2 := matches[1]

		if mockListRepo.eventLogger.log[1] != logItem {
			t.Errorf("Original log item should still be in the first position in the log")
		}

		if len(mockListRepo.eventLogger.log) != 3 {
			t.Errorf("Event log should have one null and two real events in it")
		}
		if mockListRepo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should have incremented to two")
		}

		logItem2 := mockListRepo.eventLogger.log[2]
		if logItem2.eventType != addEvent {
			t.Errorf("Event log entry should be of type AddEvent")
		}
		if logItem2.undoLine != line2 {
			t.Errorf("Event log list item should have the new line")
		}

		if logItem2.ptr != listItem2 {
			t.Errorf("The listItem ptr should be consistent with the original")
		}

		mockListRepo.Delete(listItem2)

		if len(mockListRepo.eventLogger.log) != 4 {
			t.Errorf("Event log should have one null and three real events in it")
		}
		if mockListRepo.eventLogger.curIdx != 3 {
			t.Errorf("Event logger should have incremented to three")
		}

		logItem3 := mockListRepo.eventLogger.log[3]
		if logItem3.eventType != deleteEvent {
			t.Errorf("Event log entry should be of type DeleteEvent")
		}
		if logItem3.undoLine != line2 {
			t.Errorf("Event log list item should have the original line")
		}

		if logItem3.ptr != listItem2 {
			t.Errorf("The listItem ptr should be consistent with the original")
		}

		mockListRepo.Delete(matches[0])
		matches, _ = mockListRepo.Match([][]rune{}, nil, true)

		if len(mockListRepo.eventLogger.log) != 5 {
			t.Errorf("Event log should have one null and four real events in it")
		}
		if mockListRepo.eventLogger.curIdx != 4 {
			t.Errorf("Event logger should have incremented to four")
		}

		logItem4 := mockListRepo.eventLogger.log[4]
		if logItem4.eventType != deleteEvent {
			t.Errorf("Event log entry should be of type DeleteEvent")
		}
		if logItem4.undoLine != line {
			t.Errorf("Event log list item should have the original line")
		}

		if logItem4.ptr != listItem {
			t.Errorf("The listItem ptr should be consistent with the original")
		}

		err := mockListRepo.Undo()
		if err != nil {
			t.Fatal(err)
		}

		if len(mockListRepo.eventLogger.log) != 5 {
			t.Errorf("Event log should still have five events in it")
		}
		if mockListRepo.eventLogger.curIdx != 3 {
			t.Errorf("Event logger should have decremented to three")
		}

		matches, _ = mockListRepo.Match([][]rune{}, nil, true)
		if len(matches) != 1 {
			t.Errorf("Undo should have added the original item back in")
		}
		if matches[0].Line != line {
			t.Errorf("List item should now have the original line")
		}

		err = mockListRepo.Undo()
		if err != nil {
			t.Fatal(err)
		}

		if len(mockListRepo.eventLogger.log) != 5 {
			t.Errorf("Event log should still have five events in it")
		}
		if mockListRepo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should have decremented to two")
		}

		matches, _ = mockListRepo.Match([][]rune{}, nil, true)
		if len(matches) != 2 {
			t.Errorf("Undo should have added the second original item back in")
		}
		if matches[1].Line != line2 {
			t.Errorf("List item should now have the original line")
		}

		err = mockListRepo.Redo()
		if err != nil {
			t.Fatal(err)
		}

		if len(mockListRepo.eventLogger.log) != 5 {
			t.Errorf("Event log should still have five events in it")
		}
		if mockListRepo.eventLogger.curIdx != 3 {
			t.Errorf("Event logger should have incremented to three")
		}

		matches, _ = mockListRepo.Match([][]rune{}, nil, true)
		if len(matches) != 1 {
			t.Errorf("Undo should have removed the second original item again")
		}
		if matches[0].Line != line {
			t.Errorf("List item should now have the original line")
		}
	})
	t.Run("Add empty item, update with character, Undo, Redo", func(t *testing.T) {
		eventLogger := NewDbEventLogger()
		mockListRepo := NewDBListRepo(nil, 1, eventLogger)

		mockListRepo.Add("", nil, nil, nil)

		logItem := mockListRepo.eventLogger.log[1]
		if logItem.eventType != addEvent {
			t.Errorf("Event log entry should be of type AddEvent")
		}

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)

		newLine := "a"
		mockListRepo.Update(newLine, &[]byte{}, matches[0])

		if len(mockListRepo.eventLogger.log) != 3 {
			t.Errorf("Event log should have one null and two real events in it")
		}
		if mockListRepo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should have incremented to two")
		}
		logItem2 := mockListRepo.eventLogger.log[2]
		if logItem2.eventType != updateEvent {
			t.Errorf("Event log entry should be of type UpdateEvent")
		}

		matches, _ = mockListRepo.Match([][]rune{}, nil, true)
		if matches[0].Line != newLine {
			t.Errorf("List item should now have the new line")
		}

		err := mockListRepo.Undo()
		if err != nil {
			t.Fatal(err)
		}

		if len(mockListRepo.eventLogger.log) != 3 {
			t.Errorf("Event log should still have three events in it")
		}
		if mockListRepo.eventLogger.curIdx != 1 {
			t.Errorf("Event logger should have decremented to one")
		}

		matches, _ = mockListRepo.Match([][]rune{}, nil, true)
		if matches[0].Line != "" {
			t.Errorf("Undo should have removed the line")
		}
		//fmt.Println(mockListRepo.eventLogger.curIdx)

		err = mockListRepo.Redo()
		if err != nil {
			t.Fatal(err)
		}

		if len(mockListRepo.eventLogger.log) != 3 {
			t.Errorf("Event log should still have two events in it")
		}
		if mockListRepo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should have returned to the head at two")
		}

		// TODO problem is, looking ahead to next log item for `Redo` redoes the old PRE state
		// Idea: store old and new state in the log item lines, Undo sets to old, Redo sets to new
		matches, _ = mockListRepo.Match([][]rune{}, nil, true)
		if matches[0].Line != newLine {
			t.Errorf("Redo should have added the line back in")
		}
	})
	t.Run("Delete line, Undo, delete character, Undo", func(t *testing.T) {
		originalLine := "Original line"
		item1 := ListItem{
			Line: originalLine,
			id:   1,
		}
		mockListRepo := NewDBListRepo(&item1, 2, NewDbEventLogger())

		if len(mockListRepo.eventLogger.log) != 1 {
			t.Errorf("Event log should have only the nullEvent in it")
		}
		if mockListRepo.eventLogger.curIdx != 0 {
			t.Errorf("Event logger should be set to zero")
		}

		mockListRepo.Delete(&item1)

		matches, _ := mockListRepo.Match([][]rune{}, nil, true)
		if len(matches) != 0 {
			t.Errorf("Item should have been deleted")
		}

		if len(mockListRepo.eventLogger.log) != 2 {
			t.Errorf("Event log should have the nullEvent and one Delete event in it")
		}
		if mockListRepo.eventLogger.curIdx != 1 {
			t.Errorf("Event logger should have incremented to one")
		}

		err := mockListRepo.Undo()
		if err != nil {
			t.Fatal(err)
		}

		matches, _ = mockListRepo.Match([][]rune{}, nil, true)
		if len(matches) != 1 {
			t.Errorf("Item should have been added back in")
		}

		if len(mockListRepo.eventLogger.log) != 2 {
			t.Errorf("Event log should still have one Delete event in it")
		}
		if mockListRepo.eventLogger.curIdx != 0 {
			t.Errorf("Event logger should have decremented to zero")
		}

		newLine := "Updated line"
		mockListRepo.Update(newLine, &[]byte{}, matches[0])

		if len(mockListRepo.eventLogger.log) != 2 {
			t.Errorf("Event log should have been overidden with the nullEvent and the single Update event now")
		}
		if mockListRepo.eventLogger.log[1].eventType != updateEvent {
			t.Errorf("Event logger item should be of type updateEvent")
		}
		if mockListRepo.eventLogger.curIdx != 1 {
			t.Errorf("Event logger should have incremented to one")
		}

		err = mockListRepo.Undo()
		if err != nil {
			t.Fatal(err)
		}

		matches, _ = mockListRepo.Match([][]rune{}, nil, true)
		if len(matches) != 1 {
			t.Errorf("There should still be one match")
		}

		if len(mockListRepo.eventLogger.log) != 2 {
			t.Errorf("Event log should still have one Insert event in it")
		}
		if mockListRepo.eventLogger.curIdx != 0 {
			t.Errorf("Event logger should have incremented to one")
		}

		if matches[0].Line != originalLine {
			t.Errorf("The line should have reverted back to the original")
		}
	})
}
