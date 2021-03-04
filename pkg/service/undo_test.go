package service

import (
	"os"
	//"runtime"
	"testing"
)

func TestTransactionUndo(t *testing.T) {
	t.Run("Undo on empty db", func(t *testing.T) {
		repo := NewDBListRepo(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		err := repo.Undo()
		if err != nil {
			t.Fatal(err)
		}

		if len(repo.eventLogger.log) != 1 {
			t.Errorf("Event log should instantiate with a null event log at idx zero")
		}

		repo.Match([][]rune{}, true)
		matches := repo.matchListItems

		if len(matches) != 0 {
			t.Errorf("Undo should have done nothing")
		}
	})
	t.Run("Undo single item Add", func(t *testing.T) {
		repo := NewDBListRepo(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		line := "New item"
		repo.Add(line, nil, 0)

		if len(repo.eventLogger.log) != 2 {
			t.Errorf("Event log should have one null and one real event in it")
		}

		repo.Match([][]rune{}, true)
		matches := repo.matchListItems

		logItem := repo.eventLogger.log[1]
		if logItem.eventType != addEvent {
			t.Errorf("Event log entry should be of type AddEvent")
		}
		if logItem.listItemID != matches[0].id {
			t.Errorf("Event log list item should have the same id")
		}
		if (repo.eventLogger.curIdx) != 1 {
			t.Errorf("The event logger index should increment to 1")
		}

		err := repo.Undo()
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true)
		matches = repo.matchListItems

		if len(matches) != 0 {
			t.Errorf("Undo should have removed the only item")
		}
		if repo.Root != nil {
			t.Errorf("The root should have been cleared")
		}

		if len(repo.eventLogger.log) != 2 {
			t.Errorf("Event logger should persist the log")
		}
		if (repo.eventLogger.curIdx) != 0 {
			t.Errorf("The event logger index should decrement back to 0")
		}
	})
	t.Run("Undo single item Add and Update", func(t *testing.T) {
		repo := NewDBListRepo(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		line := "New item"
		repo.Add(line, nil, 0)

		updatedLine := "Updated item"
		repo.Match([][]rune{}, true)
		matches := repo.matchListItems
		repo.Update(updatedLine, &[]byte{}, 0)

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should have one null and two real events in it")
		}
		if repo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should be at position two")
		}

		newestLogItem := repo.eventLogger.log[2]
		if newestLogItem.eventType != updateEvent {
			t.Errorf("Newest event log entry should be of type UpdateEvent")
		}
		if newestLogItem.undoLine != line {
			t.Errorf("Newest event log list item should have the original line")
		}

		oldestLogItem := repo.eventLogger.log[1]
		if oldestLogItem.eventType != addEvent {
			t.Errorf("Oldest event log entry should be of type AddEvent")
		}

		repo.Match([][]rune{}, true)
		matches = repo.matchListItems
		if matches[0].Line != updatedLine {
			t.Errorf("List item should have the updated line")
		}

		err := repo.Undo()
		if err != nil {
			t.Fatal(err)
		}

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should still have three events in it")
		}
		if repo.eventLogger.curIdx != 1 {
			t.Errorf("Event logger should have decremented to one")
		}

		repo.Match([][]rune{}, true)
		matches = repo.matchListItems
		if len(matches) != 1 {
			t.Errorf("Undo should have updated the item, not deleted it")
		}
		if matches[0].Line != line {
			t.Errorf("List item should now have the original line")
		}

		err = repo.Undo()
		if err != nil {
			t.Fatal(err)
		}

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should still have three events in it")
		}
		if repo.eventLogger.curIdx != 0 {
			t.Errorf("Event logger should have decremented to zero")
		}

		repo.Match([][]rune{}, true)
		matches = repo.matchListItems
		if len(matches) != 0 {
			t.Errorf("Second undo should have deleted the item")
		}
		if repo.Root != nil {
			t.Errorf("Undo should have removed the root")
		}
	})
	t.Run("Add twice, Delete twice, Undo twice, Redo once", func(t *testing.T) {
		repo := NewDBListRepo(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		line := "New item"
		repo.Add(line, nil, 0)

		if len(repo.eventLogger.log) != 2 {
			t.Errorf("Event log should have one null and one real event in it")
		}
		if repo.eventLogger.curIdx != 1 {
			t.Errorf("Event logger should have incremented to one")
		}

		logItem := repo.eventLogger.log[1]
		if logItem.eventType != addEvent {
			t.Errorf("Event log entry should be of type AddEvent")
		}
		if logItem.undoLine != line {
			t.Errorf("Event log list item should have the original line")
		}

		repo.Match([][]rune{}, true)
		matches := repo.matchListItems
		listItem := matches[0]
		if logItem.listItemID != listItem.id {
			t.Errorf("The listItem ptr should be consistent with the original")
		}

		line2 := "Another item"
		repo.Add(line2, nil, 1)
		repo.Match([][]rune{}, true)
		matches = repo.matchListItems
		idx := 1
		listItem2 := matches[idx]

		if repo.eventLogger.log[1] != logItem {
			t.Errorf("Original log item should still be in the first position in the log")
		}

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should have one null and two real events in it")
		}
		if repo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should have incremented to two")
		}

		logItem2 := repo.eventLogger.log[2]
		if logItem2.eventType != addEvent {
			t.Errorf("Event log entry should be of type AddEvent")
		}
		if logItem2.undoLine != line2 {
			t.Errorf("Event log list item should have the new line")
		}

		if logItem2.listItemID != listItem2.id {
			t.Errorf("The listItem ptr should be consistent with the original")
		}

		repo.Delete(idx)

		if len(repo.eventLogger.log) != 4 {
			t.Errorf("Event log should have one null and three real events in it")
		}
		if repo.eventLogger.curIdx != 3 {
			t.Errorf("Event logger should have incremented to three")
		}

		logItem3 := repo.eventLogger.log[3]
		if logItem3.eventType != deleteEvent {
			t.Errorf("Event log entry should be of type DeleteEvent")
		}
		if logItem3.undoLine != line2 {
			t.Errorf("Event log list item should have the original line")
		}

		if logItem3.listItemID != listItem2.id {
			t.Errorf("The listItem ptr should be consistent with the original")
		}

		repo.Delete(0)
		repo.Match([][]rune{}, true)
		matches = repo.matchListItems

		if len(repo.eventLogger.log) != 5 {
			t.Errorf("Event log should have one null and four real events in it")
		}
		if repo.eventLogger.curIdx != 4 {
			t.Errorf("Event logger should have incremented to four")
		}

		logItem4 := repo.eventLogger.log[4]
		if logItem4.eventType != deleteEvent {
			t.Errorf("Event log entry should be of type DeleteEvent")
		}
		if logItem4.undoLine != line {
			t.Errorf("Event log list item should have the original line")
		}

		if logItem4.listItemID != listItem.id {
			t.Errorf("The listItem ptr should be consistent with the original")
		}

		err := repo.Undo()
		if err != nil {
			t.Fatal(err)
		}

		if len(repo.eventLogger.log) != 5 {
			t.Errorf("Event log should still have five events in it")
		}
		if repo.eventLogger.curIdx != 3 {
			t.Errorf("Event logger should have decremented to three")
		}

		repo.Match([][]rune{}, true)
		matches = repo.matchListItems
		if len(matches) != 1 {
			t.Errorf("Undo should have added the original item back in")
		}
		if matches[0].Line != line {
			t.Errorf("List item should now have the original line")
		}

		err = repo.Undo()
		if err != nil {
			t.Fatal(err)
		}

		if len(repo.eventLogger.log) != 5 {
			t.Errorf("Event log should still have five events in it")
		}
		if repo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should have decremented to two")
		}

		repo.Match([][]rune{}, true)
		matches = repo.matchListItems
		if len(matches) != 2 {
			t.Errorf("Undo should have added the second original item back in")
		}
		//runtime.Breakpoint()
		if matches[1].Line != line2 {
			t.Errorf("List item should now have the original line")
		}

		err = repo.Redo()
		if err != nil {
			t.Fatal(err)
		}

		if len(repo.eventLogger.log) != 5 {
			t.Errorf("Event log should still have five events in it")
		}
		if repo.eventLogger.curIdx != 3 {
			t.Errorf("Event logger should have incremented to three")
		}

		repo.Match([][]rune{}, true)
		matches = repo.matchListItems
		if len(matches) != 1 {
			t.Errorf("Undo should have removed the second original item again")
		}
		if matches[0].Line != line {
			t.Errorf("List item should now have the original line")
		}
	})
	t.Run("Add empty item, update with character, Undo, Redo", func(t *testing.T) {
		repo := NewDBListRepo(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		repo.Add("", nil, 0)

		logItem := repo.eventLogger.log[1]
		if logItem.eventType != addEvent {
			t.Errorf("Event log entry should be of type AddEvent")
		}

		repo.Match([][]rune{}, true)
		matches := repo.matchListItems

		newLine := "a"
		repo.Update(newLine, &[]byte{}, 0)

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should have one null and two real events in it")
		}
		if repo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should have incremented to two")
		}
		logItem2 := repo.eventLogger.log[2]
		if logItem2.eventType != updateEvent {
			t.Errorf("Event log entry should be of type UpdateEvent")
		}

		repo.Match([][]rune{}, true)
		matches = repo.matchListItems
		if matches[0].Line != newLine {
			t.Errorf("List item should now have the new line")
		}

		err := repo.Undo()
		if err != nil {
			t.Fatal(err)
		}

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should still have three events in it")
		}
		if repo.eventLogger.curIdx != 1 {
			t.Errorf("Event logger should have decremented to one")
		}

		repo.Match([][]rune{}, true)
		matches = repo.matchListItems
		if matches[0].Line != "" {
			t.Errorf("Undo should have removed the line")
		}
		//fmt.Println(repo.eventLogger.curIdx)

		err = repo.Redo()
		if err != nil {
			t.Fatal(err)
		}

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should still have two events in it")
		}
		if repo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should have returned to the head at two")
		}

		// TODO problem is, looking ahead to next log item for `Redo` redoes the old PRE state
		// Idea: store old and new state in the log item lines, Undo sets to old, Redo sets to new
		repo.Match([][]rune{}, true)
		matches = repo.matchListItems
		if matches[0].Line != newLine {
			t.Errorf("Redo should have added the line back in")
		}
	})
	t.Run("Add line, Delete line, Undo, delete character, Undo", func(t *testing.T) {
		repo := NewDBListRepo(rootDir)
		os.Mkdir(rootDir, os.ModePerm)
		defer clearUp(repo)

		originalLine := "Original line"
		repo.Add(originalLine, nil, 0)

		if len(repo.eventLogger.log) != 2 {
			t.Errorf("Event log should have a nullEvent and addEvent in it")
		}
		if repo.eventLogger.curIdx != 1 {
			t.Errorf("Event logger should be set to one")
		}

		repo.Match([][]rune{}, true)

		repo.Delete(0)

		repo.Match([][]rune{}, true)
		matches := repo.matchListItems
		if len(matches) != 0 {
			t.Errorf("Item should have been deleted")
		}

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should have the nullEvent, addEvent and one deleteEvent")
		}
		if repo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should have incremented to one")
		}

		err := repo.Undo()
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true)
		matches = repo.matchListItems
		if len(matches) != 1 {
			t.Errorf("Item should have been added back in")
		}

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should be unchanged")
		}
		if repo.eventLogger.curIdx != 1 {
			t.Errorf("Event logger should have decremented to one")
		}

		newLine := "Updated line"
		repo.Update(newLine, &[]byte{}, 0)

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should have the nullEvent, addEvent and overriding updateEvent")
		}
		if repo.eventLogger.log[2].eventType != updateEvent {
			t.Errorf("Event logger item should be of type updateEvent")
		}
		if repo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should have incremented to 2")
		}

		err = repo.Undo()
		if err != nil {
			t.Fatal(err)
		}

		repo.Match([][]rune{}, true)
		matches = repo.matchListItems
		if len(matches) != 1 {
			t.Errorf("There should still be one match")
		}

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should be unchanged")
		}
		if repo.eventLogger.curIdx != 1 {
			t.Errorf("Event logger should have decremented to 1")
		}

		if matches[0].Line != originalLine {
			t.Errorf("The line should have reverted back to the original")
		}
	})
}
