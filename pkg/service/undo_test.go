package service

import (
	"os"
	"testing"
)

func TestUndoTransaction(t *testing.T) {
	os.Mkdir(rootDir, os.ModePerm)
	t.Run("Undo on empty db", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Undo()

		if len(repo.eventLogger.log) != 1 {
			t.Errorf("Event log should instantiate with a null event log at idx zero")
		}

		if matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0); len(matches) != 0 {
			t.Errorf("Undo should have done nothing")
		}
	})
	t.Run("Undo single item Add", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		line := "New item"
		repo.Add(line, nil, nil)

		if len(repo.eventLogger.log) != 2 {
			t.Errorf("Event log should have one null and one real event in it")
		}

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)

		item := repo.eventLogger.log[1]
		if item.events[0].EventType != UpdateEvent {
			t.Errorf("Event log event entry should be of type UpdateEvent")
		}
		if item.oppEvents[0].EventType != DeleteEvent {
			t.Errorf("Event log oppEvent entry should be of type Delete")
		}

		if item.events[0].ListItemKey != matches[0].Key() {
			t.Errorf("Event log list item should have the same key")
		}
		if item.oppEvents[0].ListItemKey != matches[0].Key() {
			t.Errorf("Event log list item should have the same key")
		}

		if (repo.eventLogger.curIdx) != 1 {
			t.Errorf("The event logger index should increment to 1")
		}

		repo.Undo()

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		if len(matches) != 0 {
			t.Errorf("Undo should have removed the only item")
		}

		if len(repo.eventLogger.log) != 2 {
			t.Errorf("Event logger should persist the log")
		}
		if (repo.eventLogger.curIdx) != 0 {
			t.Errorf("The event logger index should decrement back to 0")
		}
	})
	t.Run("Undo single item Add and Update", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		line := "New item"
		repo.Add(line, nil, nil)

		updatedLine := "Updated item"

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)

		repo.Update(updatedLine, &matches[0])

		if l := len(repo.eventLogger.log); l != 3 {
			t.Errorf("Event log should have one null and two real events in it, but has %d", l)
		}
		if repo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should be at position two")
		}

		newestLogItem := repo.eventLogger.log[2]
		if newestLogItem.events[0].EventType != UpdateEvent {
			t.Errorf("Newest event log event entry should be of type UpdateEvent")
		}
		if newestLogItem.oppEvents[0].EventType != UpdateEvent {
			t.Errorf("Newest event log oppEvent entry should be of type UpdateEvent")
		}

		if newestLogItem.events[0].Line != updatedLine {
			t.Errorf("Newest event log event line should have the updated line")
		}
		if newestLogItem.oppEvents[0].Line != line {
			t.Errorf("Newest event log oppEvent line should have the original line")
		}

		oldestLogItem := repo.eventLogger.log[1]
		if oldestLogItem.events[0].EventType != UpdateEvent {
			t.Errorf("Oldest event log event entry should be of type UpdateEvent")
		}
		if oldestLogItem.oppEvents[0].EventType != DeleteEvent {
			t.Errorf("Oldest event log oppEvent entry should be of type DeleteEvent")
		}

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)
		if matches[0].Line() != updatedLine {
			t.Errorf("List item should have the updated line")
		}

		repo.Undo()

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should still have three events in it")
		}
		if repo.eventLogger.curIdx != 1 {
			t.Errorf("Event logger should have decremented to one")
		}

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)
		if len(matches) != 1 {
			t.Errorf("Undo should have updated the item, not deleted it")
		}
		if matches[0].Line() != line {
			t.Errorf("List item should now have the original line")
		}

		repo.Undo()

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should still have three events in it")
		}
		if repo.eventLogger.curIdx != 0 {
			t.Errorf("Event logger should have decremented to zero")
		}

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)
		if len(matches) != 0 {
			t.Errorf("Second undo should have deleted the item")
		}
	})
	t.Run("Add twice, Delete twice, Undo twice, Redo once", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		line := "New item"
		repo.Add(line, nil, nil)

		if len(repo.eventLogger.log) != 2 {
			t.Errorf("Event log should have one null and one real event in it")
		}
		if repo.eventLogger.curIdx != 1 {
			t.Errorf("Event logger should have incremented to one")
		}

		logItem := &repo.eventLogger.log[1]
		checkFirstLogItemFn := func() string {
			if logItem.events[0].EventType != UpdateEvent {
				return "Event log entry event should be of type UpdateEvent"
			}
			if logItem.oppEvents[0].EventType != DeleteEvent {
				return "Event log entry oppEvent should be of type DeleteEvent"
			}

			if logItem.events[0].Line != line {
				return "Event log list event should have the original line"
			}
			return ""
		}
		if errStr := checkFirstLogItemFn(); errStr != "" {
			t.Error(errStr)
		}

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
		listItem := matches[0]
		if logItem.events[0].ListItemKey != listItem.Key() {
			t.Errorf("The listItem key should be consistent with the original")
		}
		if logItem.oppEvents[0].ListItemKey != listItem.Key() {
			t.Errorf("The listItem key should be consistent with the original")
		}

		line2 := "Another item"
		repo.Add(line2, nil, &listItem)
		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)
		idx := 1
		listItem1 := matches[0]
		listItem2 := matches[idx]

		// Ensure the first logItem remains unchanged
		if errStr := checkFirstLogItemFn(); errStr != "" {
			t.Error("Original log item should still be in the first position in the log")
		}

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should have one null and two real events in it")
		}
		if repo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should have incremented to two")
		}

		logItem2 := repo.eventLogger.log[2]
		if logItem2.events[0].EventType != UpdateEvent {
			t.Errorf("Event log entry event should be of type UpdateEvent")
		}
		if logItem2.oppEvents[0].EventType != DeleteEvent {
			t.Errorf("Event log entry oppEvent should be of type DeleteEvent")
		}
		if logItem2.events[0].Line != line2 {
			t.Errorf("Event log event should have the new line")
		}

		if logItem2.events[0].ListItemKey != listItem2.Key() {
			t.Errorf("The listItem key should be consistent with the original")
		}
		if logItem2.oppEvents[0].ListItemKey != listItem2.Key() {
			t.Errorf("The listItem key should be consistent with the original")
		}

		// need to reference the item in matchListItems
		repo.Delete(repo.matchListItems[listItem2.Key()])

		if len(repo.eventLogger.log) != 4 {
			t.Errorf("Event log should have one null and three real events in it")
		}
		if repo.eventLogger.curIdx != 3 {
			t.Errorf("Event logger should have incremented to three")
		}

		logItem3 := repo.eventLogger.log[3]
		if logItem3.events[0].EventType != DeleteEvent {
			t.Errorf("Event log event entry should be of type DeleteEvent")
		}
		if logItem3.oppEvents[0].EventType != UpdateEvent {
			t.Errorf("Event log oppEvent entry should be of type UpdateEvent")
		}
		if logItem3.oppEvents[0].Line != line2 {
			t.Errorf("Event log oppEvent should have the new line")
		}

		if logItem3.events[0].ListItemKey != listItem2.Key() {
			t.Errorf("The listItem key should be consistent with the original")
		}
		if logItem3.oppEvents[0].ListItemKey != listItem2.Key() {
			t.Errorf("The listItem key should be consistent with the original")
		}

		repo.Delete(repo.matchListItems[listItem1.Key()])
		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)

		if len(repo.eventLogger.log) != 5 {
			t.Errorf("Event log should have one null and four real events in it")
		}
		if repo.eventLogger.curIdx != 4 {
			t.Errorf("Event logger should have incremented to four")
		}

		logItem4 := repo.eventLogger.log[4]
		if logItem4.events[0].EventType != DeleteEvent {
			t.Errorf("Event log entry event should be of type DeleteEvent")
		}
		if logItem4.oppEvents[0].EventType != UpdateEvent {
			t.Errorf("Event log entry event should be of type UpdateEvent")
		}
		if logItem4.oppEvents[0].Line != line {
			t.Errorf("Event log list item should have the original line")
		}

		if logItem4.events[0].ListItemKey != listItem.Key() {
			t.Errorf("The listItem key should be consistent with the original")
		}
		if logItem4.oppEvents[0].ListItemKey != listItem.Key() {
			t.Errorf("The listItem key should be consistent with the original")
		}

		repo.Undo()

		if len(repo.eventLogger.log) != 5 {
			t.Errorf("Event log should still have five events in it")
		}
		if repo.eventLogger.curIdx != 3 {
			t.Errorf("Event logger should have decremented to three")
		}

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)
		if len(matches) != 1 {
			t.Errorf("Undo should have added the original item back in")
		}
		if matches[0].Line() != line {
			t.Errorf("List item should now have the original line")
		}

		repo.Undo()

		if len(repo.eventLogger.log) != 5 {
			t.Errorf("Event log should still have five events in it")
		}
		if repo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should have decremented to two")
		}

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)
		if len(matches) != 2 {
			t.Errorf("Undo should have added the second original item back in")
		}
		if matches[1].Line() != line2 {
			t.Errorf("List item should now have the original line")
		}

		repo.Redo()

		if len(repo.eventLogger.log) != 5 {
			t.Errorf("Event log should still have five events in it")
		}
		if repo.eventLogger.curIdx != 3 {
			t.Errorf("Event logger should have incremented to three")
		}

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)
		if len(matches) != 1 {
			t.Errorf("Undo should have removed the second original item again")
		}
		if matches[0].Line() != line {
			t.Errorf("List item should now have the original line")
		}
	})
	t.Run("Add empty item, update with character, Undo, Redo", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		repo.Add("", nil, nil)

		logItem := repo.eventLogger.log[1]
		if logItem.events[0].EventType != UpdateEvent {
			t.Errorf("Event log event entry should be of type UpdateEvent")
		}
		if logItem.oppEvents[0].EventType != DeleteEvent {
			t.Errorf("Event log event entry should be of type DeleteEvent")
		}

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)

		newLine := "a"
		repo.Update(newLine, &matches[0])

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should have one null and two real events in it")
		}
		if repo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should have incremented to two")
		}

		logItem2 := repo.eventLogger.log[2]
		if logItem2.events[0].EventType != UpdateEvent {
			t.Errorf("Event log entry event should be of type UpdateEvent")
		}
		if logItem2.oppEvents[0].EventType != UpdateEvent {
			t.Errorf("Event log entry oppEvent should be of type UpdateEvent")
		}
		if logItem2.events[0].Line != newLine {
			t.Errorf("Event log entry event Line should be the new line")
		}
		if logItem2.oppEvents[0].Line != "" {
			t.Errorf("Event log entry event Line should be empty")
		}

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)
		if matches[0].Line() != newLine {
			t.Errorf("List item should now have the new line")
		}

		repo.Undo()

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should still have three events in it")
		}
		if repo.eventLogger.curIdx != 1 {
			t.Errorf("Event logger should have decremented to one")
		}

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)
		// TODO currently caused by bad `if len(line) > 0` check in `update()` handling
		if matches[0].Line() != "" {
			t.Errorf("Undo should have emptied the line")
		}

		repo.Redo()

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should still have two events in it")
		}
		if repo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should have returned to the head at two")
		}

		// TODO problem is, looking ahead to next log item for `Redo` redoes the old PRE state
		// Idea: store old and new state in the log item lines, Undo sets to old, Redo sets to new
		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)
		if matches[0].Line() != newLine {
			t.Errorf("Redo should have added the line back in")
		}
	})
	t.Run("Add line, Delete line, Undo, delete character, Undo", func(t *testing.T) {
		repo, clearUp := setupRepo()
		defer clearUp()

		originalLine := "Original line"
		repo.Add(originalLine, nil, nil)

		if len(repo.eventLogger.log) != 2 {
			t.Errorf("Event log should have a nullEvent and addEvent in it")
		}
		if repo.eventLogger.curIdx != 1 {
			t.Errorf("Event logger should be set to one")
		}

		matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)

		repo.Delete(&matches[0])

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)
		if len(matches) != 0 {
			t.Errorf("Item should have been deleted")
		}

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should have the nullEvent, addEvent and one deleteEvent")
		}
		if repo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should have incremented to one")
		}

		repo.Undo()

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)
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
		repo.Update(newLine, &matches[0])

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should have the nullEvent, addEvent and overriding updateEvent")
		}
		logEvent := repo.eventLogger.log[2]
		if logEvent.events[0].EventType != UpdateEvent {
			t.Errorf("Event logger item event should be of type updateEvent")
		}
		if logEvent.oppEvents[0].EventType != UpdateEvent {
			t.Errorf("Event logger item oppEvent should be of type updateEvent")
		}
		if repo.eventLogger.curIdx != 2 {
			t.Errorf("Event logger should have incremented to 2")
		}

		repo.Undo()

		matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)
		if len(matches) != 1 {
			t.Errorf("There should still be one match")
		}

		if len(repo.eventLogger.log) != 3 {
			t.Errorf("Event log should be unchanged")
		}
		if repo.eventLogger.curIdx != 1 {
			t.Errorf("Event logger should have decremented to 1")
		}

		if matches[0].Line() != originalLine {
			t.Errorf("The line should have reverted back to the original")
		}
	})
	//t.Run("Add three, Move middle down, Move bottom up, Undo twice, Redo twice", func(t *testing.T) {
	//    repo, clearUp := setupRepo()
	//    defer clearUp()

	//    repo.Add("Third", nil, nil)
	//    repo.Add("Second", nil, nil)
	//    repo.Add("First", nil, nil)

	//    matches, _, _ := repo.Match([][]rune{}, true, "", 0, 0)
	//    repo.MoveDown(&matches[1])

	//    matches, _, _ = repo.Match([][]rune{}, true, "", 0, 0)
	//    repo.MoveUp(&matches[2])

	//    if l := len(repo.eventLogger.log); l != 4 {
	//        t.Errorf("Event log len should be %d but is %d", 4, l)
	//    }
	//    if l := repo.eventLogger.curIdx; l != 3 {
	//        t.Errorf("Event logger should be set to %d but is %d", 3, l)
	//    }
	//})
}
