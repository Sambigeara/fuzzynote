# Fuzzy-search based note tool

**IMPORTANT**: This app is in it's very early stages - it may break in unexpected ways and you *may* lose your data. Be wary of what you store in it, or take regular backups of the data in `$HOME/.fzn/` as this is where it stores it's files (see below).

## Config

The root data directory location `$HOME/.fzn/`. You can override this by setting `FZN_ROOT_DIR`. This stores the main list in a file in `primary.db` , and subsequent list item notes in `$FZN_ROOT_DIR/notes/`. 

## Running

Run the bin direct `./bin/fzn` or add to your PATH (soz, haven't yet gotten my head around proper Go build/install processes).

## Search

The top line of the client is used as a search bar. You can separate search terms with `TAB`, each "search group" will be run on each line separately. If you want to match on a full string, prepend the search group with `=`. To inverse string match (on full strings), prepend the search group with `=!`.

Examples:

- `foo` will match `fobo`
- `=foo` will not match on `fobo`, but will match on `foo`
- `=!foo` will ignore any lines with the **full** string `foo` in it

## Controls

### Navigation

- `Arrow keys`: General navigation
- `Ctrl-a` (not search line): Go to start of item
- `Ctrl-e` (not search line): Go to end of item
- `ESCAPE` (not top line): Go to search line
- `Double ESCAPE`: Exit

### Search

- `TAB (top line)`: Add new search group
- `Enter`: Add new line below (prepends search line to new line)
- `Ctrl-d`: Copies line into buffer and deletes it, or clear search groups
- `Alt-]/Alt-[`: Moves the current item up or down in the list
- `Ctrl-s`: Select items under cursor. Then press `Enter` to set common prefix to search, or `Escape` to clear selected items.

### State

- `Ctrl-u/Ctrl-r`: Undo/Redo

### Items

- `Ctrl-i (top line)`: Toggle between `show all` and `show visible`
- `Ctrl-i`: Toggle visibility of current item
- `Ctrl-o`: Opens "Notes" on the currently selected list item. For now, this opens a vim buffer, which will be attributed to the item in question. The note will save when you write and quit out of vim
- `Ctrl-c`: Copy current item into buffer. If there are any URLs in the string, copy the first into the system clipboard
- `Ctrl-p`: Paste current item from buffer below current position
- `Ctrl-_`: If there are any URLs in the string, open the first using the default browser

## Token operators

When typed in a search group, the following character combinations will parse to different useful outputs:

- `{d}`: A date in the form `Mon, Jan 2, 2006`

## Colour scheme

You can trigger between `light` and `dark` by setting `FZN_COLOUR` to the former or latter accordingly.

## Workflowy importer

I have historically used Workflowy as my note-taking tool of choice. Because of this, I've written an importer which converts the output XML from Workflowy's `export` function into the flat format fzn expects. It will also take care of converting notes that existed on the Workflowy nodes. To use this, do the following:

1. Export data from workflowy in the OPML (aka extended XML) format
2. Paste this data into a file named `workflowy_source.xml` in the project root
3. Run `./bin/importer`
4. By default, this generates all the data in `$FZN_ROOT_DIR/import/` (to prevent any accidental merges - you can then move to `.fzn/` manually)
5. If you wish to merge directly with any existing data, you can specify the directory with the `FZN_IMPORT_ROOT_DIR` argument as such:

```bash
FZN_IMPORT_ROOT_DIR=$HOME/.fzn/ ./bin/import
```
