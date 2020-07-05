# Fuzzy-search based note tool

**IMPORTANT**: This app is in it's very early stages - it may break in unexpected ways and you *may* lose your data. Be wary of what you store in it, or take regular backups of the data in `$HOME/.fzn/` as this is where it stores it's files (see below).

## Config

The root data directory location `$HOME/.fzn/`. You can override this by setting `FZN_ROOT_DIR`. This stores the main list in a file in `primary.db` , and subsequent list item notes in `$FZN_ROOT_DIR/notes/`. 

## Running

Run the bin direct `./bin/fzn` or add to your PATH (soz, haven't yet gotten my head around proper Go build/install processes).

To see the app TODO list, run the following: `FZN_ROOT_DIR=todo ./bin/fzn`

## Search

The top line of the client is used as a search bar. You can separate search terms with `TAB`, each "search group" will be run on each line separately. If you want to match on a full string, prepend the search group with `#` .

Examples:

- "foo" will match "fobo"
- "#foo" will not match on "fobo", but will match on "foo"

## Controls

- `Enter`: Add new line below
- `Ctrl-d`: Delete line
- `Ctrl-s`: Save
- `Ctrl-x`: Exit
- `Ctrl-_`: Exit WITHOUT saving
- `TAB (top line)`: Add new search group
- `ESCAPE` (top line): Clear search groups
- `ESCAPE` (not top line): Go to search line
- `Ctrl-o`: Opens "Notes" on the currently selected list item. For now, this opens a vim buffer, which will be attributed to the item in question. The note will save when you write and quit out of vim.

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
