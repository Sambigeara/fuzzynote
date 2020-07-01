# Fuzzy-search based note tool

**IMPORTANT**: This app is in it's very early stages - it may break in unexpected ways and you *may* lose your data. Be wary of what you store in it, or take regular backups of the data in `$HOME/.fzn/` as this is where it stores it's files (see below).

## Config

The root data directory location `$HOME/.fzn/`. You can override this by setting `FZN_ROOT_DIR`. This stores the main list in a file in `primary.db` , and subsequent list item notes in `$FZN_ROOT_DIR/notes/`. 

## Running

Run the bin direct `./bin/fzn` or (soz, haven't yet gotten my head around proper Go build/install processes).

## Search

The top line of the client is used as a search bar. You can separate search terms with `TAB`, each ""search group" will be run on each line separately. If you want to match on a full string, prepend the search group with `#` .

Examples:

- "foo" will match "fobo"
- "#foo" will not match on "fobo", but will match on "foo"

## Controls

`Enter`: Add new line below
`Ctrl-d`: Delete line
`Ctrl-c`: Save and exit
`TAB (top line)`: Add new search group
`ESCAPE` (top line): Clear search groups
`ESCAPE` (not top line): Go to search line

`Ctrl-o`: Opens "Notes" on the currently selected list item. For now, this opens a vim buffer, which will be attributed to the item in question. The note will save when you write and quit out of vim.
