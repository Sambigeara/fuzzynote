Fuzzynote (fzn)
![Github release (latest by date)](https://img.shields.io/github/v/release/sambigeara/fuzzynote)
![Status](https://img.shields.io/badge/status-beta-blue)
![Downloads](https://img.shields.io/github/downloads/sambigeara/fuzzynote/total.svg)
==========

## Hyper-fast, local-first, CRDT-backed, collaborative note-taking tool

### Simple, powerful, extremely fast search

String together numerous full or fuzzy match groups. Zoom in using common prefixes. FZN is "local-first" - it acts on local in-memory data, meaning native speeds (and as-good-as instant performance).

### Real time collaboration

Backed by a [CRDT](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type)-based, append-only, mergeable text database. Collaborate on a list live, or make changes offline and sync later, with guaranteed and consistent output.

### Single view, multiple documents

Rather than collaborating on single documents, FZN will sync lines matching specific terms to different remotes. Remotes can have any number of "collaborators" with access. Therefore you can collaborate on multiple "documents" from the same view at the same time.

### S3 remote support

Fuzzynote can be powered by a single shared static file store. Configure one yourself and share between collaborators for near real-time collaboration and backup.

### Hosted offering

*link*

## Installation

Compile locally (requires Go):

```shell
git clone git@github.com:Sambigeara/fuzzynote.git
cd fuzzynote
make build # Installs binary to `/bin/fzn`
```

Or download the binary direct from the [releases page](https://github.com/Sambigeara/fuzzynote/releases/latest).

## Quick-start

### General usage

#### Search

Any number of tab-separated search groups are applied to the lists independently. Use full, fuzzy, or inverse string matching.

- Full string match: prepend the search group with `=`

- Inverse string match (full strings), prepend the search group with `=!`

- Separate search groups with `TAB`

E.g.:

```shell
foo # matches "fobo"
=foo # will not match "fobo"
=!foo # will ignore any lines with "foo" in it
```

#### Controls

##### Navigation

- General navigation: `Arrow keys`
- Go to start of item: `Ctrl-a`
- Go to end of item: `Ctrl-e`
- Go to search line: `ESCAPE`
- Exit: `Double ESCAPE`

##### State

- Add new line below (prepending search line to new line): `Enter`
- Copies line into buffer and deletes it, or clear search groups: `Ctrl-d`
- Moves the current item up or down in the list: `Alt-]/Alt-[`
- Select items under cursor. Then press `Enter` to set common prefix to search, or `Escape` to clear selected items: `Ctrl-s`
- Undo/Redo: `Ctrl-u/Ctrl-r`

#### Items

- `Ctrl-i (top line)`: Toggle between `show all` and `show visible`
- `Ctrl-i`: Toggle visibility of current item
- `Ctrl-o`: Opens "Notes" on the currently selected list item. For now, this opens a vim buffer, which will be attributed to the item in question. The note will save when you write and quit out of vim
- `Ctrl-c`: Copy current item into buffer. If there are any URLs in the string, copy the first into the system clipboard
- `Ctrl-p`: Paste current item from buffer below current position
- `Ctrl-_`: If there are any URLs in the string, open the first using the default browser

### Token operators

When typed in a search group, the following character combinations will parse to different useful outputs:

- `{d}`: A date in the form `Mon, Jan 2, 2006`

### Colour scheme

You can trigger between `light` and `dark` by setting `FZN_COLOUR` to the former or latter accordingly.
