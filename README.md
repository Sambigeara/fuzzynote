Fuzzynote (fzn)
![Github release (latest by date)](https://img.shields.io/github/v/release/sambigeara/fuzzynote)
![Status](https://img.shields.io/badge/status-beta-blue)
![Downloads](https://img.shields.io/github/downloads/sambigeara/fuzzynote/total.svg)
![Follow](https://img.shields.io/twitter/follow/fuzzynote_app?style=social)
==========

# Hyper-fast, local-first, CRDT-backed, collaborative note-taking tool

## Simple, powerful, extremely fast search

`fzn` is local-first; remote-second. It acts on local in-memory data, meaning no expensive I/O and native speeds.

Instant search (over large datasets) via any number of full or fuzzy match groups.

Zoom in using common prefixes.
<br/><br/>

![](basic.gif)

#### Things the user does in this gif :point_up::

1. Opens `fzn`
2. Fuzzy searches for `shopping`, highlights matching lines with `Ctrl-s`, and zooms on common prefix with `Enter` (`=` denotes a full string match).
3. Adds new line (with auto-prepended `shopping ` prefix)
4. Presses `Escape` to go to search line
5. Fuzzy searches for `impo`, highlights and zooms on `important project`
6. Focuses on line ``write `hello` script``, and opens a note buffer (in vim) with `Ctrl-o`
7. Adds the script, then saves and closes the vim buffer, thus attaching the note to the line
8. Fuzzy matches on `fzn`, focuses on a line with a URL, and presses `Ctrl-_` to match the URL and open in default browser

## Real time collaboration

Backed by a [CRDT](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type)-based, append-only, mergeable text database.

Collaborate on a list live, or make changes offline and sync later, with guaranteed and consistent output.

Rather than collaborating on multiple documents, `fzn` will sync lines matching specific terms to different remotes. Remotes can have any number of "collaborators" with access.

In short you can collaborate on multiple "documents" from the same view at the same time.
<br/><br/>

![collaboration](collab.gif)

*Frodo (left) and Joe (right) only share lines with match term `important project`. Non-matched lines remain private. NOTE: Frodo and Joe are using the cloud support to manage their remotes, and for real time collaboration.*

# Remotes

- A "remote" is a remote target where we sync lists.
- Sync all lines, or specify match terms to sync only some. A remote can be a full or partial view.
- Add any number of collaborators who will have access to each remote.

## Web ([quickstart](#web-sign-up-terminal-login))

- Simple remote management
- Real time collaboration
- Managed cloud data store, easy data sync across different computers (simply `fzn login` and start `fzn`)

*Note: for full transparency, if the project takes off, I plan on integrating a paid subscription system to enable me to support the infrastructure and the project in general moving forwards! I'll be proactive in communicating this and will acknowledge any support from early users when making any decisions. Consider this an N-month free trial with "early-release" user status, whatever that means.*

## S3 ([quickstart TODO](foo))

- Configure an S3 bucket yourself and share between collaborators for near real-time collaboration and backup.

# Quickstart

## Web sign-up, terminal login

1. [Install `fzn`](#installation)
2. Sign up [here](https://fuzzynote.auth.eu-west-1.amazoncognito.com/signup?client_id=5a7brt2fuvlfnl8aql1af3758m&response_type=token&scope=email+openid&redirect_uri=https://github.com/Sambigeara/fuzzynote)
3. Login
```
./fzn login
```
4. Start
```
./fzn
```

## Add a remote and collaborator

In this example, Frodo creates a remote for `important project` and invites Joe to collaborate

1. Open the interactive menu
```
./fzn cfg
```
2. Select `Add new remote...`
```
# Example output
? Select action:
  ▸ Remote: main (1748937357)
    Add new remote...  # <- select this
    Exit
```
3. Specify the name, Frodo chooses `important project`
```
✔ Add new remote...
✔ Specify name for new remote: important project
```
4. Select newly created remote
```
? Select action:
  ▸ Remote: main (1748937357)
    Remote: important project (8934754397)  # <- select this
    Add new remote...
    Exit
```
5. Set the "match term" - all lines that match this term will sync with the remote. Note: this includes lines that previously matched the term, but no longer do.
```
? Remote: important project (8934754397):
    Manage collaborators...
  ▸ Name: important project
    Match: UPDATE ME 2596996162  # <- select this, and enter "important project"
    IsActive: true
    Delete? (for all collaborators)
    Exit
```
6. Add Joe as a collaborator
```
# Go to collaborator management
? Remote: important project (8934754397):
    Manage collaborators...  # <- select this
  ▸ Name: important project
    Match: important project
    IsActive: true
    Delete? (for all collaborators)
    Exit

# Invite Joe
? Manage collaborators:
  ▸ Add new collaborator...  # <- select this, and add the email
    Exit
```

## Accept and configure invite to remote

Joe responds to the invite above

# Installation

Compile locally (requires Go):

```shell
git clone git@github.com:Sambigeara/fuzzynote.git
cd fuzzynote
make build # Installs binary to `/bin/fzn`
```

Or download the binary direct from the [releases page](https://github.com/Sambigeara/fuzzynote/releases/latest).

# Quick-start

## General usage

### Search

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

### Controls

#### Navigation

- General navigation: `Arrow keys`
- Go to start of item: `Ctrl-a`
- Go to end of item: `Ctrl-e`
- Go to search line: `ESCAPE`
- Exit: `Double ESCAPE`

#### State

- Add new line below (prepending search line to new line): `Enter`
- Copies line into buffer and deletes it, or clear search groups: `Ctrl-d`
- Moves the current item up or down in the list: `Alt-]/Alt-[`
- Select items under cursor. Then press `Enter` to set common prefix to search, or `Escape` to clear selected items: `Ctrl-s`
- Undo/Redo: `Ctrl-u/Ctrl-r`

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
