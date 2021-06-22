Fuzzynote (fzn)
![Github release (latest by date)](https://img.shields.io/github/v/release/sambigeara/fuzzynote)
![Status](https://img.shields.io/badge/status-beta-blue)
![Downloads](https://img.shields.io/github/downloads/sambigeara/fuzzynote/total.svg)
![Follow](https://img.shields.io/twitter/follow/fuzzynote_app?style=social)
==========

- [Installation](#installation)
- [Quickstart](#quickstart)

# Terminal-based, hyper-fast, CRDT-backed, collaborative note-taking tool

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

*Frodo (left) and Joe (right) only share lines with match term `important project`. Non-matched lines remain private. NOTE: Frodo and Joe are using the web support to manage their remotes, and for real time collaboration.*

# Remotes

A "remote" is a remote target where we sync lists.

- Sync all lines, or specify match terms to sync only some. A remote can be a full or partial view.
- Add any number of collaborators who will have access to each remote.

## Web ([quickstart](#web-sign-up-terminal-login))

- Simple remote management
- Real time collaboration
- Managed cloud data store, easy data sync across different computers (simply `fzn login` and start `fzn`)

*Note: for full transparency, if the project takes off, I'll integrate a paid subscription system to enable me to support the infrastructure and the project on an ongoing basis. I'll be proactive in communicating this and will acknowledge any support from early users when making any decisions! Consider this an N-month free trial with "early-release" user status.*

<!--## S3 ([quickstart TODO](foo))-->

<!--Configure an S3 bucket yourself and share between collaborators for near real-time collaboration and backup.-->

# Installation

Compile locally (requires Go):

```shell
git clone git@github.com:Sambigeara/fuzzynote.git
cd fuzzynote
make build # Installs binary to `/bin/fzn`
```

Or download the binary direct from the [releases page](https://github.com/Sambigeara/fuzzynote/releases/latest).

# Quickstart

- [Basic usage](#basic-usage)
- [Web sign-up, terminal login](#web-sign-up-terminal-login)
- [Add a "remote"](#add-a-remote)
- [Add a collaborator](#add-a-collaborator)
- [Accept an invitation](#accept-an-invitation)
<!--- [Setup an S3 remote](#setup-an-s3-remote)-->

## Basic usage

1. [Install `fzn`](#installation)
2. Start
```shell
./fzn
```

## Web sign-up, terminal login

1. [Install `fzn`](#installation)
2. Sign up [here](https://fuzzynote.auth.eu-west-1.amazoncognito.com/signup?client_id=5a7brt2fuvlfnl8aql1af3758m&response_type=token&scope=email+openid&redirect_uri=https://github.com/Sambigeara/fuzzynote)
3. Login and follow prompts
```shell
./fzn login
```
4. Start
```shell
./fzn
```

## Add a "remote"

In this example, Frodo creates a remote for `important project`

1. Open the interactive menu
```shell
./fzn cfg
```
2. Select `Add new remote...`
```shell
# Example output
? Select action:
    Remote: main (1748937357)
  ▸ Add new remote...
    Exit
```
3. Specify the name, Frodo chooses `important project`
```shell
✔ Add new remote...
✔ Specify name for new remote: important project
```
4. Select newly created remote
```shell
? Select action:
    Remote: main (1748937357)
  ▸ Remote: important project (8934754397)
    Add new remote...
    Exit
```
5. Set the "match term"
   - **All lines that match this term will sync with the remote**
   - **Setting to an empty string will mean that ALL lines will be sync'd and all collaborators will have access**
   - **Note**: this includes lines that previously matched the term, but no longer do (e.g. if you delete the particular matching substring from the line)
```shell
# Select "Match"
? Remote: important project (8934754397):
    Manage collaborators...
    Name: important project
  ▸ Match: UPDATE ME 2596996162
    IsActive: true
    Delete? (for all collaborators)
    Exit

# Enter new match term and press Enter
✔ Enter new value: important project
```

## Add a collaborator

Joe is invited to the `important project` remote:

```shell
# Select "Manage collaborators..."
? Remote: important project (8934754397):
  ▸ Manage collaborators...
    Name: important project
    Match: important project
    IsActive: true
    Delete? (for all collaborators)
    Exit

# Select "Add new collaborator..."
? Manage collaborators:
  ▸ Add new collaborator...
    Exit

# Add email address, and press Enter
✔ Enter email address: joe@bloggs.com
```

## Accept an invitation

Joe responds to the invite above

1. Open the interactive menu

```shell
./fzn cfg
```

2. Select the newly added remote - all invited remotes will start with `INVITE: `

```shell
? Select action:
    Remote: main (6782346574)
  ▸ Remote: INVITE: important project (8934754397)
    Add new remote...
    Exit
```

3. (Optional) update the name

```shell
# Select "Name"
? Remote: INVITE: important project (8934754397):
  ▸ Name: INVITE: important project
    Match: UPDATE ME 7398574395
    IsActive: false
    Delete?
    Exit
    
# Enter new name and press Enter
✔ Enter new value: important project
```

4. Update the match term (this can be anything, and does not need to be consistent with other collaborator's match terms - but keeping them consistent is more predictable).

```shell
# Select "Match"
? Remote: important project (8934754397):
    Name: important project
  ▸ Match: UPDATE ME 7398574395
    IsActive: false
    Delete?
    Exit

# Enter new match term and press Enter
✔ Enter new value: important project
```

5. Activate the remote (it's `false` by default to prevent accidental sharing of your personal notes)

```shell
# Select "IsActive" and choose "true"
? Remote: important project (8934754397):
    Name: important project
    Match: UPDATE ME 7398574395
  ▸ IsActive: false
    Delete?
    Exit
```

6. Start the app - the sync will occur in the background and you can start collaborating on the remote

```shell
./fzn
```

<!--## Setup an S3 remote-->

<!--1. Configure an S3 bucket with access via access key/secret - [link to AWS docs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html)-->

<!--2. Create a file called `config.yml` in the `fzn` root directory. By default, this is at `$HOME/.fzn/` on `*nix` systems, or `%USERPROFILE%\.fzn` on Windows.-->

<!--3. Add the following to the file, using key/secret from above:-->

<!--```yml-->
<!--s3:-->
<!--  - key: {AWS_ACCESS_KEY}-->
<!--    secret: {AWS_SECRET}-->
<!--    bucket: fuzzynote-->
<!--    prefix: main-->
<!--```-->

<!--4. Start the app-->

<!--```shell-->
<!--./fzn-->
<!--```-->


# Controls

## Search

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

## Navigation

- General navigation: `Arrow keys`
- Go to start of item: `Ctrl-a`
- Go to end of item: `Ctrl-e`
- Go to search line: `ESCAPE`
- Exit: `Double ESCAPE`

## State

- Add new line below (prepending search line to new line): `Enter`
- Copies line into buffer and deletes it, or clear search groups: `Ctrl-d`
- Moves the current item up or down in the list: `Alt-]/Alt-[`
- Select items under cursor. Then press `Enter` to set common prefix to search, or `Escape` to clear selected items: `Ctrl-s`
- Undo/Redo: `Ctrl-u/Ctrl-r`

## Items

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
