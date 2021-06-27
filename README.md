Fuzzynote (fzn)
![Github release (latest by date)](https://img.shields.io/github/v/release/sambigeara/fuzzynote)
![Status](https://img.shields.io/badge/status-beta-blue)
![Downloads](https://img.shields.io/github/downloads/sambigeara/fuzzynote/total.svg)
---

- [Installation](#installation)
- [Quickstart](#quickstart)
- [Controls](#controls)
- [Configuration](#configuration)
- [Future Plans](#future-plans)
- [Issues/Considerations](#issuesconsiderations)

[Follow me on Twitter](https://twitter.com/fuzzynote_app) for the latest `fzn` updates and announcements, or just to watch me talk to myself.

---

# Terminal-based, hyper-fast, CRDT-backed, collaborative note-taking tool

## Simple, powerful, extremely fast search

`fzn` is local-first; remote-second. It acts on local in-memory data, meaning no expensive I/O and native speeds.

Instant search (over large datasets) via any number of full or fuzzy match groups.

Zoom in using common prefixes.
<br/><br/>

![](demos/basic.gif)

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

In short, you can collaborate on multiple "documents" from the same view at the same time.
<br/><br/>

![collaboration](demos/collab.gif)

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

## S3 ([quickstart](#setup-an-s3-remote))

Configure an S3 bucket yourself and share between collaborators for near real-time collaboration and backup.

# Installation

## Local compilation

Compile locally (requires Go):

```shell
git clone git@github.com:Sambigeara/fuzzynote.git
cd fuzzynote
make build # Installs binary to `/bin/fzn`
```

## Direct download

From the [releases page](https://github.com/Sambigeara/fuzzynote/releases/latest).

## ArchLinux

[Link to AUR package](https://aur.archlinux.org/packages/fuzzynote/).

ArchLinux users can build and install `fzn` with:

```shell
yay -S fuzzynote
```

# Quickstart

- [Basic usage](#basic-usage)
- [Web sign-up, terminal login](#web-sign-up-terminal-login)
- [Add a "remote"](#add-a-remote)
- [Add a collaborator](#add-a-collaborator)
- [Accept an invitation](#accept-an-invitation)
- [Setup an S3 remote](#setup-an-s3-remote)

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
    Match: important project
  ▸ IsActive: false
    Delete?
    Exit
```

6. Start the app - the sync will occur in the background and you can start collaborating on the remote

```shell
./fzn
```

## Setup an S3 remote

1. Configure an S3 bucket with access via access key/secret - [link to AWS docs](https://docs.aws.amazon.com/AmazonS3/latest/userguide/create-bucket-overview.html).

2. Create a file called `config.yml` in the `fzn` root directory. By default, this is at `$HOME/.fzn/` on `*nix` systems, or `%USERPROFILE%\.fzn` on Windows. If you've already run `fzn`, the root directory will have a `primary.db` and one or more `wal_*.db` files, for reference.

3. Add the following to the file, using key/secret from above:
```yml
s3:
  - key: {AWS_ACCESS_KEY}
    secret: {AWS_SECRET}
    bucket: bucket_name
    prefix: some_prefix
```

4. **Optional:** specify the "match term" to only sync matching lines:
```yml
s3:
  - key: {AWS_ACCESS_KEY}
    secret: {AWS_SECRET}
    bucket: bucket_name
    prefix: some_prefix
    match: some match term  # Add this to only sync matching lines
```

5. **Optional:** set the `sync` intervals (via envvar or inline flag on startup). The default interval is 10 seconds, meaning `fzn` will flush local changes to the remote every 10 seconds. Likewise, in a separate thread, `fzn` will retrieve new changes _from_ the remote every 10 seconds.

   If you want nearer real-time sync (perhaps for collaboration?), you can reduce the interval via an envvar, e.g.:
   ```shell
   export FZN_SYNC_FREQUENCY_MS=1000
   ```

   or in-line:
   ```shell
   ./fzn --sync-frequency-ms=1000
   ```

   Each of the above will set to the interval to 1000ms (1 second).

   **Note:** extensive I/O to S3 can be more expensive than expected, albeit only pennies in the beginning - worth keeping an eye out if you favour short sync intervals.

5. Start the app, if you haven't already
```shell
./fzn
```

## Other remote platforms?

At present `fzn` only supports S3 as a remote target. However, it is easily extensible, so if there is demand for additional platforms, then please make a request via a [new issue](https://github.com/Sambigeara/fuzzynote/issues/new)!

# Controls

## Navigation

- General navigation: `Arrow keys`
- Go to start of line: `Ctrl-a`
- Go to end of line: `Ctrl-e`
- Go to search line: `ESCAPE`
- Exit: `Double ESCAPE`

## Search (top line)

Any number of tab-separated search groups are applied to the lists independently. Use full, fuzzy, or inverse string matching.

- Full string match: start the search group with `=`
- Inverse string match (full strings), start the search group with `=!`
- Separate search groups: `TAB`

```shell
foo # matches "fobo"
=foo # will not match "fobo"
=!foo # will ignore any lines with "foo" in it
```

## List items (lines)

- Add new line (prepending search line text to new line): `Enter`
- Delete line: `Ctrl-d`
- Undo/Redo last operation: `Ctrl-u/Ctrl-r`
- Moves current item up or down: `Alt-]/Alt-[`
- Open note on the currently selected list item in selected terminal editor (default is Vim). Save in editor saves to list item: `Ctrl-o`
- Copy current item into buffer: `Ctrl-c`
- Paste current item from buffer: `Ctrl-p`

## Group operations

- Select item under cursor: `Ctrl-s`
- Set common prefix string to search line: `Enter`
- Clear selected items: `Escape`

## Visibility

- Toggle global visibility: `Ctrl-i (top line)`
- Toggle list item visibility: `Ctrl-i`

## Handy functions

- Open first URL in list item: `Ctrl-_`
- Copy first URL from list item into the system clipboard: `Ctrl-c`
- Export current matched lines to text file (will output to `current_dir/export_*.txt`): `Ctrl-^`

## Token operators

The following character combinations will parse to different useful outputs:

- `{d}`: A date in the form `Mon, Jan 2, 2006`

# Configuration

`fzn --help` will print out the configurable options.

```
> fzn --help
Usage: fzn [options] [arguments]

OPTIONS
  --root/$FZN_ROOT                                <string>
  --colour/$FZN_COLOUR                            <string>  (default: light)
  --editor/$FZN_EDITOR                            <string>  (default: vim)
  --sync-frequency-ms/$FZN_SYNC_FREQUENCY_MS      <uint>    (default: 10000)
  --gather-frequency-ms/$FZN_GATHER_FREQUENCY_MS  <uint>    (default: 30000)
  --help/-h
  display this help message
  --version/-v
  display version information
```

- `editor`: specifies the terminal editor which is used when opening notes on list items. `vim`, `emacs` and `nano` all appear to work. Others may too.
- `sync-frequency-ms`/`gather-frequency-ms`: these can be ignored for now
- `root`: **(mostly for testing and can be ignored for general use)** specifies the directory that `fzn` will treat as it's root. By default, this is at `$HOME/.fzn/` on `*nix` systems, or `%USERPROFILE%\.fzn` on Windows.

# Future plans

- Web-app (Wasm)
- E2E encryption

# Issues/Considerations

The terminal client is fully functioning, however given the early stages of the project, and the (at points) rapid development, there are likely to be some bugs hanging around. Things to look out for:

- If `fzn` is left idle for some time, it might ungracefully error (usually due to some web connection issue) and exit. Under very rare circumstances, it might hang and require the user to kill the process manually (via `kill` commands). Due to the nature of the app, your data will almost certainly be unaffected.
- Sometimes the sync algorithm gets confused. Usually, all that is needed is just to add or delete a line or character (adding additional events to the WAL will trigger a flush and get things moving). If that doesn't work, turning it off and on again usually does the trick.
- Notice something wrong? Please do [open an issue](https://github.com/Sambigeara/fuzzynote/issues/new)!
