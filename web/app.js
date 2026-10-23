// The browser face: a blank page with one cursor on it.
//
// The engine is the app. It runs here as WebAssembly and is told what was asked for, as an
// action, and says what to show. This file is what a page adds to it: which key or tap asks
// for what, painting the view, the socket to the relay this page came from, the other tabs open
// on the space, and somewhere to keep lines between visits.
//
// There is one cursor on the page and it moves. On the line it is the caret, and typing finds. Down
// puts it on a row, whose head is the editor with the caret at its end, so typing there changes the
// head and nothing has to be pressed to begin or to keep it: the engine writes each change as it is
// typed. The head reads as the row read off the cursor, but for the span the caret touches, which
// shows its marks as typed, as an open body does (see the editor), so that walking the list moves
// no text. Enter with text on the line writes it and puts the editor in the note it wrote. Enter
// on a row opens a new note under it, its empty head the editor's, so that Enter, type, Enter, type
// writes notes in a run, as the original's Enter did, unless its head has `/task` or a date word
// still to write out, which Enter writes out first. Ctrl-O opens the body, the original's chord for
// it, in the sheet: a bounded overlay over the dimmed page, scrolled on its own, where the editor
// holds the whole note. The arrows stay inside it, and Escape closes it, or with vim's keys `:q`;
// leaving it for anywhere else on the page closes it too. Touch has no chord: a tap beside a head's
// words puts the cursor on the note and a second there opens it, and a tap on the words writes in
// the head with the caret under the finger. A body is never shown under its row. On a wide page the
// body of what the cursor is on is read beside the list: the note under it, or from the line the
// note you are in. On a narrower one a note's body is read by opening it, and the note you are in
// is read above the line. A body read is rendered as markdown, and a row's links are links (see
// render). Tab goes into the note from anywhere in its head, and from the line into what it names,
// and the arrows are the caret's; Escape comes back a rung at a time, off the row, off the text on
// the line, out of the place. Control with an arrow moves the note on the row up or down the list.
// Everything else is a word in the foot, moving a note included, and only what applies right now
// gets one; a word a key also asks for has the key written after it, where there is a pointer to
// read it with. The chords are the terminal's wherever a browser lets them go, and where it does
// not, the terminal names the page's, so that the two faces are one set of keys to learn. While a
// tag or a person is typed, or a note's name inside `[[`, what the space has that it starts floats
// under it, and Tab or Enter takes the one marked (see paintOffer).
//
// The painting is kept cheap because it runs on every keystroke: only the rows that can be seen
// are made, rows are keyed by id and their nodes reused, nothing measures the page but the caret,
// once at the end, and the note you are in when it changes, and only a row that is new fades in.

import init, { Fzn, deriveKey } from './pkg/fzn_web.js';

const $ = (id) => document.getElementById(id);
const el = (tag, cls, text) => {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  if (text != null) node.textContent = text;
  return node;
};

// A space is asked for by name: /?space=work. A name has to be a plain one, as the name the spaces
// are under does.
const PLAIN = /^[A-Za-z0-9_-][A-Za-z0-9._-]{0,63}$/;
const address = new URLSearchParams(location.search);
const asked = address.get('space') || '';
const space = PLAIN.test(asked) ? asked : 'main';
const title = space === 'main' ? 'fzn' : `fzn · ${space}`;
document.title = title;

// What this browser keeps for a space, besides its lines: the key, and the writer ids.
const KEY = `fzn.key.${space}`;

// The browser's storage, and the tab's own, which a reload keeps and another tab does not have.
const local = () => localStorage;
const session = () => sessionStorage;

function kept(name, where = local) {
  try {
    return where().getItem(name);
  } catch {
    return null;
  }
}

function keep(name, value, where = local) {
  try {
    if (value == null) where().removeItem(name);
    else where().setItem(name, value);
  } catch {
    // Nowhere to keep it, so it is asked for or made again next time.
  }
}

// A token in the link, `#joe:3f9a…` or `#3f9a…`, is kept for this browser and taken off the
// address bar and out of the tab's history, though the browser's list of the pages visited still
// has the link as it was opened. It is what lets a browser make a space on the relay, under the
// name before it or at the relay's root; a space that is there already is entered with its key
// alone. With a name the spaces are asked for under it.
const TOKEN = 'fzn.token';
const given = location.hash.match(/^#(?:([A-Za-z0-9_-][A-Za-z0-9._-]{0,63}):)?([0-9a-f]{32})$/);
if (given) keep(TOKEN, given[1] ? `${given[1]}:${given[2]}` : given[2]);
// The name is said in the address as well, `?name=joe`, being no secret, since an address is all a
// home-screen app is added with, and on iOS such an app keeps storage apart from Safari's and so
// starts with no token: with the name it still asks for joe's spaces, which are there by then and
// let it in on their key alone. What a browser keeps of a space is under the space's name and not
// the name it is under, so a browser keeps to one name: one from the address is kept as a token's
// is when the browser holds none, and the address says the name held, whatever it said before.
const addressed = address.get('name') || '';
if (!kept(TOKEN) && PLAIN.test(addressed)) keep(TOKEN, `${addressed}:`);
const credential = kept(TOKEN) || (PLAIN.test(addressed) ? `${addressed}:` : '');
const whose = credential.includes(':') ? credential.slice(0, credential.indexOf(':')) : '';
const token = credential.slice(credential.indexOf(':') + 1);
if (whose) address.set('name', whose);
else address.delete('name');
const query = address.toString();
history.replaceState(history.state, '', `${location.pathname}${query ? `?${query}` : ''}${given ? '' : location.hash}`);

const hex = (bytes) =>
  [...crypto.getRandomValues(new Uint8Array(bytes))].map((b) => b.toString(16).padStart(2, '0')).join('');
const plain = (id) => /^[0-9a-f]{8}$/.test(id || '');
const lapse = (ms) => new Promise((done) => setTimeout(done, ms));

// A tab's writer id for the space: its name in the versions it writes, and so the name of its log.
// Every tab open on the space writes a log of its own, since two writing one would fork it.
//
// With Web Locks the tabs share a pool of ids. A tab holds a slot for as long as it is open, the
// lowest that no other tab holds, by a lock the browser lets go when the tab is closed or dies; the
// lock's callback never settles, so it is held for the page's life. The id is kept under the slot,
// made once, so the next tab to hold the slot goes on with the same log. Slot 0's id is kept where
// the one id a browser had was kept before tabs had slots, and a browser from before ids were kept
// a space at a time had one for every space, which each space's slot 0 takes over.
//
// A browser gives Web Locks only to a page served over TLS or from the machine itself, and a relay
// elsewhere serves neither. Without them nothing tells a tab that has gone from one the browser has
// frozen, so a tab never goes on with a log another may still write: its id is its own for as long
// as the browser keeps the tab, in the tab's own storage. It is gone on with only after the page
// before it said, as it went, that it had left, so a reload, the reload onto a new build and a
// closed tab opened again keep their log, and a tab duplicated, whose storage is copied from a page
// still open, a new tab, or one the browser killed, start another. That is a log for every tab that
// writes, a few lines each on the relay, where a log shared would be a fork. A tab's storage can
// also be copied after its page has left, by a window it opens or by Duplicate, and then the copy
// and the tab would both find that it had left; so what the page leaves is a token that one page
// alone can spend: the same random word in the tab's storage and in the browser's under the id,
// taken from the browser's as the id is gone on with. Whichever copy opens first has the log, and
// the others start their own.
let WRITER = '';
let writerIn = local;
const LEFT = `fzn.left.${space}`;
const leftBy = (id) => `${LEFT}.${id}`;
async function writer() {
  if (navigator.locks) {
    const forever = new Promise(() => {});
    for (let n = 0; ; n++) {
      const got = await new Promise((done) =>
        navigator.locks.request(`fzn:${space}:writer:${n}`, { ifAvailable: true }, (lock) => {
          done(lock !== null);
          return lock && forever;
        }),
      );
      if (!got) continue;
      WRITER = n === 0 ? `fzn.writer.${space}` : `fzn.writer.${space}.${n}`;
      let id = kept(WRITER);
      if (!plain(id)) {
        const old = n === 0 ? kept('fzn.device') : null;
        id = plain(old) ? old : hex(4);
        keep(WRITER, id);
      }
      return id;
    }
  }
  WRITER = `fzn.writer.${space}`;
  writerIn = session;
  const left = kept(LEFT, session);
  keep(LEFT, null, session);
  let id = kept(WRITER, session);
  if (plain(id) && left && kept(leftBy(id)) === left) keep(leftBy(id), null);
  else keep(WRITER, (id = hex(4)), session);
  forgetOldLeavings();
  return id;
}

// A token is left behind for every tab closed and never opened again, which is bytes; the newest
// fifty are kept, by the time each begins with.
function forgetOldLeavings() {
  try {
    const leavings = Object.keys(localStorage).filter((name) => name.startsWith('fzn.left.'));
    leavings.sort((a, b) => (kept(b) ?? '').localeCompare(kept(a) ?? ''));
    for (const name of leavings.slice(50)) keep(name, null);
  } catch {
    // Left where they are, they are pruned another time.
  }
}

// The tabs open on the space find each other over a channel, as `tab`, an id made afresh each
// time a page loads. Once open each is a peer of the others, numbered from 1, the relay being 0:
// what one writes reaches the others as a line from any peer does, in the next frame or two, and
// what one hears from the relay it passes on, so a tab whose socket is down still syncs through
// another.
const tab = hex(4);
const tabs = 'BroadcastChannel' in window ? new BroadcastChannel(`fzn:${space}`) : null;

// The other tabs, each a peer: a tab's id to its number, and back.
const peers = new Map();
const tabOf = new Map();
let nextPeer = 1;
// Whether this tab has opened the space, and so can take in what the others send.
let joined = false;
function peerOf(id) {
  if (!peers.has(id)) {
    const n = nextPeer++;
    peers.set(id, n);
    tabOf.set(n, id);
    // Says what this tab holds, to that tab only.
    carryOut(fzn.connected(n));
  }
  return peers.get(id);
}

// A bridge for pages of the build before tabs were peers, which knew one tab at a time: such a
// page stands down when it hears `here`, and writes the log slot 0 now goes on with. So this page
// says `here` as it opens, and says it back to a page that opens later, but not straight after
// saying it itself, or two pages of this build would answer each other for ever. It goes once no
// page of that build can still be open.
let hereAt = 0;
const here = () => {
  hereAt = Date.now();
  tabs?.postMessage('here');
};

if (tabs) {
  tabs.onmessage = ({ data }) => {
    if (data === 'here') return Date.now() - hereAt > 1000 && here();
    if (!data || typeof data !== 'object' || data.from === tab || (data.to && data.to !== tab)) return;
    if (!joined) return;
    if (data.bye) {
      const n = peers.get(data.from);
      if (n == null) return;
      fzn.disconnected(n);
      peers.delete(data.from);
      tabOf.delete(n);
    } else if (data.hello) {
      peerOf(data.from);
      if (!data.to) tabs.postMessage({ from: tab, to: data.from, hello: true });
    } else if (typeof data.text === 'string') {
      carryOut(fzn.received(peerOf(data.from), data.text));
    }
  };
}

// The space's key, from what was kept or from the passphrase. The field is where the line will
// be, so that the page does not change shape when it opens. Argon2 holds the page for a moment,
// so the page says what it is doing first, and goes on saying it until the space is open and the
// page is shown (see start): a line shown earlier would take keys that the opening then clears.
const ASK = $('for').innerHTML;
function key(why = '') {
  const found = kept(KEY);
  if (found) return Promise.resolve(found);
  const form = $('unlock');
  const secret = $('secret');
  const told = $('unlocking');
  const asking = $('for');
  asking.innerHTML = ASK;
  $('space').textContent = space;
  if (why) {
    told.textContent = why;
    told.classList.add('trouble');
  }
  form.hidden = false;
  document.body.classList.add('locked');
  secret.disabled = false;
  if (!coarse.matches) secret.focus();
  placeCaret();
  return new Promise((resolve) => {
    form.onsubmit = (event) => {
      event.preventDefault();
      if (!secret.value) return;
      secret.disabled = true;
      asking.textContent = 'opening…';
      // Two frames, so that the word is on the screen before Argon2 has the page.
      requestAnimationFrame(() =>
        requestAnimationFrame(() => {
          const derived = deriveKey(secret.value, space);
          secret.value = '';
          keep(KEY, derived);
          resolve(derived);
        }),
      );
    };
  });
}

// Where lines are kept between visits: one record a line, under the log it is in and how far
// into it. With no such place to keep them, everything is asked of the relay each time.
async function openStore() {
  if (!('indexedDB' in window)) return null;
  const db = await new Promise((resolve) => {
    const request = indexedDB.open(`fzn:${space}`, 1);
    request.onupgradeneeded = () => request.result.createObjectStore('lines', { keyPath: ['log', 'n'] });
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => resolve(null);
    request.onblocked = () => resolve(null);
  });
  if (!db) return null;
  // A change is a transaction, and it is done once the browser says it has completed, which is
  // when what it wrote would outlast the page.
  const write = (change) =>
    new Promise((done, failed) => {
      const tx = db.transaction('lines', 'readwrite');
      change(tx.objectStore('lines'));
      tx.oncomplete = () => done();
      tx.onabort = tx.onerror = () => failed(tx.error);
    });
  const made = {
    // Every log, each as far as it runs without a gap. What lies past a gap is asked for again.
    load: () =>
      new Promise((resolve) => {
        const request = db.transaction('lines').objectStore('lines').getAll();
        request.onerror = () => resolve(new Map());
        request.onsuccess = () => {
          const logs = new Map();
          for (const { log, n, line } of request.result) {
            const lines = logs.get(log) || logs.set(log, []).get(log);
            if (n === lines.length) lines.push(line);
          }
          resolve(logs);
        };
      }),
    keep: ({ log, from, lines }) => write((kept) => lines.forEach((line, i) => kept.put({ log, n: from + i, line }))),
    // Every line of a log. Transactions on one store run in the order they were made, so this
    // comes after whatever was kept before it was asked for.
    forget: (log) => write((kept) => kept.delete(IDBKeyRange.bound([log, 0], [log, Infinity]))),
  };
  // A later version of the page asking for the database is let have it; this page then keeps
  // nothing more, and asks the relay for it next time. What it goes on writing is then in no store,
  // so its log is not one a later page may go on with (see couldNotKeep), nor is a new id a
  // compaction gives it later kept.
  db.onversionchange = () => {
    db.close();
    if (store === made) store = null;
    unkept = true;
    forgetWriter();
  };
  return made;
}

let fzn;
let store = null;
let view = null;
let socket = null;
let wait = 1000;
let synced = 'connecting';

const line = $('line');
const list = $('list');
const pane = $('pane');
// The sheet, and in it the scroll that the body is written in and, at its foot, where vim's
// prompt for a command goes, under the text and never over it.
const buffer = $('buffer');
const sheet = $('sheet');

// An action the engine fails on is said in the foot rather than thrown inside a key's handler,
// where it would go unseen, and its cause is written to the console with its stack. A failure
// inside the WebAssembly can leave the engine unable to do anything more, so a second one says
// that the page wants loading again; the count never goes back down, since a trap leaves the
// engine as it was when it failed for good.
let failures = 0;
function act(action) {
  if (!fzn || reloading) return;
  // Typing leaves the caret where the person put it, as the engine does, so a caret it put before a
  // key typed in the same frame is not put after it.
  if (['type', 'edited'].includes(named(action))) placed = null;
  let todo;
  try {
    todo = fzn.act(JSON.stringify(action));
  } catch (cause) {
    console.error(cause);
    failures++;
    trouble(failures > 1 ? 'fzn needs reloading' : `fzn could not do that: ${cause}`);
    return;
  }
  carryOut(todo);
}

// Says in the foot what went wrong, in ink, until the engine has something else to say: it does
// not fade as what the engine says does.
function trouble(text) {
  clearTimeout(fading);
  const status = $('status');
  status.textContent = text;
  status.classList.remove('faded', 'gone');
  status.classList.add('trouble');
}

// A line this browser could not keep is said once, not at every keystroke after it, and is not
// sent. The writer id is forgotten, so that no later page goes on with this log from a store that
// lacks the line: one that did would write its own line where this one is, against the relay's,
// which is a fork. This page goes on writing the log, which its memory and the relay agree on,
// and the next page starts another, which costs one log.
let unkept = false;
function couldNotKeep(cause) {
  console.error(cause);
  if (!unkept && fzn) {
    unkept = true;
    forgetWriter();
    fzn.say('could not keep that in this browser; it will be asked of the relay again');
    look();
  }
  return false;
}
const forgetWriter = () => keep(WRITER, null, writerIn);

// Everything kept so far, settled: what a page about to be reloaded waits for.
let stored = Promise.resolve();
// The messages for peers, in the order the engine gave them, each sent once what came before it
// has been kept.
let sending = Promise.resolve();

// Does what the engine wants done, in its order: lines to keep, then logs to delete, which a
// compaction has retired, then a new writer id to keep, then messages for the relay, and the
// things it leaves to whoever is showing the notes. Keeping comes before deleting so that a page
// closed between the two still has the notes somewhere. A line is sent only once the browser has
// kept it: a page closed before then would leave the relay holding a line of this writer's log
// that the page does not, and the next page to write that log would write over it, a fork. The
// view is painted at once; only the wire waits, a few milliseconds.
function carryOut(json) {
  const todo = JSON.parse(json);
  const kept = store ? todo.keep.map((lines) => store.keep(lines).then(() => true, couldNotKeep)) : [];
  // A log not deleted is deleted the next time the page opens.
  for (const log of todo.delete) store?.forget(log).catch(() => {});
  if (todo.device && !unkept) keep(WRITER, todo.device, writerIn);
  if (kept.length) stored = Promise.all([stored, ...kept]);
  if (todo.send.length) {
    const ready = stored;
    const all = Promise.all(kept);
    sending = sending
      .then(() => ready)
      .then(() => all)
      .then((done) => done.every(Boolean) && todo.send.forEach(send))
      .catch((cause) => console.error(cause));
  }
  if (todo.copy != null) copy(todo.copy);
  if (todo.open) window.open(todo.open, '_blank', 'noopener');
  // Where the engine put the caret in the head, for the painting to put the editor's there, and in
  // the line, for the line to be given it as it is read (see look).
  if (todo.caret?.head != null) placed = todo.caret.head;
  if (todo.caret?.line != null) placedLine = todo.caret.line;
  look();
}

// Where the cursor is, as far as the focus is concerned: the line, a row's head, or a row's
// whole note.
function place(v) {
  if (!v || v.cursor == null) return '';
  return `${v.rows[v.cursor]?.id ?? ''}${v.field?.whole ? ' whole' : ''}`;
}

// Reads what there is to show. This is done at once, so that the next key is judged by where
// the cursor is now and the line holds what the engine made of it. The painting can wait for the
// next frame, except when the cursor moves from one field to another, or a body opens or closes:
// then the focus has to move before another key arrives, or that key would land in the wrong
// field.
function look() {
  const was = place(view);
  view = JSON.parse(fzn.view());
  // The line is the person's while they type. It is only set when the engine has changed it, as
  // it does on the way into a note and out of one, and when the line is written; its fades are
  // then those of what it holds.
  if (line.value !== view.line) {
    line.value = view.line;
    edges(line);
  }
  // Setting it leaves the caret at its end, where the engine puts it but for a word taken from
  // what was offered, which puts it after the word.
  if (placedLine != null) {
    const at = unitsIn(line.value, placedLine);
    if (line.selectionStart !== at || line.selectionEnd !== at) line.setSelectionRange(at, at);
    placedLine = null;
  }
  if (place(view) !== was) safely();
  else paintSoon();
  keepHistory();
  rememberSoon();
}

// The path is the browser's history: each step in, a note, a label or a body opened, is an entry,
// so that the browser's Back, a mouse's back button or a phone's back gesture steps out as Escape
// does, and the history holds no more entries than the path has steps. The entry says how deep it
// is as `depth` in its state, beside whatever else the page keeps there, which is left alone. A
// step in pushes its entry inside the gesture that made it, which is when a browser honours one; a
// step out from the page goes back through the history, and the browser says when it has, so
// nothing else is done to the history until then. Forward has nothing to go back into, since
// the page does not keep where a step out came from, so it is gone back from at once. While the
// page is hidden nothing is done, as it is being put away rather than walked, nor while it opens:
// a page reloaded where it was opens in an entry of that depth with the entries before it behind,
// and once the place is put back the history is made to match it (see start).
let depth = 0;
let opening = true;
let travelling = false;
let trips = 0;
const entry = () => history.state ?? {};
const deep = () => (view ? view.path.length + (view.field?.whole ? 1 : 0) : 0);

function keepHistory() {
  if (!view || opening || travelling || document.visibilityState !== 'visible') return;
  const want = deep();
  if (want > depth) {
    // A build given up on, and when the page was last reloaded for one, are carried into each new
    // entry, so that stepping in neither asks for that build again nor reloads within the minute.
    const kept = { ...(gaveUp && { gaveUp }), ...(reloadedAt && { at: reloadedAt }) };
    while (depth < want) history.pushState({ depth: ++depth, kept }, '');
  } else if (want < depth) {
    travelling = true;
    const trip = ++trips;
    history.go(want - depth);
    // A browser that does not go, for an entry that is not there, says nothing; the page does
    // not wait on it for good.
    setTimeout(() => {
      if (!travelling || trip !== trips) return;
      travelling = false;
      depth = entry().depth ?? 0;
      keepHistory();
    }, 1000);
  }
}

window.addEventListener('popstate', () => {
  if (!view || opening) return;
  const at = entry().depth ?? 0;
  if (travelling) {
    travelling = false;
    depth = at;
    keepHistory();
    return;
  }
  // Back: out of the body, then a step at a time, as the path's steps go, until the page is as
  // deep as the entry. A step that changes nothing ends it, rather than going round for ever.
  depth = at;
  travelling = true;
  try {
    for (let was = deep(); was > at; was = deep()) {
      act(view.field?.whole ? 'close' : 'back');
      if (deep() >= was) break;
    }
  } finally {
    travelling = false;
  }
  keepHistory();
  follow();
});
document.addEventListener('visibilitychange', () => keepHistory());

// Says something to a peer: the relay, over the socket, or another tab, over the channel.
function send({ to, text }) {
  if (to === 0) {
    if (socket?.readyState === WebSocket.OPEN) socket.send(text);
  } else if (tabOf.has(to)) {
    tabs.postMessage({ from: tab, to: tabOf.get(to), text });
  }
}

// The socket to the relay, for the space under the name the token was given with, or at the
// relay's root. The token is offered as a subprotocol, the one place a page can put a credential
// on a socket, beside `fzn`, which the relay says back. A relay lost is dialled again, waiting
// longer each time; a connection that ends within five seconds of opening, as one the relay
// refuses does, is not a reason to wait less.
let connections = 0;
function connect() {
  if (socket) return;
  const scheme = location.protocol === 'https:' ? 'wss' : 'ws';
  const path = whose ? `/1/${whose}/${space}` : `/1/${space}`;
  const opened = new WebSocket(`${scheme}://${location.host}${path}`, token ? ['fzn', `fzn.token.${token}`] : []);
  let openedAt = 0;
  socket = opened;
  opened.onopen = () => {
    openedAt = Date.now();
    synced = 'syncing';
    carryOut(fzn.connected(0));
    // Coming back to a relay that went away is a reason to think it has a new page.
    if (connections++) check();
  };
  opened.onmessage = (event) => carryOut(fzn.received(0, event.data));
  opened.onclose = () => {
    if (socket !== opened) return;
    socket = null;
    fzn.disconnected(0);
    synced = 'not syncing';
    look();
    if (openedAt && Date.now() - openedAt >= 5000) wait = 1000;
    setTimeout(connect, wait);
    wait = Math.min(wait * 2, 30000);
  };
}

async function copy(text) {
  try {
    await navigator.clipboard.writeText(text);
  } catch {
    // A page not served securely has no clipboard to ask, so the old way, in a text area that a
    // phone raises no keyboard for, and the focus goes back where it was.
    const had = document.activeElement;
    const scratch = el('textarea');
    scratch.value = text;
    scratch.className = 'scratch';
    scratch.readOnly = true;
    document.body.append(scratch);
    scratch.select();
    document.execCommand('copy');
    scratch.remove();
    if (had && had !== document.body) had.focus({ preventScroll: true });
  }
}

// A painting that fails says so in the foot, where it can be seen, rather than leaving the page
// as it was with no word of why.
function safely() {
  try {
    paint();
  } catch (cause) {
    console.error(cause);
    trouble(`fzn could not show that: ${cause}`);
  }
}

let waiting = false;
function paintSoon() {
  if (waiting) return;
  waiting = true;
  requestAnimationFrame(() => {
    waiting = false;
    safely();
  });
}

const day = (moment) => (moment ? moment.slice(0, 10) : '');

// What a note says past its head, without the blank lines between.
const bodyOf = (text) => {
  const at = text.indexOf('\n');
  return at < 0 ? '' : text.slice(at + 1).replace(/^\s*\n/, '').replace(/\s+$/, '');
};

// The page's reading of markdown and the note's editor, modules of their own, fetched as the page
// starts, beside the engine, and waited for before anything is painted, since the head of the row
// under the cursor is the editor's and the first row the cursor lands on reads as it did (see
// start). A body read is painted again when a language arrives for code it showed plain, which
// `readings` counts.
const modules = Promise.all([import('./vendor/render.js'), import('./vendor/editor.js')]);
let render = null;
let readings = 0;
const reread = () => {
  readings++;
  paintSoon();
};
// A task's head starts with its box, as the engine says it does, and the box is drawn, as a body
// read draws one, and ticks the task when it is pressed (see tick); the rest of the head reads as
// any head does. The box is the head's first three letters and the space after them, as `task`
// on the engine's row says.
const readHead = (text, task) => {
  const head = el('span', 'head');
  if (task != null) {
    const button = el('button', task ? 'tick done' : 'tick');
    button.type = 'button';
    button.tabIndex = -1;
    button.setAttribute('aria-label', task ? 'Untick' : 'Tick');
    head.append(button);
    text = text.slice(text[3] === ' ' ? 4 : 3);
  }
  head.append(render.head(text));
  return head;
};
const readBody = (text, upTo) => render.body(text, upTo);

const named = (action) => (typeof action === 'string' ? action : Object.keys(action)[0]);
// Whether the engine offers this action where the cursor is, which is when the page may ask for it.
const offered = (name) => view.hints.some((hint) => named(hint.action) === name);

// A touch screen: a coarse pointer and nothing to hover with. A field with the focus there is a
// keyboard up.
const coarse = window.matchMedia('(hover: none) and (pointer: coarse)');
const mac = /mac/i.test(navigator.userAgentData?.platform ?? navigator.platform ?? '');
const typing = () => [line, $('secret')].includes(document.activeElement) || !!editor?.hasFocus;
// The keyboard there is the person's: the page moves the focus while a field has it, and
// otherwise only for a press that asked to write, which sets this until the painting that
// follows has given the field the focus. A key pressed asks to write too: on a touch screen it is
// a keyboard joined to the device, and raises nothing. A phone shows no keyboard for a focus
// nobody asked for, leaving the page typing into nothing, or raises one under the hands; and a
// focus given inside the press's own event is the only kind for which iOS raises one.
let wantsText = false;
// Where the engine put the caret in the head since the last painting, in characters: when it, and
// not the person's typing, changed what the caret is in, by landing on a row, writing out a word,
// putting a head back, a version from elsewhere, or a body opening or closing. Otherwise the caret
// stays where it was, as far as the text still reaches.
let placed = null;
// Where the engine put the caret in the line, in characters, until the line is next read.
let placedLine = null;

// Where a count of characters, as the engine counts them, ends in a text, as the browser counts it.
const unitsIn = (text, chars) => {
  let at = 0;
  for (const ch of text) {
    if (chars-- <= 0) break;
    at += ch.length;
  }
  return at;
};
// How many characters, as the engine counts them, the first `units` of a text hold.
const charsIn = (text, units) => [...text.slice(0, units)].length;
const mayFocus = () => !coarse.matches || typing() || wantsText !== false;

// On a touch screen the page is a frame the size of the visual viewport, the part of the screen
// that the keyboard and the browser's bars leave (see style.css). Two variables say where it is
// and how tall, read once a frame when the visual viewport says it has changed, and when a field
// gains or loses the focus. A browser that shrinks its layout for the keyboard gives the numbers
// of its layout; iOS Safari, which does not, gives the part still in view. Only while a field
// has the focus are they trusted: an iOS has been seen to leave the viewport's offset where the
// keyboard put it after the keyboard has gone, which would leave the frame hanging in the air.
function frame() {
  const seen = window.visualViewport;
  const up = seen && typing();
  const root = document.documentElement.style;
  root.setProperty('--vvt', `${up ? Math.max(0, Math.round(seen.offsetTop)) : 0}px`);
  root.setProperty('--vvh', `${up ? Math.round(seen.height) : innerHeight}px`);
}
let framing = false;
function reframe() {
  if (framing) return;
  framing = true;
  requestAnimationFrame(() => {
    framing = false;
    frame();
    fits = 0;
    unscroll();
    placeCaret();
    // The head being written stays above the keyboard as it comes up.
    if (editor?.hasFocus && !editor.whole) editor.dom.scrollIntoView({ block: 'nearest' });
  });
}

// The resting list scrolls only at a phone's width. One scrolled there and then made wider, as a
// phone turned on its side is, would be held where it was with no way to scroll back, so it goes
// back to its top, where the arrows begin.
const stuck = () => getComputedStyle(list).overflowY === 'hidden';
function unscroll() {
  if (list.scrollTop && stuck()) list.scrollTop = 0;
}
window.visualViewport?.addEventListener('resize', reframe);
window.visualViewport?.addEventListener('scroll', reframe);
window.addEventListener('resize', reframe);
// The body says whether a field has the focus, which on a phone hides the note you are in, and
// the frame is read again. A field losing the focus to another is not a keyboard going down, so
// what has the focus is read once the move is over.
function typed() {
  const now = typing();
  if (document.body.classList.contains('typing') === now) return;
  document.body.classList.toggle('typing', now);
  reframe();
}
document.addEventListener('focusin', typed);
document.addEventListener('focusout', () => queueMicrotask(typed));
// Wide enough for the body to be read beside the list, as style.css lays it out there; otherwise
// it is read above the line. Only the one shown is painted.
const wide = window.matchMedia('(min-width: 75rem)');
wide.addEventListener('change', () => view && paintSoon());
const still = window.matchMedia('(prefers-reduced-motion: reduce)');

// Sets what a part of the page holds, but only when that has changed, and says whether it had.
// Buttons that are put back the same as they were would lose the focus for nothing.
function set(node, key, make) {
  if (node.dataset.shows === key) return false;
  node.dataset.shows = key;
  node.replaceChildren(...make());
  return true;
}

// Whether the note you are in runs past its lines is seen once it is laid out, so it is read only
// when what it says changes and when its box does, rather than at every key: as it is shown again
// when a phone's keyboard goes down, and as the page's width changes. Only a note cut there has its
// foot faded.
function clampHere() {
  const here = $('here');
  here.classList.toggle('clamp', here.scrollHeight > here.clientHeight);
}
new ResizeObserver(clampHere).observe($('here'));

// The rows on the page by note id.
const rows = new Map();

// The editor: the head of the row under the cursor, and the whole note while its body is open in
// the sheet. There is one, made as the page starts (see start), and it is moved into whichever row
// the cursor lands on, its state made afresh there with the caret where the engine put it, and into
// the sheet when a body opens, so that it is never replaced while the cursor stays: it keeps the
// person's caret and what they have typed. `editorIn` is the row it is in. `quiet` is set while
// the painter moves it about, so that a blur it causes is not taken for the person leaving.
let editor = null;
let editorIn = null;
let quiet = false;
// Set while the editor is being left for somewhere else, so that the painting that follows does
// not take the focus back.
let leaving = false;
let vimOn = kept('fzn.vim') === 'on';

// The letters of a text, where each starts in it and then where it ends, which is the end a caret
// can be at: a letter is what Left and Right step over, a whole grapheme, an emoji or a letter and
// its accent, and never half a surrogate pair.
const graphemes = window.Intl?.Segmenter ? new Intl.Segmenter(undefined, { granularity: 'grapheme' }) : null;
function lettersOf(text) {
  const starts = [];
  if (graphemes) for (const { index } of graphemes.segment(text)) starts.push(index);
  else for (let at = 0; at < text.length; at += text.codePointAt(at) > 0xffff ? 2 : 1) starts.push(at);
  starts.push(text.length);
  return starts;
}

const headOf = (text) => {
  const at = text.indexOf('\n');
  return at < 0 ? text : text.slice(0, at);
};

// The path's labels a row reads without, as the engine says: what its head says past what it
// shows. The head of the row under the cursor reads without them too, until the caret goes into
// them (see the editor's openHead).
const hiddenOf = (row) =>
  row?.shown != null && row.head.startsWith(row.shown) ? row.head.slice(row.shown.length) : '';

// On a head the engine is told the note with the head as typed and the rest as it was, and where
// the caret is after typing, for what is offered; with the body open, the whole of what the editor
// holds. The caret is read once the editor has finished with a change, and in a head a move while
// something is offered is said, so that it closes once the caret leaves the word.
const editorHooks = {
  onChange: (text) => {
    const field = view?.field;
    if (!field) return;
    if (field.whole) return act({ edited: text });
    const at = field.text.indexOf('\n');
    act({ edited: at < 0 ? text : `${text}\n${field.text.slice(at + 1)}` });
    sayCaret(editor.contentDOM, text, editor.selection());
  },
  onSelection: () => {
    queueMicrotask(placeCaret);
    if (!view?.offer || !editor.hasFocus || editor.whole) return;
    sayCaret(editor.contentDOM, editor.text(), editor.selection(), true);
  },
  onMode: () => queueMicrotask(caretMoved),
  onFocus: () => {
    held = editor.contentDOM;
    if (!quiet) caretMoved();
  },
  // Going to vim's prompt for a command, at the foot of the sheet, is not leaving, and leftFor
  // knows it. Nor is a blur the editor causes itself by taking its content out of the page for a
  // moment, as the vim keys do to end a composition after a dead key, or the painter does in
  // moving it to another row.
  onBlur: (event) => {
    if (held === editor.contentDOM) held = null;
    placeCaret();
    if (!event.target.isConnected) return;
    leftFor(event);
  },
  // Vim's write keeps the body as it stands and stays; its quits close it, and the one with a
  // bang first puts the note back as it was when the cursor landed on it. Each is done once vim
  // has finished with the key that asked for it, since vim goes on with the editor after its
  // command, and closing makes the editor's state afresh, vim gone from it: an Enter in vim's
  // prompt would be left to land in the head as a space.
  onEx: (what) =>
    queueMicrotask(() => {
      if (what === 'write') keepOpen();
      else {
        // Put back is asked for only when the engine offers it, since on a body that has not
        // changed undo would take back the last version written instead, which may be another
        // note's.
        if (what === 'discard' && offered('undo')) act('undo');
        act('close');
      }
      follow();
    }),
};

// Leaving an open body for somewhere else on the page closes it, writing it if it changed.
// Leaving the window does not: the body is there, still open, when the person comes back. Leaving
// a head does nothing: the field stays the cursor's, and a click on the line or a row says where
// the cursor goes.
function leftFor(event) {
  if (quiet || !view?.field?.whole) return;
  const to = event.relatedTarget;
  // On a touch screen a focus that goes nowhere is the keyboard put away, and the body stays open
  // to be read; `done`, or the back gesture, closes it.
  if (coarse.matches && !to) return;
  // Anywhere in the sheet is still in it.
  if (to?.closest?.('#buffer')) return;
  if (!(to ? document.body.contains(to) : document.hasFocus())) return;
  leaving = true;
  try {
    act('close');
  } finally {
    leaving = false;
  }
}

// The keys of a head, heard before the editor hears them. Those that leave the row or act on the
// note as a whole are the session's: Up, Down and the pages move the cursor and Control with an
// arrow the note, Tab goes in from anywhere in the head, Enter writes, Shift-Enter ticks the task
// or archives the note, as Ctrl-V does in the terminal, Escape leaves the row, Backspace on an
// empty head takes the row away, undo and redo are the engine's, which puts a head back as it was
// when the cursor landed, and so are the chords. Shift-Tab is nothing: the focus never leaves the
// field by a key. The rest, the letters and the keys that move the caret or select, are the
// editor's, as in any one-line field.
function keyInHead(event) {
  if (event.isComposing || !view?.field || editor.whole) return;
  if (onOffer(event, true)) return event.stopPropagation();
  const plain = !event.shiftKey && !event.ctrlKey && !event.metaKey && !event.altKey;
  switch (escapes(event) ? 'Escape' : event.key) {
    case 'ArrowDown':
    case 'ArrowUp':
    case 'PageDown':
    case 'PageUp':
      if (ctrlOnly(event) && event.key.startsWith('Arrow')) act({ shift: event.key === 'ArrowUp' ? -1 : 1 });
      else if (plain) act({ move: { ArrowDown: 1, ArrowUp: -1, PageDown: 10, PageUp: -10 }[event.key] });
      else return;
      break;
    case 'Tab':
      if (event.ctrlKey || event.metaKey || event.altKey) return;
      if (!event.shiftKey) act('go_in');
      break;
    case 'Enter':
      if (event.ctrlKey || event.metaKey || event.altKey) return;
      act(event.shiftKey ? 'archive' : 'write');
      break;
    // Backspace on an empty head takes the row away and steps up, as it would an empty line. A new
    // row with only the labels it opened with is empty while the caret is where it landed, after a
    // task's box and before the rest.
    case 'Backspace':
      if (!plain || !atEmpty()) return;
      act('erase');
      break;
    case 'Escape':
      act({ select: null });
      break;
    // An editor's undo, Ctrl-Z or on a Mac Cmd-Z, and with Shift its redo.
    case 'z':
    case 'Z':
      if (event.altKey || !(mac ? event.metaKey && !event.ctrlKey : event.ctrlKey && !event.metaKey)) return;
      act(event.shiftKey ? 'redo' : 'undo');
      break;
    default:
      if (!ctrlOnly(event) || !chord(event.key, true)) return;
  }
  event.preventDefault();
  event.stopPropagation();
  follow();
}

// An open body is the editor's but for the chords that never reach the browser, from the text or
// from vim's prompt, the editor having them first where it binds them, Ctrl-U among them (see
// swallowedInBody), and Escape or Ctrl-[, which close it without vim's keys. With them Escape is
// vim's alone, since hands that press it twice to be sure of normal mode would close the note
// under themselves; `:q`, `ZZ` and the word in the foot close it.
buffer.addEventListener('keydown', (event) => {
  if (event.isComposing || editor?.composing || !view?.field?.whole) return;
  if (swallowedInBody(event)) return event.preventDefault();
  if (!escapes(event) || event.defaultPrevented || editor.vimOn) return;
  act('close');
  event.preventDefault();
});

// The chords an open body never lets reach the browser, though the editor may act on them first:
// Ctrl-O and Ctrl-U, the file dialog and the page source, and away from a Mac Ctrl-H and Ctrl-K,
// history and search. On a Mac those two are the text's own Emacs keys, delete back and kill, and
// the browser binds them to nothing, so they are left to it. Ctrl-Y is the editor's, vim's scroll.
const swallowedInBody = (event) =>
  ctrlOnly(event) && (/^[ou]$/i.test(event.key) || (!mac && /^[hk]$/i.test(event.key)));

// Whether a key is Escape: Escape itself, or Ctrl-[, which a terminal reads as Escape.
const escapes = (event) => event.key === 'Escape' || (ctrlOnly(event) && event.key === '[');

// Whether the caret is where Backspace takes an empty head away.
function atEmpty() {
  const at = view.field.fresh ?? (editor.text() === '' ? 0 : -1);
  const { anchor, head } = editor.selection();
  return anchor === at && head === at;
}

// The chords with Control alone that ask for something of the note under the cursor, or of the
// line, and say whether one did. They are the terminal's, where a browser lets them go: Ctrl-D
// deletes, or clears the line; Ctrl-O opens the body, and on the line in a note the body of the
// note you are in; Ctrl-U undoes, which costs only the browser's page source; Ctrl-C copies the
// note's address or its head, but only while nothing is selected, so that a selection is still
// copied as anywhere else; Ctrl-/ opens the note's address, which no browser binds. Ctrl-R, Ctrl-L
// and Ctrl-V are a browser's reload, address bar and paste, so redo, link and showing what is
// archived take keys a browser lets a page have: Ctrl-Y redoes, Ctrl-K links or unlinks the note,
// as it makes a link in most editors, and Ctrl-H shows or hides what is archived, as it shows what
// is hidden in a file chooser. Each is swallowed where it does nothing, as Ctrl-O is on the line
// outside a note, so that the browser's search and history never open from the page. The table is
// what each letter asks for of a row, and the foot writes a word's chord from it (see keyFor), so
// that the letter written after a word is the letter that asks for it.
const CHORDS = {
  d: 'delete',
  o: 'open',
  u: 'undo',
  y: 'redo',
  k: 'link',
  h: 'show_archived',
  c: 'copy',
  '/': 'open_url',
};
function chord(key, onRow) {
  const letter = key.toLowerCase();
  const name = Object.hasOwn(CHORDS, letter) ? CHORDS[letter] : undefined;
  switch (name) {
    case undefined:
      return false;
    case 'delete':
      act(onRow ? 'delete' : { type: '' });
      return true;
    case 'undo':
    case 'redo':
      act(name);
      return true;
    case 'show_archived':
    case 'open':
      if (offered(name)) act(name);
      return true;
    case 'copy':
      if (!onRow || !nothingSelected()) return false;
      act(name);
      return true;
    default:
      if (onRow) act(name);
      return true;
  }
}

// The key written after a word in the foot: the one that asks for the same where the cursor is,
// in the head (see keyInHead), anywhere else (see pressed) or as a chord, named as the
// terminal names its keys and in the same marks, `↵` and `⇧↵` as most keycaps print them. Every
// word a key asks for has it, Enter, Tab and Escape as much as the chords, so that no word is left
// to be guessed at. In an open body the chords are the editor's, so only the way out has a key:
// Escape, or vim's quit where Escape is vim's alone.
function keyFor(action) {
  const name = named(action);
  if (view?.field?.whole) return name === 'close' ? (editor?.vimOn ? ':q' : 'Esc') : '';
  switch (name) {
    case 'write':
      return '↵';
    case 'archive':
      return '⇧↵';
    case 'go_in':
    case 'take':
      return 'Tab';
    case 'choose':
      return '↑↓';
    // Escape's rungs, off the row, off the text on the line and out of the place, each a word as
    // the way back, and closing what is offered; clearing the line is its cross as well.
    case 'dismiss':
    case 'select':
    case 'type':
    case 'back':
      return 'Esc';
    // On a Mac Control with an arrow is the system's, so moving a note is its words alone there.
    case 'shift':
      return mac ? '' : action.shift < 0 ? '^↑' : '^↓';
  }
  const letter = Object.keys(CHORDS).find((key) => CHORDS[key] === name);
  return letter ? `^${letter.toUpperCase()}` : '';
}

function nothingSelected() {
  if (editor?.hasFocus) return editor.selection().anchor === editor.selection().head;
  if (document.activeElement === line) return line.selectionStart === line.selectionEnd;
  return getSelection()?.type !== 'Range';
}

// The focus goes where the cursor is: the editor when it is on a row, the line when it is not.
function follow() {
  if (!view || !mayFocus()) return;
  if (view.cursor != null && editorIn) {
    if (!editor.hasFocus) editor.focus();
    return;
  }
  if (document.activeElement !== line) line.focus({ preventScroll: true });
}

const soon = (task, timeout) =>
  window.requestIdleCallback ? requestIdleCallback(() => task(), timeout && { timeout }) : setTimeout(task, 1500);

// Writes an open body as it stands and leaves it open, for vim's `:w`: the engine keeps the field
// and stays, so the editor is left as it is, its caret, scroll and step undo with it.
function keepOpen() {
  if (view?.field?.whole) act('keep');
}

function rowKey(row, selected, apart) {
  const field = selected ? view.field : null;
  return JSON.stringify([
    // The head of the row under the cursor is the editor's, so it is left out here: a row is not
    // filled again, taking the editor out and putting it back, for a letter typed into it. With
    // the body open in the sheet the row reads its head as it is being written. Nor is its date in
    // the key, which a new note's first letter gives it: the date is set in place, since filling
    // the row would take the editor out under a composition the first letter began.
    field && !field.whole ? null : field ? headOf(field.text) : row.shown ?? row.head,
    row.task, row.links, row.reasons, row.archived, row.has_body, apart, selected,
    view.scores_shown ? row.score : null,
  ]);
}

// Sets the date at the end of the selected row's head in place, making it or taking it away.
function dateRow(li) {
  const main = li.querySelector(':scope > .main');
  if (!main) return;
  let time = main.querySelector(':scope > time');
  // The day the note was written, which is the day it belongs to.
  const created = view.field?.created;
  if (!created) return time?.remove();
  const text = day(created);
  if (!time) {
    time = el('time', null, text);
    main.insertBefore(time, main.querySelector(':scope > .chev'));
  } else if (time.textContent !== text) time.textContent = text;
}

function fill(li, row, selected) {
  const field = selected ? view.field : null;
  const main = el('div', 'main');
  if (view.scores_shown && row.score != null) main.append(el('span', 'score', row.score.toFixed(2)));
  // Off the cursor a row reads as the engine shows it, without the path's labels at its end. On
  // it the head is the editor's, held in a frame of its own, which reads it the same way.
  if (field && !field.whole) {
    const edit = el('div', 'edit');
    edit.append(editor.dom);
    main.append(edit);
  } else main.append(readHead(field ? headOf(field.text) : row.shown ?? row.head, row.task));
  const tail = el('span', 'tail');
  if (row.archived) tail.append(el('span', null, 'archived'));
  if (row.has_body) tail.append(el('span', null, '¶'));
  if (row.links.length) tail.append(el('span', null, `→ ${row.links.join(', ')}`));
  if (row.reasons.length) tail.append(el('span', null, row.reasons.join(' · ')));
  main.append(tail);
  // The tail is not there on the cursor, so a body is said by its mark before the date, which is
  // set before the arrow (see dateRow).
  if (selected && row.has_body) main.append(el('span', 'mark', '¶'));
  if (selected) {
    const chev = el('button', 'chev', '›');
    chev.type = 'button';
    chev.tabIndex = -1;
    chev.setAttribute('aria-label', 'Go in');
    main.append(chev);
  }
  li.replaceChildren(main);
}

// The rows made are those that can be seen and twenty more, and more as the list is scrolled
// or walked towards its end: the engine lists up to 500, and a list cleared made all of them anew,
// with their style and layout, in 40 ms or more, four frames for one key. How many fit is read once
// and again only when the window's size changes, from the list's height and a row's, the `--row`
// of the stylesheet, 2.5rem, or a phone's, a little more, which makes a few rows more than fit. A
// new list, for another line or another place, starts again from what fits. The row under the
// cursor is always made, so the list can follow it.
const MARGIN = 20;
let fits = 0;
let further = 0;
let made = 0;
let listFor = '';
// The note you are in while its body is open from the line, which the list does not follow.
let hereFromLine = '';
let listed = new Set();
const rowHeight = () => 2.5 * parseFloat(getComputedStyle(document.documentElement).fontSize);
function reach() {
  if (!fits) fits = Math.ceil((list.clientHeight || innerHeight) / rowHeight());
  return Math.max(fits + MARGIN, (view.cursor ?? 0) + MARGIN, further);
}
list.addEventListener(
  'scroll',
  () => {
    if (!view || made >= view.rows.length) return;
    if (list.scrollTop + list.clientHeight < list.scrollHeight - 2 * rowHeight()) return;
    further = made + 50;
    paintSoon();
  },
  { passive: true },
);

// The focus follows the cursor at the end of every painting: no key moves it off a field, so
// wherever else it is, a press put it there and the field takes it back. Only a field being left
// for somewhere else keeps its hands off, and vim's prompt for a command, at the sheet's foot, which
// is still in the body, and text the person has selected outside the fields, in the body beside the
// list or above the line, to copy it: a painting for a line from the relay must not take that away.
// The next key typed brings the focus back. On a touch screen the focus is taken only while a field
// had it as the painting began, or for a press that asked to write (see wantsText).
const reading = () => document.activeElement === document.body && getSelection()?.type === 'Range';
let wasTyping = false;
const takesFocus = (field) =>
  (!coarse.matches || wasTyping || wantsText !== false) &&
  !holdFocus &&
  !leaving &&
  !reading() &&
  document.activeElement !== field &&
  !$('sheet-foot').contains(document.activeElement);

function paintRows() {
  const field = view.field;
  const fieldId = view.cursor != null && field ? view.rows[view.cursor]?.id ?? null : null;
  const wasWhole = editor.whole;
  wasTyping = typing();
  quiet = true;

  // The cursor has left the row the editor was in: that row goes back to being read.
  const landed = fieldId !== editorIn;
  if (landed && editorIn) {
    const left = rows.get(editorIn);
    if (left) left.dataset.k = '';
    editor.dom.remove();
  }
  editorIn = fieldId;

  const asking = JSON.stringify([view.line, view.path]);
  const anew = asking !== listFor;
  if (anew) {
    listFor = asking;
    further = 0;
  }
  const shown = view.rows.slice(0, reach());
  made = shown.length;
  const seen = new Set(shown.map((row) => row.id));
  for (const [id, li] of rows) {
    if (!seen.has(id)) {
      li.remove();
      rows.delete(id);
    }
  }

  let newcomers = 0;
  const wanted = shown.map((row, i) => {
    let li = rows.get(row.id);
    if (!li) {
      li = el('li', 'row');
      li.dataset.id = row.id;
      li.dataset.k = '';
      rows.set(row.id, li);
      // Only a row that is new to the list fades in, not one that was listed and is made now
      // because the list was scrolled or walked down to it.
      if (!still.matches && !field?.whole && !listed.has(row.id)) {
        li.classList.add('in');
        li.style.setProperty('--i', Math.min(newcomers++, 8));
      }
    }
    const selected = i === view.cursor;
    // The row under the cursor is the list's current one, to the eye and to a screen reader alike;
    // its head is a text field, which an option of a listbox could not hold.
    if (selected && !li.hasAttribute('aria-current')) li.setAttribute('aria-current', 'true');
    else if (!selected && li.hasAttribute('aria-current')) li.removeAttribute('aria-current');
    // In a place, what the line finds in it is set apart from what it only finds there, where the
    // engine says the finds begin.
    const apart = i === view.finds_from;
    li.classList.toggle('apart', apart);
    li.classList.toggle('archived', row.archived);
    // How deep the row is drawn, which the stylesheet turns into its indent.
    if (li.dataset.depth !== String(row.depth)) {
      li.dataset.depth = row.depth;
      li.style.setProperty('--depth', row.depth);
    }
    // A new note with nothing in it has nowhere to go in to, so its row has no arrow.
    li.classList.toggle('blank', selected && field?.fresh != null);
    const k = rowKey(row, selected, apart);
    if (li.dataset.k !== k) {
      li.dataset.k = k;
      fill(li, row, selected);
    }
    if (selected) dateRow(li);
    return li;
  });

  listed = new Set(view.rows.map((row) => row.id));

  // Rows are put in order by moving only those that are out of it. A node that is moved is taken
  // out and put back, which would restart its fade and drop the focus of the editor.
  let at = list.firstChild;
  for (const li of wanted) {
    if (li === at) at = at.nextSibling;
    else list.insertBefore(li, at);
  }

  // An open body is the sheet's, and a head the row's, where fill put the editor; with the cursor
  // on the line it is nowhere.
  if (field && editorIn && field.whole && editor.dom.parentElement !== sheet) sheet.prepend(editor.dom);
  else if (!(field && editorIn) && editor.dom.isConnected) editor.dom.remove();

  // The editor holds what the field says. Landing on a row, and a body opening or closing, make
  // its state afresh, with the caret where the engine put it, which is the end of the head, so that
  // Down in a body goes to the second line, and in a new row with only the labels it opened with is
  // after a task's box and before the rest; a row landed on reads from its start, whatever the row
  // before was scrolled to. Otherwise it is told the text only when the engine's differs from it,
  // put back, a date word expanded, a version from elsewhere, and the caret only when the engine
  // put it, so that the person's caret is left alone while they type.
  if (field && editorIn) {
    const text = field.whole ? field.text : headOf(field.text);
    const to = placed != null ? unitsIn(text, placed) : null;
    if (field.whole) {
      // A newline just made at the end of a body is not a difference, as it is not one to the
      // engine, which keeps the note without it: told the kept text, the editor would take the new
      // line away under the hands.
      const kept = (body) => body.replace(/[\r\n]+$/, '');
      if (!wasWhole || landed) editor.openBody(text, to ?? headOf(text).length);
      else if (kept(editor.text()) !== kept(text)) editor.setText(text);
    } else if (landed || wasWhole) {
      editor.scrollDOM.scrollLeft = 0;
      editor.openHead(text, to ?? text.length, hiddenOf(view.rows[view.cursor]));
      headEdges();
    } else if (editor.text() !== text || to != null) editor.setText(text, to);
    if (takesFocus(editor.contentDOM)) editor.focus();
  } else if (takesFocus(line)) {
    line.focus({ preventScroll: true });
  }
  wantsText = false;
  placed = null;
  quiet = false;

  // The list follows the cursor when the cursor moves, and not otherwise: a note arriving from
  // elsewhere, or a head being written, must not pull the list from where it was scrolled to. A new
  // list with the cursor on the line starts at its top, though the list before it was scrolled, as
  // a phone's resting list can be. The note you are in, opened from the line, is the one move that
  // leaves the list alone both ways: its body is read in the sheet over the list, and the row put
  // under the cursor for it goes again as the cursor comes back to the line, where the list is as
  // the line left it, the browser keeping what was in sight in sight as the row comes and goes.
  const under = view.cursor != null ? view.rows[view.cursor]?.id ?? '' : '';
  const was = list.dataset.under;
  if (under !== was) {
    const back = !under && was === hereFromLine && !anew;
    hereFromLine = !was && field?.whole && under === view.here?.id ? under : '';
    if (!hereFromLine && !back) {
      if (under) rows.get(under)?.scrollIntoView({ block: 'nearest', inline: 'nearest' });
      else list.scrollTop = 0;
    }
  } else if (!under && anew) list.scrollTop = 0;
  list.dataset.under = under;
}

// The words in the foot: what can be asked for where the cursor is. Moving has the arrows and a
// tap on a row, and the editor is the row itself, so neither gets a word. On the line, going in is
// the arrow after it, so it has none either. Clearing the line has its cross, and a word too, since
// it is Escape's next rung and the foot always ends with that, as the terminal's footer does.
function paintHints() {
  const unsaid = view.cursor == null ? ['move', 'editor', 'go_in'] : ['move', 'editor'];
  const hints = view.hints.filter((hint) => !unsaid.includes(named(hint.action)));
  // Vim's keys are this browser's to ask for, and only where they apply: in an open body, once
  // the editor is there to have them.
  const vimWord = view.field?.whole && editor ? (vimOn ? 'no vim' : 'vim') : null;
  // The way back, Escape's next rung, which the engine offers last, follows the words and stays in
  // sight when they do not fit, the rest scrolling; on a phone it is pinned at the right of the
  // bar, where a thumb finds it without reading, so that a way out of a place several steps deep
  // is one spot pressed again and again, clearing the line and then stepping out.
  const last = hints.length ? named(hints[hints.length - 1].action) : '';
  const way = ['back', 'select', 'type', 'close', 'dismiss'].includes(last) ? hints.pop() : null;
  // What a word's key is can change while the words do not, as vim's keys come and go.
  const said = (hint) => [hint, keyFor(hint.action)];
  set($('way'), JSON.stringify(way && said(way)), () => (way ? [word(way)] : []));
  const words = $('hints');
  const changed = set(words, JSON.stringify([hints.map(said), vimWord]), () => {
    const words = hints.map((hint) => word(hint));
    if (vimWord) {
      const button = el('button', 'word vim', vimWord);
      button.type = 'button';
      button.onclick = () => {
        vimOn = !vimOn;
        keep('fzn.vim', vimOn ? 'on' : null);
        editor.setVim(vimOn);
        paintSoon();
        follow();
      };
      words.push(button);
    }
    return words;
  });
  // New words, and the words of another row, start from the left, and a fade at each edge says
  // when there are more words past it.
  const under = view.cursor != null ? (view.rows[view.cursor]?.id ?? '') : '';
  const moved = words.dataset.under !== under;
  words.dataset.under = under;
  if (changed || moved) {
    words.scrollLeft = 0;
    hintEdges();
  }
}

const hintsNode = $('hints');
// A few pixels past the edge, a word's own room, is not more words.
function hintEdges() {
  const over = hintsNode.scrollWidth - hintsNode.clientWidth;
  hintsNode.classList.toggle('before', over > 4 && hintsNode.scrollLeft > 1);
  hintsNode.classList.toggle('after', over > 4 && hintsNode.scrollLeft < over - 1);
}
hintsNode.addEventListener('scroll', hintEdges, { passive: true });
// A wheel turned over words that do not fit scrolls them sideways, since a mouse has no other way
// to; a wheel over words that fit is left to the page.
hintsNode.addEventListener('wheel', (event) => {
  const over = hintsNode.scrollWidth - hintsNode.clientWidth;
  if (over <= 1 || event.ctrlKey || Math.abs(event.deltaY) <= Math.abs(event.deltaX)) return;
  const by = event.deltaMode === 1 ? event.deltaY * 16 : event.deltaMode === 2 ? event.deltaY * over : event.deltaY;
  const to = Math.max(0, Math.min(over, hintsNode.scrollLeft + by));
  if (to === hintsNode.scrollLeft) return;
  hintsNode.scrollLeft = to;
  event.preventDefault();
});
window.addEventListener('resize', () => view && hintEdges());

// A word in the foot, with the key that asks for the same written after it, which the stylesheet
// shows only where there is a pointer to read it with. A word does what its key does, after which
// the focus follows the cursor. A new note is asked for to be written in, so the keyboard comes for
// it.
function word(hint) {
  const name = named(hint.action);
  const button = el('button', 'word', hint.label);
  button.type = 'button';
  const key = keyFor(hint.action);
  if (key) {
    const written = el('span', 'key', key);
    written.setAttribute('aria-hidden', 'true');
    button.append(written);
  }
  button.onclick = () => {
    try {
      if (name === 'write') wantsText = true;
      act(hint.action);
    } finally {
      follow();
      wantsText = false;
    }
  };
  return button;
}

let status = '';
let fading = 0;
// A status fades, and once it has faded it is gone, so that what is said, floating above the words,
// is drawn only while something can be read there. Without motion there is no fade to wait for.
function fade(node) {
  node.classList.add('faded');
  if (still.matches) node.classList.add('gone');
}
$('status').addEventListener('transitionend', (event) => {
  if (event.propertyName === 'opacity' && event.target.classList.contains('faded')) event.target.classList.add('gone');
});
function paintFoot() {
  // What just happened is said, and goes after five seconds. What is still true stays: that the
  // relay cannot be reached, or that this device's clock is far from the relay's, and that the
  // key opens nothing here. A refusal from the relay is still true until a relay takes the tab
  // in, so it is said in ink while nothing else is, and does not go.
  const refused = fzn.refused();
  const says = view.status || refused || '';
  if (says !== status) {
    status = says;
    const node = $('status');
    node.textContent = status;
    node.classList.remove('faded', 'gone');
    node.classList.toggle('trouble', !!refused && status === refused);
    clearTimeout(fading);
    if (status && status !== refused) fading = setTimeout(() => fade(node), 5000);
  }
  // A passphrase in doubt, as the engine judges it, says so with a way to type it again: a key
  // that opens nothing here, or one the relay refused that nothing here has proved. A key nothing
  // has yet proved, in a space with no notes, has the word to type it again; once a note has been
  // opened with it there is nothing to retype, and the word goes.
  const doubt = fzn.key_in_doubt();
  $('trouble').hidden = !doubt;
  $('passphrase').hidden = doubt || view.notes > 0;
  const sync = $('sync');
  const said = synced === 'not syncing' ? synced : (fzn.clock() ?? '');
  if (sync.textContent !== said) sync.textContent = said;
  sync.hidden = !said;
}

const readStep = (text) => {
  const read = render.head(text);
  if (typeof read !== 'string') for (const a of read.querySelectorAll('a')) a.replaceWith(...a.childNodes);
  return read;
};

function paintPath() {
  const path = $('path');
  const zoomed = view.path.length > 0;
  if (zoomed && path.hidden && !still.matches) {
    path.classList.add('in');
    path.addEventListener('animationend', () => path.classList.remove('in'), { once: true });
  }
  path.hidden = !zoomed;
  // Each step back along the path is a way back to it, and the first, the space's name, is the way
  // to the top: where you are is inside the space, and the app's name says nothing about that. A
  // note's step reads as its row does, its marks rendered and its heading's mark dropped, and a
  // link in it reads as its text, since a step is a button and a link inside one would be a second
  // thing to press. Every step is shown while they fit (see fitPath).
  const made = set(path, JSON.stringify(view.path), () => {
    const steps = [space, ...view.path];
    const parts = [];
    steps.forEach((head, depth) => {
      if (depth > 0) parts.push(el('span', null, '›'));
      if (depth === 1) parts.push(el('span', 'gone', '…'), el('span', 'gone', '›'));
      const step = el('button');
      step.append(depth > 0 ? readStep(head) : head);
      step.type = 'button';
      step.dataset.depth = depth;
      if (depth === steps.length - 1) step.setAttribute('aria-current', 'location');
      parts.push(step);
    });
    return parts;
  });
  if (made) fitPath();
}

// No step is wider than 22rem, a longer one cut at that. A path too long for its box gives up
// its middle, the oldest step after the space first, into one `…`, as the terminal's does; what
// still does not fit, the space and the last step share the cut of. Since a note written here
// carries every label on the path, a step is hidden only when there is no room for it.
function fitPath() {
  const path = $('path');
  if (path.hidden) return;
  const parts = [...path.children];
  const steps = parts.filter((part) => part.dataset.depth > 0);
  const middle = steps.slice(0, -1).map((step) => [step, step.nextElementSibling]);
  const ellipsis = parts.slice(2, 4);
  const ends = [parts[0], steps.at(-1)];
  for (const part of parts) part.classList.remove('gone');
  for (const end of ends) end.style.maxWidth = '';
  ellipsis.forEach((part) => part.classList.add('gone'));
  // Measured with no step shrinking, which is what overflowing is.
  path.classList.add('measure');
  const over = () => path.scrollWidth > path.clientWidth;
  for (let i = 0; i < middle.length && over(); i++) {
    ellipsis.forEach((part) => part.classList.remove('gone'));
    middle[i].forEach((part) => part.classList.add('gone'));
  }
  // What still does not fit, the space and the last step share as the terminal's do: the shorter
  // keeps its width, up to half the room, and the longer has the rest, so that a long space name
  // cannot squeeze where you are to nothing.
  if (over()) {
    const widths = ends.map((end) => end.getBoundingClientRect().width);
    const room = Math.floor(path.clientWidth - (path.scrollWidth - widths[0] - widths[1]));
    const short = widths[0] <= widths[1] ? 0 : 1;
    const kept = Math.min(widths[short], Math.floor(room / 2));
    ends[short].style.maxWidth = `${kept}px`;
    ends[1 - short].style.maxWidth = `${Math.max(0, room - kept)}px`;
  }
  path.classList.remove('measure');
}
window.addEventListener('resize', () => view && fitPath());

// A body read, beside the list or above the line, is painted when it has changed, and says
// whether it had. A long one is rendered at first only as far as either shows, and whole once the
// page is idle, so that the arrows walk onto a long document without waiting for all of it to be
// laid out.
const GLANCE = 5000;
function paintBody(node, says) {
  const key = `${readings} ${says}`;
  if (!set(node, key, () => [readBody(says, GLANCE)])) return false;
  if (says.length > GLANCE) {
    soon(() => {
      if (node.dataset.shows === key) node.replaceChildren(readBody(says));
    });
  }
  return true;
}

// The body of what the cursor is on, beside the list, which the page shows only when it is wide
// enough to have room for it: the note under the cursor as it is being written, and on the line
// the note you are in, which is then not read above the line as well. A note landed on is read
// from its top. With the body open the sheet has it, and the pane says nothing. Above the body it
// reads the whole of a head its row cuts, as the row would read it uncut, and as it is typed once
// it is written in; a head the row shows whole is not read twice. The two are parts of their own,
// so that a letter typed in a long head reads the head again and never the body.
const paneHead = $('pane-head');
const paneBody = $('pane-body');
function paneSays() {
  const field = view.field;
  if (!wide.matches || field?.whole) return '';
  return field ? bodyOf(field.text) : view.here ? bodyOf(view.here.text) : '';
}
// The pane reads the note you are in while the cursor is on the line and it has something to read,
// and only then does a click on it open that note (see openHere).
const paneReadsHere = (says = paneSays()) => view.cursor == null && says !== '';
function paintPane() {
  const says = paneSays();
  paintBody(paneBody, says);
  pane.classList.toggle('here', paneReadsHere(says));
  paintPaneHead();
  const under = (view.cursor != null ? view.rows[view.cursor]?.id : view.here?.id) ?? '';
  if (pane.dataset.under !== under) {
    pane.dataset.under = under;
    pane.scrollTop = 0;
  }
}

// The head as its row reads it, as it is typed: its box drawn while it begins with one, as the
// engine reads a box, and the path's labels left off while they end it.
function paintPaneHead() {
  const field = view.field;
  const scroll = editor.scrollDOM;
  const cut = wide.matches && field && !field.whole && editorIn && scroll.scrollWidth > scroll.clientWidth + 1;
  paneHead.hidden = !cut;
  if (!cut) return set(paneHead, '', () => []);
  const text = editor.reads();
  const task = /^\[[ x]\]/.test(text) ? text[1] === 'x' : null;
  set(paneHead, JSON.stringify([text, task]), () => [readWhole(text, task)]);
}

// A head read whole, its marks rendered and a task's box drawn, which is never pressed.
function readWhole(text, task) {
  const head = el('div');
  if (task != null) {
    const drawn = el('span', task ? 'tick done' : 'tick');
    drawn.setAttribute('aria-hidden', 'true');
    head.append(drawn);
    text = text.slice(text[3] === ' ' ? 4 : 3);
  }
  head.append(render.head(text));
  return head;
}

function paint() {
  if (!view) return;
  const zoomed = view.path.length > 0;
  const whole = !!view.field?.whole;
  const body = document.body.classList;
  body.toggle('resting', view.line === '' && view.cursor == null && !zoomed);
  body.toggle('zoomed', zoomed);
  // The sheet is shown before the rows are painted, which put the focus in it.
  const wasWhole = !buffer.hidden;
  buffer.hidden = !whole;

  paintPath();
  $('clear').hidden = view.line === '';
  $('go-in').hidden = !(view.cursor == null && offered('go_in'));
  $('dates').textContent = view.dates.length ? `→ ${view.dates.join(', ')}` : '';

  // The note you are in, when it says more than its head and the page has no room for it beside
  // the list, clamped to six lines; a tap opens it, which reads the rest. While the sheet holds it,
  // it is not read twice, but keeps its room, so that nothing moves as the sheet comes and goes.
  const here = $('here');
  const told = view.here && !wide.matches ? bodyOf(view.here.text) : '';
  here.hidden = !told;
  here.classList.toggle('opened', whole && view.rows[view.cursor]?.id === view.here?.id);
  const into = view.here?.id ?? '';
  const arrived = here.dataset.into !== into;
  if (arrived) here.dataset.into = into;
  if (paintBody(here, told) || arrived) clampHere();

  $('empty').hidden = !(view.notes === 0 && !zoomed && view.line === '' && view.cursor == null);

  paintRows();
  paintPane();
  // A body opened is read from its first line.
  if (whole && !wasWhole) sheet.scrollTop = 0;
  paintHints();
  paintFoot();
  paintOffer();
  caretMoved();
}

line.addEventListener('input', () => {
  act({ type: line.value });
  sayCaret(line, line.value, lineCaret());
});

// A click on the line takes the cursor there, so that the engine's cursor follows the focus. An
// open body has already been closed by then, by its blur.
line.addEventListener('focus', () => {
  if (!quiet && view?.cursor != null) act({ select: null });
});

// The caret is drawn by the page rather than left to the browser, whose thin blinking line is the
// one thing on the page that looks like a control. A soft block over the letter reads as part of
// the text, and it is a shape that can be given to someone else: another person's caret, one day,
// would be the same block in another tint.
//
// In the editor, a head's or a body's, it is drawn from the editor's coordinates. On the line it
// is measured in a copy of the field, never seen: the text up to the caret, the letter at the caret
// in a span, and the rest. That is one read of the layout, made here and nowhere else, so the
// painting of the rows stays free of it.
const caret = el('div');
caret.id = 'caret';
caret.hidden = true;
const mirror = el('div');
mirror.id = 'mirror';
mirror.setAttribute('aria-hidden', 'true');
document.body.append(mirror);
const COPIED = [
  'fontFamily', 'fontSize', 'fontWeight', 'fontStyle', 'fontStretch', 'fontFeatureSettings',
  'fontVariationSettings', 'lineHeight', 'letterSpacing', 'wordSpacing', 'textTransform', 'tabSize',
  'paddingTop', 'paddingRight', 'paddingBottom', 'paddingLeft',
];
// The field that has the focus, as its own focus and blur say. The active element alone does not
// tell a field in a window that has lost the focus from one that has it.
let held = null;
let composing = false;
let caretWas = '';
// What the line's caret was last measured for. The block is a child of the field's own parent;
// it is measured again only when something in the field has changed: its text, its selection, its
// weight, or the dates that take width from the line. An event that changes none of it, such as
// the selection set again by the painter, is not measured twice. The editor's is placed again at
// every change it makes, the reading's included, since a mark opening or closing moves the letters
// round the caret.
let measured = null;
const inEditor = (field) => !!editor && field === editor.contentDOM;
// On a touch screen the caret is drawn where the cursor is whether or not the keyboard is up, still
// rather than breathing, so that the blank page has its cursor before a finger asks for the
// keyboard: in the head when the cursor is on a row, and on the line otherwise. A field without the
// focus keeps the selection last set in it, which the painter sets as the cursor lands.
const restingField = () =>
  coarse.matches && view && !view.field?.whole && !document.body.classList.contains('locked')
    ? (view.cursor != null && editorIn ? editor.contentDOM : line)
    : null;
const caretField = () => (held && held === document.activeElement ? held : restingField());
const asMeasured = () => {
  const field = caretField();
  return (
    field &&
    !inEditor(field) &&
    [field, field.selectionStart, field.selectionEnd, field.value, faceOf(field), $('dates').textContent,
      view?.cursor, composing, field === document.activeElement, view?.offer?.from].join('\u0000')
  );
};
const caretMoved = () => {
  if (inEditor(caretField()) || asMeasured() !== measured) placeCaret();
};

// The mirror is given the field's face, when the field, its class or the window's width has
// changed.
function dress(field) {
  const style = getComputedStyle(field);
  const face = `${field.id} ${faceOf(field)} ${innerWidth}`;
  if (mirror.dataset.face !== face) {
    mirror.dataset.face = face;
    for (const name of COPIED) mirror.style[name] = style[name];
    mirror.style.whiteSpace = 'pre';
  }
  return style;
}
// A field's class but for the fades at its edges, which change nothing that is measured.
const faceOf = (field) => field.className.replace(/ (before|after)\b/g, '');

// On a touch screen the passphrase's field waits for a tap with the caret resting at its start, so
// that the page asking for it is not a hairline under a grey sentence.
const secret = $('secret');
const restsOnSecret = () =>
  coarse.matches &&
  !$('unlock').hidden &&
  !secret.disabled &&
  secret.value === '' &&
  document.activeElement !== secret;

function placeCaret() {
  if (restsOnSecret()) {
    const style = getComputedStyle(secret);
    const size = parseFloat(style.fontSize);
    const tall = parseFloat(style.lineHeight) || 1.3 * size;
    if (caret.parentElement !== secret.parentElement) secret.parentElement.append(caret);
    caret.classList.remove('bar');
    caret.classList.add('still');
    measured = null;
    showCaret(secret.offsetLeft, secret.offsetTop + (secret.offsetHeight - tall) / 2, 0.55 * size, tall);
    return;
  }
  const field = caretField();
  if (inEditor(field)) return placeEditorCaret();
  const shown =
    field &&
    view &&
    !composing &&
    !document.body.classList.contains('locked') &&
    field.selectionStart === field.selectionEnd &&
    view.cursor == null;
  measured = asMeasured();
  // While a word is composed the input method draws its own caret, and what is offered is still
  // set under the word, so the word is measured all the same.
  const offered =
    !shown &&
    composing &&
    !!view?.offer &&
    field === line &&
    view.cursor == null &&
    field.selectionStart === field.selectionEnd;
  if (!shown && !offered) {
    caret.hidden = true;
    caretWas = '';
    return;
  }
  // The mirror is written first and the layout read after, once: the field's width and place and
  // the span's come from the same layout. The mirror's width follows the field's, and is written
  // between the two reads only when it has changed, which a keystroke seldom does.
  const style = dress(field);
  const size = parseFloat(style.fontSize);
  const padding = parseFloat(style.paddingLeft) + parseFloat(style.paddingRight);
  const at = field.selectionStart;
  const text = field.value;
  // At the end of the text there is no letter, so the block is a little over half a letter wide,
  // measured on a no-break space. Otherwise it is over the whole letter after the caret, an emoji
  // or a letter and its accent.
  const end = at >= text.length;
  const letter = end ? '' : text.slice(at, lettersOf(text.slice(at, at + 16))[1] + at);
  const span = el('span', null, end ? '\u00a0' : letter);
  // The word something is offered for is measured in the same read, so that what is offered is
  // set under its mark.
  const from = view.offer ? Math.min(unitsIn(text, view.offer.from), at) : -1;
  const word = from >= 0 ? el('span', null, text.slice(from, at)) : null;
  const before = word ? [text.slice(0, from), word] : [text.slice(0, at)];
  mirror.replaceChildren(...before, span, text.slice(at + letter.length));
  const width = `${field.clientWidth - padding}px`;
  if (mirror.style.width !== width) mirror.style.width = width;

  // The read. The block is the height of the line, centred on the letter as the line's own leading
  // is, and never narrower than a third of a letter, so that it is still a block over an i. The
  // line is scrolled to the caret before the field's place is taken.
  const { offsetLeft, offsetTop, offsetWidth, offsetHeight } = span;
  if (field === line) sidescroll(field, offsetLeft, size);
  const [fieldLeft, fieldTop] = [field.offsetLeft - field.scrollLeft, field.offsetTop - field.scrollTop];
  const tall = parseFloat(style.lineHeight) || offsetHeight;
  const wide = end ? 0.55 * size : Math.max(offsetWidth, 0.35 * size);
  const left = fieldLeft + offsetLeft - (wide - (end ? wide : offsetWidth)) / 2;
  const top = fieldTop + offsetTop - (tall - offsetHeight) / 2;

  const parent = field.parentElement;
  if (caret.parentElement !== parent) parent.append(caret);
  caret.classList.remove('bar');
  caret.classList.toggle('still', field !== document.activeElement);
  if (shown) showCaret(left, top, wide, tall);
  else {
    caret.hidden = true;
    caretWas = '';
  }
  if (word) {
    const rect = field.getBoundingClientRect();
    const y = rect.top - field.scrollTop + offsetTop - (tall - offsetHeight) / 2;
    offerAt = { x: rect.left - field.scrollLeft + word.offsetLeft, above: y, below: y + tall };
    placeOffer();
  }
}

// The line is one line, and scrolls sideways under the caret as any one-line field does, and as a
// head in its row does in the editor: the text moves only when the caret would come within five
// letters of an edge, as vim's sidescrolloff keeps them, which a browser honouring the line's
// scroll-padding does for itself and this does for every browser. The page scrolls the line in the
// same read as it measures the caret, so that the block drawn and the letter typed cannot be a
// letter apart, as they were while Chromium scrolled it after the caret had been measured. What it
// writes is kept, so that the scroll it causes is not taken for one the browser made.
const wrote = new Map();
function sidescroll(field, x, size) {
  const room = field.clientWidth;
  const max = field.scrollWidth - room;
  const context = Math.min(2.5 * size, room / 2);
  let to = Math.min(Math.max(field.scrollLeft, x - room + context), x - context);
  // A pixel or two from an end is the end, where a line scrolled by one would fade its first
  // letters.
  to = Math.max(0, Math.min(max, Math.round(to)));
  if (to <= 2) to = 0;
  else if (to >= max - 2) to = max;
  if (Math.abs(to - field.scrollLeft) > 1) {
    field.scrollLeft = to;
    wrote.set(field, field.scrollLeft);
  }
  edges(field);
}
// Each edge of a one-line field that hides text fades: the line's, and a head's in its row, whose
// editor scrolls and is faded as a whole by its frame's classes.
function edges(field, faded = field) {
  const over = field.scrollWidth - field.clientWidth;
  faded.classList.toggle('before', field.scrollLeft > 1);
  faded.classList.toggle('after', field.scrollLeft < over - 1);
}
// A scroll the browser makes itself, as a paste or a composition running past an edge does, or
// Chromium bringing the line's caret into sight after a key, moves the fades and the caret with
// it; one sidescroll made has placed the caret already. A head's is the editor's, made as it keeps
// its caret in sight.
line.addEventListener(
  'scroll',
  () => {
    edges(line);
    const was = wrote.get(line);
    wrote.delete(line);
    if (was != null && Math.abs(line.scrollLeft - was) <= 1) return;
    measured = null;
    placeCaret();
  },
  { passive: true },
);
function headScrolled() {
  headEdges();
  placeCaret();
}
// A head's fades are its frame's, the editor keeping its own element's classes to itself.
function headEdges() {
  const frame = editor.dom.parentElement;
  if (frame?.classList.contains('edit')) edges(editor.scrollDOM, frame);
}

// What is offered for the tag, person or bracketed name the caret ends: a short list floating under
// the word, its words starting under the word's mark, or a name's under where the name starts, the
// one Tab takes marked as the cursor's row is. Its words are painted with the rest; where it goes
// is measured with the caret, in the same read (see placeCaret). It is under the word, or over it
// where under would reach the foot. A press on one takes it, and leaves the focus in the field.
const offerNode = $('offer');
let offerAt = null;
function paintOffer() {
  const offer = view.offer;
  offerNode.hidden = !offer;
  if (!offer) {
    offerAt = null;
    return;
  }
  set(offerNode, JSON.stringify(offer.words), () =>
    offer.words.map((word) => {
      const li = el('li', null, word);
      li.setAttribute('role', 'option');
      return li;
    }),
  );
  [...offerNode.children].forEach((li, i) => li.setAttribute('aria-selected', String(i === offer.chosen)));
  placeOffer();
}
function placeOffer() {
  if (offerNode.hidden || !offerAt) return;
  const tall = offerNode.offsetHeight;
  const foot = $('foot').getBoundingClientRect().top;
  const top = offerAt.below + tall <= foot || offerAt.above - tall < 0 ? offerAt.below : offerAt.above - tall;
  const inset = parseFloat(getComputedStyle(offerNode).paddingLeft);
  const left = Math.max(0, Math.min(offerAt.x - inset, innerWidth - offerNode.offsetWidth));
  offerNode.style.left = `${left}px`;
  offerNode.style.top = `${top}px`;
}
offerNode.addEventListener('mousedown', (event) => event.preventDefault());
offerNode.addEventListener('click', (event) => {
  const li = event.target.closest('li');
  if (!li || !view?.offer) return;
  act({ take: [...offerNode.children].indexOf(li) });
  safely();
  follow();
});
// A list scrolled under a head being written moves the head, and what is offered goes with it.
list.addEventListener(
  'scroll',
  () => {
    if (!view?.offer) return;
    measured = null;
    placeCaret();
  },
  { passive: true },
);

// Where the caret is, said to the engine in characters after typing in the line or a head, and
// when it moves while something is offered, so that what completes the tag or person it ends is
// offered, and closed once the caret leaves the word. A move to where the caret was last said is
// not said again; typing always is, since what it typed closed what was offered, even when it
// typed what was there already, as a composition committed or a paste of the same word does.
let saidAt = '';
function sayCaret(field, text, { anchor, head }, moved = false) {
  if (anchor !== head) return;
  const at = charsIn(text, head);
  const now = JSON.stringify([field === line, at, text]);
  if (moved && now === saidAt) return;
  saidAt = now;
  act({ at });
}
const lineCaret = () => ({ anchor: line.selectionStart, head: line.selectionEnd });
const caretOffered = () => {
  if (view?.offer && document.activeElement === line) sayCaret(line, line.value, lineCaret(), true);
};
document.addEventListener('selectionchange', caretOffered);

// While a word is offered for, Tab and Enter take the one chosen, the arrows up and down choose
// another, and Escape closes it; every other key means what it always does. On a row Shift-Enter
// still ticks; on the line it is Enter. Taking paints at once, so that the next key lands in the
// text with the word in it.
function onOffer(event, onRow) {
  if (!view?.offer || event.isComposing || composing || editor?.composing || event.keyCode === 229) return false;
  const plain = !event.ctrlKey && !event.metaKey && !event.altKey;
  let action = null;
  if (escapes(event)) action = 'dismiss';
  else if (plain && event.key === 'Tab' && !event.shiftKey) action = { take: null };
  else if (plain && event.key === 'Enter' && !(event.shiftKey && onRow)) action = { take: null };
  else if (plain && !event.shiftKey && event.key === 'ArrowDown') action = { choose: 1 };
  else if (plain && !event.shiftKey && event.key === 'ArrowUp') action = { choose: -1 };
  if (!action) return false;
  event.preventDefault();
  act(action);
  if (named(action) === 'take') safely();
  follow();
  return true;
}

function showCaret(left, top, wide, tall) {
  caret.style.cssText = `left:${left}px;top:${top}px;width:${wide}px;height:${tall}px`;
  caret.hidden = false;
  // A caret that has moved starts its breath again from solid.
  const now = `${left} ${top} ${wide}`;
  if (now !== caretWas) for (const breath of caret.getAnimations?.() ?? []) breath.currentTime = 0;
  caretWas = now;
}

// In the editor there is no copy to measure: the editor says where the caret is and where the
// letter after it ends. The block is sized and centred as it is on the line. In vim's insert mode
// it is a bar at the caret, as a terminal draws vim's. The block is placed in the editor's frame,
// what the sheet scrolls or the head's frame in its row, so that it moves with the text. A head
// longer than its row reads from its start as the cursor lands, its caret at the end, past the
// edge: the block is at the edge until the caret is brought into sight, where the terminal draws
// its cursor on the row's `…`. What is offered for a word in a head is set under the word, from
// its mark, while a composition is under way too.
function placeEditorCaret() {
  const c = editor.caret();
  const parent = editor.dom.parentElement;
  const head = !!view?.field && !editor.whole;
  const style = getComputedStyle(editor.contentDOM);
  const tall = parseFloat(style.lineHeight) || 1.5 * parseFloat(style.fontSize);
  if (head && parent && view.offer) {
    const mark = editor.coordsAt(Math.min(unitsIn(editor.text(), view.offer.from), editor.selection().head));
    if (mark) {
      const y = mark.top - (tall - (mark.bottom - mark.top)) / 2;
      offerAt = { x: mark.left, above: y, below: y + tall };
      placeOffer();
    }
  }
  if (!view || !parent || !c.empty || !c.rect || editor.composing) {
    caret.hidden = true;
    caretWas = '';
    return;
  }
  const size = parseFloat(style.fontSize);
  const bar = c.mode === 'insert';
  const letter = c.right ? c.right.left - c.rect.left : 0;
  const wide = bar ? 2 : c.right ? Math.max(letter, 0.35 * size) : 0.55 * size;
  const frame = parent.getBoundingClientRect();
  const inset = bar ? 1 : c.right ? (wide - letter) / 2 : 0;
  let x = c.rect.left - inset;
  if (head) x = Math.min(x, editor.scrollDOM.getBoundingClientRect().right - wide);
  const left = x - frame.left - parent.clientLeft + parent.scrollLeft;
  const top =
    c.rect.top - frame.top - parent.clientTop + parent.scrollTop - (tall - (c.rect.bottom - c.rect.top)) / 2;
  if (caret.parentElement !== parent) parent.append(caret);
  caret.classList.toggle('bar', bar);
  caret.classList.toggle('still', document.activeElement !== editor.contentDOM);
  showCaret(left, top, wide, tall);
}

line.addEventListener('focus', () => {
  held = line;
  if (!quiet) caretMoved();
});
line.addEventListener('blur', () => {
  if (held === line) held = null;
  placeCaret();
});
line.addEventListener('input', caretMoved);
// Some browsers say a text field's selection changed to the field and not to the document.
line.addEventListener('selectionchange', () => {
  if (held !== line) return;
  caretMoved();
  caretOffered();
});
line.addEventListener('compositionstart', () => {
  composing = true;
  placeCaret();
});
line.addEventListener('compositionend', () => {
  composing = false;
  placeCaret();
});
for (const moment of ['focus', 'blur', 'input']) secret.addEventListener(moment, () => placeCaret());
document.addEventListener('selectionchange', () => {
  if (held && document.activeElement === held) caretMoved();
});
window.addEventListener('resize', () => {
  placeCaret();
  // The fades of the line and of a head follow their width even while no caret is drawn there.
  edges(line);
  if (editor) headEdges();
  // Whether the head under the cursor is cut, and so read beside the list, follows the width.
  if (view && wide.matches) paintSoon();
});

const ctrlOnly = (event) => event.ctrlKey && !event.shiftKey && !event.metaKey && !event.altKey;

// The keys, wherever the focus has got to but the editor and the sheet, which have their own. A key
// that closes the sheet takes what it was pressed in out of the page, so the sheet is looked for
// on the way the key came rather than round where it was pressed. Typing goes to the field the
// cursor is in, so a letter pressed on a word in the foot takes the focus there before it lands.
// A button that has the focus keeps Enter and Space.
document.addEventListener('keydown', (event) => {
  wantsText = true;
  try {
    pressed(event);
  } finally {
    wantsText = false;
  }
});

function pressed(event) {
  if (event.isComposing || !view || !fzn) return;
  const path = event.composedPath();
  if (path.includes(buffer) || path.includes(editor?.dom)) return;
  const onButton = event.target instanceof HTMLButtonElement;
  if (onButton && (event.key === 'Enter' || event.key === ' ')) return;
  const onLine = view.cursor == null;
  if (onOffer(event, !onLine)) return;
  // Tab goes into what the line names, from anywhere in its text, and the session says whether it
  // names anywhere. It never moves the focus, since the page's keys are fzn's while it has them,
  // and Shift-Tab is nothing for the same reason.
  if (event.key === 'Tab') {
    if (!event.shiftKey && !event.ctrlKey && !event.metaKey && !event.altKey && !view.field?.whole) act('go_in');
    event.preventDefault();
    follow();
    return;
  }
  // The chords: on the line Ctrl-D clears it, and on a row it deletes the note; Ctrl-O on a row
  // opens its body, and on the line the body of the note you are in, where there is one; none asks
  // for the browser's bookmark or file dialog, its search or its history, either way (see chord).
  // Ctrl-[ is Escape.
  if (ctrlOnly(event) && event.key.length === 1 && event.key !== '[') {
    if (!chord(event.key, !onLine)) return;
    event.preventDefault();
    follow();
    return;
  }
  const typed = event.key.length === 1 && !event.ctrlKey && !event.metaKey && !event.altKey;
  if (typed) {
    follow();
    return;
  }
  // Control with an arrow on a row moves the note, and the page is not scrolled.
  if (ctrlOnly(event) && !onLine && (event.key === 'ArrowUp' || event.key === 'ArrowDown')) {
    act({ shift: event.key === 'ArrowUp' ? -1 : 1 });
    event.preventDefault();
    follow();
    return;
  }
  if (!escapes(event) && (event.ctrlKey || event.metaKey || event.altKey)) return;
  switch (escapes(event) ? 'Escape' : event.key) {
    case 'ArrowDown':
      act({ move: 1 });
      break;
    case 'ArrowUp':
      act({ move: -1 });
      break;
    case 'PageDown':
      act({ move: 10 });
      break;
    case 'PageUp':
      act({ move: -10 });
      break;
    // On a row Shift-Enter ticks; on the line it is Enter.
    case 'Enter':
      act(event.shiftKey && !onLine ? 'archive' : 'write');
      break;
    // Off the row, then off the text on the line, then out of the place.
    case 'Escape':
      if (!onLine) act(view.field?.whole ? 'close' : { select: null });
      else if (view.line !== '') act({ type: '' });
      else if (view.path.length) act('back');
      else return;
      break;
    default:
      return;
  }
  event.preventDefault();
  follow();
}

$('clear').onclick = () => {
  act({ type: '' });
  follow();
};
$('go-in').onclick = () => {
  act('go_in');
  follow();
};

// While a body is open, a press on its row behind the sheet keeps the focus in the sheet, so that
// the body is not closed by the press and opened again by the tap; so does a press in the sheet
// around the text. A press anywhere else leaves the sheet, and closes it.
list.addEventListener('mousedown', (event) => {
  if (view?.field?.whole && event.target.closest('.row')?.dataset.id === editorIn) event.preventDefault();
});
buffer.addEventListener('mousedown', (event) => {
  if (event.target === buffer || event.target === sheet) event.preventDefault();
});
// On a touch screen the body need not have the focus, the keyboard being down, so a press beside
// the sheet closes it here rather than by the blur that closes it on a desktop. A press on its row
// behind it, or on a word in the foot, does not, as on a desktop.
document.addEventListener('mousedown', (event) => {
  if (!coarse.matches || !view?.field?.whole || event.target.closest('#buffer, #foot')) return;
  if (event.target.closest('.row')?.dataset.id === editorIn) return;
  leaving = true;
  try {
    act('close');
  } finally {
    leaving = false;
  }
});
// A press on a link, in a row or a body read, leaves the focus where it was, as the cursor stays.
document.addEventListener('mousedown', (event) => {
  if (event.target.closest('a')) event.preventDefault();
});

// Whether a point is on the words of a head, a task's box among them, a few pixels round each line
// of them counting, since a finger is not a point.
const SLOP = 6;
const near = (r, x, y) => x >= r.left - SLOP && x <= r.right + SLOP && y >= r.top - SLOP && y <= r.bottom + SLOP;
function onWords(head, x, y) {
  if (!head) return false;
  const words = document.createRange();
  words.selectNodeContents(head);
  return [...words.getClientRects()].some((r) => near(r, x, y));
}

// A task's box ticks it where it reads, and the cursor stays where it was: on the line, so that
// boxes can be ticked down a list the line found, or on the note it was on. The engine ticks the
// note under the cursor, so the cursor goes to the task and back, and the focus is held where it
// was meanwhile, a field that had it keeping it.
let holdFocus = false;
function tick(at) {
  if (view.field?.whole) return;
  const had = typing();
  const was = view.cursor != null ? (view.rows[view.cursor]?.id ?? null) : null;
  const task = view.rows[at].id;
  // A head being written keeps its caret, which landing on it again would put at its end.
  const writing = was != null && editorIn === was ? editor.selection() : null;
  holdFocus = true;
  try {
    act({ select: at });
    act('archive');
    const back = view.rows.findIndex((row) => row.id === was);
    if (was == null) act({ select: null });
    else if (was !== task && back >= 0) {
      act({ select: back });
      if (writing && editorIn === was) editor.select(writing);
    }
  } finally {
    holdFocus = false;
  }
  if (!had) return;
  wantsText = true;
  follow();
  wantsText = false;
}

// Where the last touch on the list landed, as the finger put it. A browser moves a tap onto a
// control near it, a task's box among them, and the click's point with it, so a tap on a task's
// first letter would tick it; the box is pressed only where the finger landed on its room.
let touched = null;
list.addEventListener(
  'touchstart',
  (event) => {
    const [touch] = event.touches;
    touched = { x: touch.clientX, y: touch.clientY, at: Date.now() };
  },
  { passive: true },
);

// A tap beside a head's words puts the cursor on the note, and a second there opens its body; a
// tap on the words puts the cursor there too with the caret where it landed, which on a phone is
// what raises the keyboard, and the span it landed in shows its marks. The head under the cursor
// is the editor's, so a press on it is the editor's, a link's word included, which is not followed
// there; but a press on a task's box ticks it, in any row, and one let go off the box does
// nothing, as a button let go off itself does, rather than landing on the words. On the row under
// the cursor a press off the editor opens the body. The arrow on the selected row goes in, and a
// link in any other row opens and does nothing else.
let fromTick = false;
list.addEventListener('mousedown', (event) => {
  fromTick = !!event.target.closest('.tick');
});
list.addEventListener('click', (event) => {
  const item = event.target.closest('.row');
  if (fromTick && !event.target.closest('.tick')) {
    fromTick = false;
    return;
  }
  if (!item) return;
  const at = view.rows.findIndex((row) => row.id === item.dataset.id);
  if (at < 0) return;
  const fresh = touched && Date.now() - touched.at < 1000;
  const [x, y] = fresh ? [touched.x, touched.y] : [event.clientX, event.clientY];
  const ticker = event.target.closest('.tick');
  const reach = (node) => node.getBoundingClientRect().right + 0.4 * parseFloat(getComputedStyle(node).fontSize);
  if (ticker && (!fresh || x <= reach(ticker))) return tick(at);
  if (event.target.closest('.cm-editor, a')) return;
  if (event.target.closest('.chev')) act('go_in');
  else if (at !== view.cursor) {
    const words = onWords(item.querySelector('.head'), x, y);
    wantsText = words;
    act({ select: at });
    if (words && editorIn === item.dataset.id) editor.placeAt(x, y);
  } else if (!view.field?.whole) act('open');
  follow();
  wantsText = false;
});

// A browser takes a tap near a field into the field, and the head under the cursor is a field as
// wide as the row, so a second tap beside its words, on its own line or on the date under it,
// would only place the caret. So where the finger landed is read from the touch, which neither
// browser moves, and judged as the first tap is: off the head, or in it past the lines of its
// words, it opens the body. It is heard on the list, since WebKit sends a touch to what is under
// the finger and Chromium to the field. While the keyboard is up a tap in the head is the caret's,
// so that a finger can put the caret at the end of the line; and a new row's empty head is all
// words.
let besideHead = false;
list.addEventListener(
  'touchstart',
  (event) => {
    besideHead = event.touches.length === 1 && besideTheWords(event.touches[0]);
  },
  { passive: true },
);
list.addEventListener('touchmove', () => (besideHead = false), { passive: true });
list.addEventListener('touchend', (event) => {
  if (!besideHead || !event.cancelable) return;
  besideHead = false;
  event.preventDefault();
  act('open');
  follow();
});

function besideTheWords({ clientX: x, clientY: y }) {
  if (!view?.field || view.field.whole || !editorIn || editor.hasFocus) return false;
  const row = rows.get(editorIn);
  const within = (r) => x >= r.left && x <= r.right && y >= r.top && y <= r.bottom;
  if (!row || !within(row.getBoundingClientRect())) return false;
  const chev = row.querySelector('.chev');
  if (chev && within(chev.getBoundingClientRect())) return false;
  if (editor.text() === '') return !near(editor.dom.getBoundingClientRect(), x, y);
  return !onWords(editor.contentDOM.querySelector('.cm-line'), x, y);
}

list.addEventListener('animationend', (event) => event.target.classList.remove('in'));

// A tap on the note you are in, read above the line or beside the list, opens its body in the
// sheet, as a second tap on a row opens the row's, unless it was on a link, which opens and does
// nothing else. The body opened from the line is that note's, so a tap above the line with the
// cursor on a row steps onto the line first. Beside the list it is the note you are in only while
// the cursor is on the line and it has something to read, and a press that selected some of it was
// for the selection.
function openHere(event) {
  if (event.target.closest('a') || getSelection()?.type === 'Range' || !view?.here) return;
  if (view.cursor != null) act({ select: null });
  if (view.cursor == null && offered('open')) act('open');
  follow();
}
$('here').addEventListener('click', openHere);
pane.addEventListener('click', (event) => {
  if (view && paneReadsHere()) openHere(event);
});

// Each step of the path goes back to where it was.
$('path').addEventListener('click', (event) => {
  const depth = event.target.dataset?.depth;
  if (depth === undefined) return;
  for (let steps = view.path.length - Number(depth); steps > 0; steps--) act('back');
  follow();
});

// A control that is not text never takes the focus: the words in the foot, the cross and the
// arrows, a step of the path and a task's box act on where the cursor is and leave the focus, and
// so a phone's keyboard, where it was; "put back" is about the note, and an open body is not
// closed first. On a touch screen a press on a row while a field has the focus leaves it there
// too, so that the keyboard stays up while the cursor moves (see the list's click).
for (const part of [$('top'), $('path'), $('foot'), list]) {
  part.addEventListener('mousedown', (event) => {
    if (event.target.closest('button, .tick')) event.preventDefault();
    else if (part === list && coarse.matches && typing() && !event.target.closest('.cm-editor')) {
      event.preventDefault();
    }
  });
}

// A page put away with the cursor on a row writes what the row holds, if it changed. Being hidden
// comes before being unloaded and leaves more time for the line to be kept and sent, so it is
// heard too. A hidden page keeps the field and stays, an open body open, since switching tabs to
// look something up and coming back is not leaving it; only a page that is going leaves the row,
// and a second time with nothing changed does nothing. Writing may change the head under the
// caret, as a new note written inside a tag gets the tag, so the editor takes it at once: a hidden
// page has no next frame to paint it in, and a key pressed on coming back would otherwise be
// typed into the head as it was and take the tag away again.
function putAway(going) {
  if (!view?.field || reloading) return;
  act(going ? 'close' : 'keep');
  safely();
}
window.addEventListener('pagehide', () => putAway(true));
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'hidden') putAway(false);
});

// A tab that goes says so, so that the others stop sending to it, and that it has left, so that
// the next page in the tab goes on with its log (see writer). This comes after putAway's, so that
// what that writes comes first. A page put in the back-forward cache has not left.
window.addEventListener('pagehide', (event) => {
  tabs?.postMessage({ from: tab, bye: true });
  const id = kept(WRITER, session);
  if (event.persisted || writerIn !== session || !plain(id)) return;
  const token = `${Date.now().toString(36).padStart(10, '0')}.${hex(4)}`;
  keep(leftBy(id), token);
  keep(LEFT, token, session);
});

// The page follows the relay's build. It knows its own from the page it was loaded as, which the
// relay names in a meta of `index.html`, and asks the relay which it serves now with a `HEAD` of
// the page, whose answer names it in `Fzn-Build`, when there is reason to think it has changed: the
// tab coming back into sight, and the socket coming back to a relay that went away, which is what
// a deploy looks like from a tab; at most once in 30 seconds, and never for a page whose meta is
// empty, as one from a relay from before builds were named. A newer build is
// reloaded onto unless the person is in the middle of something: a body open and changed, a
// composition, a key in the last two seconds, or the tab out of sight, in which case it is tried
// again once they are not. Text on the line, the cursor on a row and a head being written are not
// in the middle of anything: a head is written as it is typed, and the rest is carried. Before the
// reload the field is kept and the page waits for the store to have it; where the page is goes
// into the history's entry as `kept`: the path's steps as the engine gives them, the line, the
// note under the cursor, how far the list was scrolled, the build wanted and when. A reload that
// did not bring the build it was made for gives that build up, and the page does not ask for it
// again, nor reloads within a minute of the last, so that a relay answering one build and serving
// another is not reloaded onto for ever.
const BUILD = document.querySelector('meta[name="fzn-build"]')?.content ?? '';
const remembered = () => entry().kept ?? {};
const remember = (kept) => history.replaceState({ ...entry(), kept }, '');
let gaveUp = remembered().gaveUp ?? null;
const reloadedAt = remembered().at ?? 0;
let stale = null;
let lastAsked = 0;
let lastKey = 0;
let reloading = false;
document.addEventListener('keydown', () => (lastKey = Date.now()), true);
document.addEventListener('input', () => (lastKey = Date.now()), true);

// What the relay serves now, or nothing if it cannot be asked.
async function served() {
  const stop = new AbortController();
  const late = setTimeout(() => stop.abort(), 3000);
  try {
    const answer = await fetch('./', { method: 'HEAD', cache: 'no-store', signal: stop.signal });
    return answer.headers.get('Fzn-Build') || null;
  } catch {
    return null;
  } finally {
    clearTimeout(late);
  }
}

async function check() {
  if (!BUILD || !fzn || reloading) return;
  if (stale) return tryReload();
  if (Date.now() - lastAsked < 30000 || Date.now() - reloadedAt < 60000) return;
  lastAsked = Date.now();
  const fresh = await served();
  if (!fresh || fresh === BUILD || fresh === gaveUp) return;
  stale = fresh;
  tryReload();
}

const busy = () =>
  document.visibilityState !== 'visible' ||
  composing ||
  !!editor?.composing ||
  Date.now() - lastKey < 2000 ||
  (coarse.matches && typing()) ||
  (!!view?.field?.whole && offered('undo'));

let later = 0;
function tryReload() {
  clearTimeout(later);
  if (!stale || reloading) return;
  // Out of sight it waits to be seen again, which asks again.
  if (document.visibilityState !== 'visible') return;
  if (busy()) later = setTimeout(tryReload, 2000);
  else reload({ wanted: stale });
}

// Where the page is, as the history's entry keeps it.
function whereNow() {
  let where = [];
  try {
    where = JSON.parse(fzn.steps());
  } catch {
    // No path is still a place to come back to.
  }
  return {
    where,
    line: view.line,
    cursor: view.cursor != null ? (view.rows[view.cursor]?.id ?? null) : null,
    scroll: list.scrollTop,
  };
}

// Reloads the page where it is. What the field holds is kept first, and the page waits, a second
// at most, for the store to have it, unless the page already wrote what it held when it was put
// away. From here the page takes no more actions: one would be neither carried nor waited for.
async function reload({ wanted = null, writing = true } = {}) {
  if (reloading) return;
  if (writing && view?.field) act('keep');
  reloading = true;
  if (view && fzn) {
    const kept = { ...remembered(), ...whereNow(), at: Date.now() };
    if (wanted) kept.wanted = wanted;
    remember(kept);
    if (writing) await Promise.race([stored, lapse(1000)]);
  }
  location.reload();
}

document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'visible') check();
  else rememberNow();
});

// Where the page is goes into the history's entry as it changes, a second after the last change
// and at once when the tab is hidden, so that any reload comes back there: one asked for by hand,
// and a page gone back to that the browser did not keep. Not when the page is unloaded, which is
// too late: Chrome keeps nothing written to an entry once the page is going.
let rememberedAs = '';
let rememberLater = 0;
function rememberSoon() {
  clearTimeout(rememberLater);
  rememberLater = setTimeout(rememberNow, 1000);
}
function rememberNow() {
  clearTimeout(rememberLater);
  if (!view || !fzn || opening || reloading) return;
  if (travelling) return rememberSoon();
  const place = { ...remembered(), ...whereNow() };
  const said = JSON.stringify([history.length, entry().depth, place]);
  if (said === rememberedAs) return;
  rememberedAs = said;
  remember(place);
}
// A page given back from the cache of pages gone back from is reloaded where it was, whatever the
// relay serves: the other tabs have let it go.
window.addEventListener('pageshow', (event) => {
  if (event.persisted) reload({ writing: false });
});

// Puts the page back where it was before a reload, before anything is painted: the path, then
// the line, then the cursor on the note it was on, if that is still listed. The history is then
// made to match: the entry the page opened in says how deep the page was, and the entries behind
// it lead back out of that, so a path put back whole leaves them as they are, and one put back
// shorter, for a step that has gone, goes back through them to the depth it has.
function restore() {
  const { where, line: typed, cursor, scroll, wanted, ...rest } = remembered();
  if (wanted && wanted !== BUILD) gaveUp = rest.gaveUp = wanted;
  if (where || typed || cursor || wanted) remember(rest);
  if (where?.length) act({ restore: where });
  if (typed) act({ type: typed });
  if (cursor != null) {
    const at = view.rows.findIndex((row) => row.id === cursor);
    if (at >= 0) act({ select: at });
  }
  opening = false;
  depth = entry().depth ?? 0;
  if (entry().depth == null) history.replaceState({ ...entry(), depth }, '');
  keepHistory();
  if (scroll) {
    // The list is made as far as it was scrolled, so that there is somewhere to scroll back to:
    // painted once for the place put back, which starts a new list, and then further.
    safely();
    further = Math.ceil(scroll / rowHeight()) + MARGIN;
    paintSoon();
    requestAnimationFrame(() => {
      if (!stuck()) list.scrollTop = scroll;
    });
  }
}

// For a passphrase mistyped: the key is forgotten and the page asks again.
const retype = () => {
  keep(KEY, null);
  location.reload();
};
$('passphrase').onclick = retype;
$('again').onclick = retype;

async function start() {
  frame();
  await init();
  let why = '';
  let id = null;
  for (;;) {
    try {
      const secret = await key(why);
      // The id is taken before the store is read, so that whatever the log's last writer kept is
      // there to be read, and that writer, which let the id go, writes no more.
      if (id == null) {
        here();
        id = await writer();
      }
      fzn = new Fzn(id, secret);
      break;
    } catch (cause) {
      keep(KEY, null);
      why = `${cause}. Type the passphrase again.`;
    }
  }
  store = await openStore().catch(() => null);
  // The reading and the editor are here before the first painting, which may put the cursor on a
  // row, fetched meanwhile beside the engine (see modules).
  const [renderModule, editorModule] = await modules;
  render = renderModule;
  render.onLanguage(reread);
  editor = editorModule.mount(editorHooks, $('sheet-foot'));
  editor.setVim(vimOn);
  editor.dom.addEventListener('keydown', keyInHead, true);
  editor.scrollDOM.addEventListener('scroll', headScrolled, { passive: true });
  if (store) for (const [log, lines] of await store.load()) carryOut(fzn.load(log, lines));
  carryOut(fzn.opened());
  joined = true;
  tabs?.postMessage({ from: tab, hello: true });
  look();
  restore();
  $('unlock').hidden = true;
  $('for').innerHTML = ASK;
  document.body.classList.remove('locked');
  follow();
  placeCaret();
  connect();
  // A lost relay is tried again as soon as there is reason to think it is back.
  const retry = () => {
    if (document.visibilityState === 'visible' && !socket) {
      wait = 1000;
      connect();
    }
  };
  window.addEventListener('online', retry);
  document.addEventListener('visibilitychange', retry);
}

start().catch((why) => {
  console.error(why);
  trouble(`fzn could not start: ${why}`);
});
