// The browser face: a blank page with one cursor on it.
//
// The engine is the app. It runs here as WebAssembly and is told what was asked for, as an
// action, and says what to show. This file is what a page adds to it: which key or tap asks
// for what, painting the view, the socket to the relay this page came from, the other tabs open
// on the space, and somewhere to keep lines between visits.
//
// There is one cursor on the page and it moves. On the line it is the caret, and typing finds. Down
// puts it on a row, whose head is a text box with the caret at its end, so typing there changes the
// head and nothing has to be pressed to begin or to keep it: the engine writes each change as it is
// typed. The row reads as it did, its marks rendered and the caret a block at the end of the
// reading, until it is written in or the caret is put in it, and only then shows the text as typed
// (see reveal), so that walking the list moves no text. Enter with text on the line writes it and
// puts the box in the note it wrote. Enter on a row opens a new note under it, its empty head the
// box, so that Enter, type, Enter, type writes notes in a run, as the original's Enter did, unless
// its head has `/task` or a date word still to write out, which Enter writes out first. Ctrl-O
// opens the body, the original's chord for it, in the sheet: a bounded overlay over the dimmed
// page, scrolled on its own, where the box holds the whole note, or an editor takes its place once
// one has been fetched (see loadEditor). The arrows stay inside it, and Escape closes it, or with vim's keys `:q`;
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
// read it with. The chords are the terminal's wherever a browser lets them go, and where it
// does not, the terminal names the page's, so that the two faces are one set of keys to learn.
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
  // Where the engine put the caret in the head, for the painting to put the box's there. Where it
  // put it in the line needs nothing: the line is only set when the engine changed it, and setting
  // it leaves the caret at the end, where the engine puts it.
  if (todo.caret?.head != null) placed = todo.caret.head;
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
  // it does on the way into a note and out of one, and when the line is written.
  if (line.value !== view.line) line.value = view.line;
  if (!editor && view.field?.whole) loadEditor();
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

// The page's reading of markdown, a module of its own fetched as the page starts and never waited
// for. With it a body read beside the list or above the line is rendered, and a row's head shows
// its links as links; until it is here, and for good if it cannot be fetched, both read as they
// were typed. What it paints is painted again when it arrives, and when a language arrives for
// code it showed plain, which `readings` counts.
let render = null;
let readings = 0;
const reread = () => {
  readings++;
  paintSoon();
};
import('./vendor/render.js').then((module) => {
  render = module;
  render.onLanguage(reread);
  reread();
}, () => {});
// A task's head starts with its box, as the engine says it does, and the box is drawn, as a body
// read draws one, and ticks the task when it is pressed (see tick); the rest of the head reads as
// any head does. The box is the head's first three letters and the space after them, as `task`
// on the engine's row says, and how much of the text it was is kept for placing the caret.
const readHead = (text, task) => {
  const head = el('span', 'head');
  if (task != null) {
    const button = el('button', task ? 'tick done' : 'tick');
    button.type = 'button';
    button.tabIndex = -1;
    button.setAttribute('aria-label', task ? 'Untick' : 'Tick');
    const cut = text[3] === ' ' ? 4 : 3;
    head.dataset.boxed = cut;
    head.append(button);
    text = text.slice(cut);
  }
  head.append(render ? render.head(text) : text);
  return head;
};
const readBody = (text, upTo) => (render ? render.body(text, upTo) : text);

const named = (action) => (typeof action === 'string' ? action : Object.keys(action)[0]);

const narrow = window.matchMedia('(max-width: 640px)');
narrow.addEventListener('change', () => view && paintSoon());
// A touch screen: a coarse pointer and nothing to hover with. A field with the focus there is a
// keyboard up.
const coarse = window.matchMedia('(hover: none) and (pointer: coarse)');
const mac = /mac/i.test(navigator.userAgentData?.platform ?? navigator.platform ?? '');
const typing = () => [line, box, $('secret')].includes(document.activeElement) || !!editor?.hasFocus;
// The keyboard there is the person's: the page moves the focus while a field has it, and
// otherwise only for a press that asked to write, which sets this until the painting that
// follows has given the field the focus: true, or where in the head a press on its words landed,
// which may be 0. A key pressed asks to write too: on a touch screen it is
// a keyboard joined to the device, and raises nothing. A phone shows no keyboard for a focus
// nobody asked for, leaving the page typing into nothing, or raises one under the hands; and a
// focus given inside the press's own event is the only kind for which iOS raises one.
let wantsText = false;
// Where the engine put the caret in the head since the last painting, in characters: when it, and
// not the person's typing, changed what the caret is in, by landing on a row, writing out a word,
// putting a head back, a version from elsewhere, or a body opening or closing. Otherwise the caret
// stays where it was, as far as the text still reaches.
let placed = null;

// Where a count of characters, as the engine counts them, ends in a text, as the browser counts it.
const unitsIn = (text, chars) => {
  let at = 0;
  for (const ch of text) {
    if (chars-- <= 0) break;
    at += ch.length;
  }
  return at;
};
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
    if (document.activeElement === box && !view?.field?.whole) box.scrollIntoView({ block: 'nearest' });
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
  if (!now) clampHere();
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

// Whether the note you are in runs past its six lines is seen once it is laid out, so it is read
// only when that note changes and when the page's width does, rather than at every key.
function clampHere() {
  const here = $('here');
  here.classList.toggle('clamp', here.scrollHeight > here.clientHeight);
}

// The rows on the page by note id.
const rows = new Map();

// The text box that the head of the row under the cursor becomes. There is one, made once, and it
// is moved into whichever row the cursor lands on, so that it is never replaced while the cursor
// stays: it keeps the person's caret and what they have typed. On a head it holds the first line
// of the note; with the body open it is moved into the sheet and holds the whole note, until the
// editor has arrived and takes its place there. `quiet` is set while the painter moves it about,
// so that a blur it causes is not taken for the person leaving.
const box = el('textarea', 'box');
box.rows = 1;
box.spellcheck = false;
box.autocapitalize = 'sentences';
box.setAttribute('aria-label', 'The note under the cursor');
box.setAttribute('enterkeyhint', 'enter');
let boxIn = null;
let quiet = false;
// Set while the box is being left for somewhere else, so that the painting that follows does not
// take the focus back.
let leaving = false;

// The row under the cursor reads as it does off the cursor, its marks rendered and a place's
// labels hidden, until it is written in or the caret is put in it: walking the list with the
// arrows turned every row it passed to its raw text and back, so that text moved under a cursor
// that was only moving. Until then the box is in the row all the same, with the focus whenever it
// would have it, so that typing, a composition and a phone's keyboard behave as they always have,
// but it is laid under the head as read and shows nothing, no text and no caret; the first thing
// that writes in it or moves its caret takes the reading away and leaves the box, the event
// landing in it as it would have. `revealed` says which it is for the row the box is in, and is
// set again as the cursor lands on a row: a new row, being written, starts revealed. So does a row
// whose landing `revealOnLanding` asks to be written in: one pressed on its words, whose caret goes
// where the press landed, and the note Enter wrote from the line, whose head is still being typed.
let revealed = false;
let revealOnLanding = false;

// The keys that reveal the row: those that would change its head and those that move the caret
// within it. Everything else that can be pressed on a row leaves it, or acts on the note as a
// whole, and reads it as it was: Up, Down, the pages, Tab, Enter in any of its forms, Escape and
// the chords. A key is a reveal only by being named here, so that one given a meaning later is not
// a reveal by accident. What the keys miss, a paste, a cut, dictation, a composition, a selection,
// is caught where it lands (see reveal's listeners).
const CARET_KEYS = new Set(['ArrowLeft', 'ArrowRight', 'Home', 'End']);
const revealsOn = (event) =>
  CARET_KEYS.has(event.key) ||
  event.key === 'Backspace' ||
  event.key === 'Delete' ||
  (event.key.length === 1 && !event.ctrlKey && !event.metaKey);

// Takes the reading off the row under the cursor and shows the box under it, which is where it was
// all along, so that nothing moves but the marks.
function reveal() {
  if (revealed || !boxIn) return;
  revealed = true;
  const edit = box.parentElement;
  if (edit?.classList.contains('edit')) {
    edit.classList.remove('veiled');
    edit.querySelector(':scope > .head')?.remove();
  }
  placeCaret();
}

// The letters of a head, where each starts in its text and then where the text ends, which is the
// end a caret can be at: a letter is what Left and Right step over, a whole grapheme, an emoji or
// a letter and its accent, and never half a surrogate pair.
const graphemes = window.Intl?.Segmenter ? new Intl.Segmenter(undefined, { granularity: 'grapheme' }) : null;
function lettersOf(text) {
  const starts = [];
  if (graphemes) for (const { index } of graphemes.segment(text)) starts.push(index);
  else for (let at = 0; at < text.length; at += text.codePointAt(at) > 0xffff ? 2 : 1) starts.push(at);
  starts.push(text.length);
  return starts;
}

// Reveals the row with the caret at a place in its head, for a press on its words: the focus is
// given here, inside the press, since that is the only focus for which iOS raises a keyboard. The
// letter pressed stays under the pointer, at `x`, as the marks open round it (see sidescroll).
function revealAt(at, x = null) {
  reveal();
  box.focus({ preventScroll: true });
  const to = Math.min(at, box.value.length);
  pressedAt = x;
  box.setSelectionRange(to, to);
  measured = null;
  caretMoved();
  pressedAt = null;
}

const headOf = (text) => {
  const at = text.indexOf('\n');
  return at < 0 ? text : text.slice(0, at);
};

// What the box should hold for the field as it now is.
const boxText = (field) => (field.whole ? field.text : headOf(field.text));

// Where it wraps, in the sheet and at a phone's width, its size follows its text: by the browser
// where it can, and otherwise from the number of lines, each counted 72 letters wide, since
// measuring would cost the keystroke. Elsewhere it is one line high whatever it holds.
const sizes = CSS.supports?.('field-sizing', 'content');
function fit() {
  if (!sizes) box.rows = box.value.split('\n').reduce((n, part) => n + 1 + Math.floor(part.length / 72), 0);
}

// On a head the engine is told the note with the head as typed and the rest as it was; with the
// body open, the whole of what the box holds.
box.addEventListener('input', () => {
  fit();
  const field = view?.field;
  if (!field) return;
  if (field.whole) return act({ edited: box.value });
  // A head is one line: a newline pasted into it becomes a space rather than a body nobody sees.
  if (/[\r\n]/.test(box.value)) {
    const at = box.selectionStart;
    box.value = box.value.replace(/[\r\n]+/g, ' ');
    box.setSelectionRange(at, at);
  }
  const at = field.text.indexOf('\n');
  act({ edited: at < 0 ? box.value : `${box.value}\n${field.text.slice(at + 1)}` });
});

// Whatever writes in the head reveals it before it lands, by a key or not: a paste, a cut, the
// browser's own undo, dictation, and a composition as it starts. So does a selection made in it.
// In the sheet the box is the body's, and writing there reveals nothing in the row.
const revealHere = () => {
  if (!view?.field?.whole) reveal();
};
box.addEventListener('beforeinput', revealHere);
box.addEventListener('compositionstart', revealHere);
document.addEventListener('selectionchange', () => {
  if (!revealed && !quiet && held === box && box.selectionStart !== box.selectionEnd) revealHere();
});

// Leaving an open body for somewhere else on the page closes it, writing it if it changed.
// Leaving the window does not: the body is there, still open, when the person comes back. Leaving
// a head does nothing: the field stays the cursor's, and a click on the line or a row says where
// the cursor goes.
box.addEventListener('blur', (event) => leftFor(event));

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

box.addEventListener('keydown', (event) => {
  if (event.isComposing || !view?.field) return;
  const plain = !event.shiftKey && !event.ctrlKey && !event.metaKey && !event.altKey;
  if (!view.field.whole && !revealed && revealsOn(event)) reveal();
  // The body is the box's own, but for Escape or Ctrl-[, which close it, and the chords swallowed
  // so that the browser's file dialog, page source, search and history do not open in the middle
  // of writing (see swallowedInBody). Tab is a tab character in it, as it is in the editor that
  // takes its place.
  if (view.field.whole) {
    if (swallowedInBody(event)) return event.preventDefault();
    if (event.key === 'Tab' && !event.ctrlKey && !event.metaKey && !event.altKey) {
      // Inserted as typing is, so that the browser's undo takes it back as it would a letter.
      if (!event.shiftKey) document.execCommand('insertText', false, '\t');
      return event.preventDefault();
    }
    if (!escapes(event)) return;
    act('close');
    event.preventDefault();
    return;
  }
  switch (escapes(event) ? 'Escape' : event.key) {
    case 'ArrowDown':
    case 'ArrowUp':
    case 'PageDown':
    case 'PageUp':
      // Control with an arrow moves the note, and the cursor with it.
      if (ctrlOnly(event) && event.key.startsWith('Arrow')) act({ shift: event.key === 'ArrowUp' ? -1 : 1 });
      else if (plain) act({ move: { ArrowDown: 1, ArrowUp: -1, PageDown: 10, PageUp: -10 }[event.key] });
      else return;
      break;
    // Tab goes into the note from anywhere in its head, and the arrows are the caret's. Shift-Tab
    // is nothing: the focus never leaves the field by a key.
    case 'Tab':
      if (event.ctrlKey || event.metaKey || event.altKey) return;
      if (!event.shiftKey) act('go_in');
      break;
    // Shift-Enter ticks the task, or archives the note, as Ctrl-V does in the terminal, and leaves
    // the row read as it was.
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
    default:
      if (!ctrlOnly(event) || !chord(event.key, true)) return;
  }
  event.preventDefault();
  follow();
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
  const at = view.field.fresh ?? (box.value === '' ? 0 : -1);
  return box.selectionStart === at && box.selectionEnd === at;
}

// The chords with Control alone that ask for something of the note under the cursor, or of the
// line, and say whether one did. They are the terminal's, where a browser lets them go: Ctrl-D
// deletes, or clears the line; Ctrl-O opens the body; Ctrl-U undoes, which costs only the
// browser's page source; Ctrl-C copies the note's address or its head, but only while nothing is
// selected, so that a selection is still copied as anywhere else; Ctrl-/ opens the note's address,
// which no browser binds. Ctrl-R, Ctrl-L and Ctrl-V are a browser's reload, address bar and paste,
// so redo, link and showing what is archived take keys a browser lets a page have: Ctrl-Y redoes,
// Ctrl-K links or unlinks the note, as it makes a link in most editors, and Ctrl-H shows or hides
// what is archived, as it shows what is hidden in a file chooser. Each is swallowed where it does
// nothing, as Ctrl-O is on the line, so that the browser's search and history never open from the
// page. The table is what each letter asks for of a row, and the foot writes a word's chord from
// it (see keyFor), so that the letter written after a word is the letter that asks for it.
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
      undo(name);
      return true;
    case 'show_archived':
      if (view.hints.some((hint) => named(hint.action) === name)) act(name);
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
// in the head (see the box's keys), anywhere else (see pressed) or as a chord, named as the
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
      return 'Tab';
    // Escape's rungs, off the row, off the text on the line and out of the place, each a word as
    // the way back; clearing the line is its cross as well.
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
  const field = [box, line].find((it) => it === document.activeElement);
  return field ? field.selectionStart === field.selectionEnd : getSelection()?.type !== 'Range';
}

// An undo or a redo, which leaves the row under the cursor revealed when it put text back in its
// head (see putBack).
function undo(which = 'undo') {
  const before = headsNow();
  act(which);
  if (before && putBack(before)) reveal();
}

// The focus goes where the cursor is: the box when it is on a row, the line when it is not.
function follow() {
  if (!view || !mayFocus()) return;
  if (view.cursor != null && boxIn && view.field?.whole && editor) {
    if (document.activeElement !== editor.contentDOM) editor.focus();
    return;
  }
  const target = view.cursor != null && boxIn ? box : line;
  if (document.activeElement !== target) target.focus({ preventScroll: true });
}

// The body's editor, which takes the box's place while a body is open. It is a module of its own,
// CodeMirror with vim's keys, fetched once the person has paused, no key for a little under two
// seconds after the first paint, and then when the page is idle; or at once if a body opens first.
// Never at first paint. Asking for it when the cursor first reached a row ran its 60 to 100 ms of
// evaluation just as the person began to walk the list, and asking at the first idle moment ran it
// on the first letters of a search typed as the page opened: an idle main thread is not a pause.
// Until it is here, and for good if it cannot be fetched, the box is the body as it always was.
let editor = null;
let editorSoon = null;
let vimOn = kept('fzn.vim') === 'on';

const soon = (task, timeout) =>
  window.requestIdleCallback ? requestIdleCallback(() => task(), timeout && { timeout }) : setTimeout(task, 1500);

let pause = 0;
function prefetchOnPause() {
  if (editorSoon) return document.removeEventListener('keydown', prefetchOnPause, true);
  clearTimeout(pause);
  pause = setTimeout(() => soon(loadEditor, 8000), 1750);
}

function loadEditor() {
  editorSoon ??= import('./vendor/editor.js').then(
    (module) => {
      editor = module.mount(editorHooks, $('sheet-foot'));
      editor.setVim(vimOn);
      // The sheet is the editor's but for the chords that never reach the browser, from the text
      // or from vim's prompt, the editor having them first where it binds them, Ctrl-U among
      // them (see swallowedInBody), and Escape or Ctrl-[, which close it without vim's keys. With
      // them Escape is vim's alone, since hands that press it twice to be sure of normal mode
      // would close the note under themselves; `:q`, `ZZ` and the word in the foot close it.
      buffer.addEventListener('keydown', (event) => {
        if (event.isComposing || editor.composing || !view?.field?.whole) return;
        if (swallowedInBody(event)) return event.preventDefault();
        if (!escapes(event) || event.defaultPrevented || editor.vimOn) return;
        act('close');
        event.preventDefault();
      });
      paintSoon();
    },
    () => {},
  );
}

const editorHooks = {
  onChange: (text) => {
    if (view?.field?.whole) act({ edited: text });
  },
  // The caret is read once the editor has finished with the change.
  onSelection: () => queueMicrotask(caretMoved),
  onMode: () => queueMicrotask(caretMoved),
  onFocus: () => {
    held = editor.contentDOM;
    if (!quiet) caretMoved();
  },
  // Going to vim's prompt for a command, at the foot of the sheet, is not leaving, and leftFor
  // knows it. Nor is a blur the editor causes itself by taking its content out of the page for a
  // moment, as the vim keys do to end a composition after a dead key.
  onBlur: (event) => {
    if (held === editor.contentDOM) held = null;
    placeCaret();
    if (!event.target.isConnected) return;
    leftFor(event);
  },
  // Vim's write keeps the body as it stands and stays; its quits close it, and the one with a
  // bang first puts the note back as it was when the cursor landed on it.
  onEx: (what) => {
    if (what === 'write') keepOpen();
    else {
      // Put back is asked for only when the engine offers it, since on a body that has not
      // changed undo would take back the last version written instead, which may be another
      // note's.
      if (what === 'discard' && view.hints.some((hint) => named(hint.action) === 'undo')) act('undo');
      act('close');
    }
    follow();
  },
};

// Writes an open body as it stands and leaves it open, for vim's `:w`: the engine keeps the field
// and stays, so the editor is left as it is, its caret, scroll and step undo with it.
function keepOpen() {
  if (view?.field?.whole) act('keep');
}

function rowKey(row, selected, apart) {
  const field = selected ? view.field : null;
  return JSON.stringify([
    // The head of the row under the cursor is the box's, so it is left out here: a row is not
    // filled again, taking the box out and putting it back, for a letter typed into it. With the
    // body open in the sheet the row reads its head as it is being written. Nor is its date in
    // the key, which a new note's first letter gives it: the date is set in place, since filling
    // the row would take the box out under a composition the first letter began.
    field && !field.whole ? null : field ? headOf(field.text) : row.shown ?? row.head,
    !!render,
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

// The head as read, laid over the box of the row under the cursor until the row is revealed, and
// set in place, so that the box under it is not taken out of the row and put back: it would lose
// the focus, and a composition the first letter began. It reads as the row would off the cursor.
function veil(li, row) {
  const edit = li.querySelector(':scope > .main > .edit');
  if (!edit) return;
  // The engine holds the cursor's row still, so a head changed in place, from elsewhere or by the
  // engine, is the field's and not yet the row's. It is shown as it now is, revealed, since a key
  // or a press would land against that text and not the reading's. The row's head is trimmed, as
  // the engine reads a head, and the field's is the text as typed, spaces and all, so the two are
  // compared trimmed.
  const now = headOf(view.field.text);
  if (now.trim() !== row.head) reveal();
  edit.classList.toggle('veiled', !revealed);
  const read = edit.querySelector(':scope > .head');
  if (revealed) return read?.remove();
  const key = JSON.stringify([now, row.shown ?? row.head, row.task, readings, !!render]);
  if (read?.dataset.reads === key) return;
  const head = readHead(row.shown ?? row.head, row.task);
  head.dataset.reads = key;
  // The box under it, which keeps its label, is what a screen reader reads.
  head.setAttribute('aria-hidden', 'true');
  if (read) read.replaceWith(head);
  else edit.prepend(head);
}

function fill(li, row, selected) {
  const field = selected ? view.field : null;
  const main = el('div', 'main');
  if (view.scores_shown && row.score != null) main.append(el('span', 'score', row.score.toFixed(2)));
  // Off the cursor a row reads as the engine shows it, without the place's labels at its end. Under
  // it the box is held in a frame of its own, which the head as read is laid in over it (see veil).
  const head = field ? headOf(field.text) : row.shown ?? row.head;
  if (field && !field.whole) {
    const edit = el('div', 'edit');
    edit.append(box);
    main.append(edit);
  } else main.append(readHead(head, row.task));
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
  const [from, to] = [box.selectionStart, box.selectionEnd];
  // The box's class says whether the field was the whole note last time, whichever of the two
  // held it.
  const wasWhole = box.classList.contains('whole');
  const hadEditor = !!editor?.dom.isConnected;
  wasTyping = typing();
  quiet = true;

  // The cursor has left the row the box was in: that row goes back to being read.
  const landed = fieldId !== boxIn;
  if (landed) revealed = field?.fresh != null || revealOnLanding;
  if (landed && boxIn) {
    const left = rows.get(boxIn);
    if (left) left.dataset.k = '';
    box.remove();
    editor?.dom.remove();
  }
  boxIn = fieldId;

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
    // its head is a text box, which an option of a listbox could not hold.
    if (selected && !li.hasAttribute('aria-current')) li.setAttribute('aria-current', 'true');
    else if (!selected && li.hasAttribute('aria-current')) li.removeAttribute('aria-current');
    // In a place, what the line finds in it is set apart from what it only finds there, where the
    // engine says the finds begin.
    const apart = i === view.finds_from;
    li.classList.toggle('apart', apart);
    li.classList.toggle('archived', row.archived);
    // A new note with nothing in it has nowhere to go in to, so its row has no arrow.
    li.classList.toggle('blank', selected && field?.fresh != null);
    const k = rowKey(row, selected, apart);
    if (li.dataset.k !== k) {
      li.dataset.k = k;
      fill(li, row, selected);
    }
    if (selected) {
      dateRow(li);
      veil(li, row);
    }
    return li;
  });

  listed = new Set(view.rows.map((row) => row.id));

  // Rows are put in order by moving only those that are out of it. A node that is moved is taken
  // out and put back, which would restart its fade and drop the focus of the box.
  let at = list.firstChild;
  for (const li of wanted) {
    if (li === at) at = at.nextSibling;
    else list.insertBefore(li, at);
  }

  // An open body is the sheet's: the editor once it has arrived, and the box until then. Neither
  // is left there once the body has closed, when the box is back in its row.
  const opened = field && boxIn && field.whole ? (editor ? editor.dom : box) : null;
  if (opened && opened.parentElement !== sheet) sheet.prepend(opened);
  for (const other of [box, editor?.dom]) if (other && other !== opened && other.parentElement === sheet) other.remove();

  // The box holds what the field says, set only when that differs from what it holds, so that the
  // person's caret is left alone while they type. The caret goes where the engine put it, which on
  // landing, and when the body opens or closes, is the end of the head, so that Down in a body goes
  // to the second line, and in a new row with only the labels it opened with is after a task's box
  // and before the rest; otherwise it is kept where it was, as far as the text still reaches.
  if (field && boxIn && field.whole && editor) {
    box.classList.add('whole');
    // The editor is given a fresh state when the body opens, its caret where the box's would be,
    // and when it takes over from the box, with the box's caret. Otherwise it is told the text only
    // when the engine's differs from it: put back, a date word expanded, a version from elsewhere.
    // A newline just made at the end is not a difference, as it is not one to the engine, which
    // keeps the note without it: told the kept text, the editor would take the new line away under
    // the hands.
    const toggled = !wasWhole;
    const kept = (text) => text.replace(/[\r\n]+$/, '');
    if (toggled) editor.open(field.text, placed != null ? unitsIn(field.text, placed) : headOf(field.text).length);
    else if (!hadEditor) editor.open(field.text, Math.min(from, field.text.length));
    else if (kept(editor.text()) !== kept(field.text)) editor.setText(field.text);
    if (takesFocus(editor.contentDOM)) editor.focus();
  } else if (field && boxIn) {
    box.classList.toggle('whole', field.whole);
    const want = boxText(field);
    const told = box.value !== want;
    if (told) {
      box.value = want;
      fit();
    }
    const end = box.value.length;
    // A press on a head's words puts the caret where it landed, but in a new row with only its
    // labels, whose caret goes where the writer's text does. A row landed on reads from its start,
    // whatever the box was scrolled to in the row before. A row read has its caret at the end,
    // where its block is drawn (see placeReadingCaret), which is where the engine puts it when it
    // changes the head under it, so that the first key types or moves from where the block was.
    const pressed = landed && field.fresh == null && typeof wantsText === 'number';
    const at = pressed ? Math.min(wantsText, end) : placed != null ? Math.min(unitsIn(want, placed), end) : null;
    if (landed) box.scrollLeft = 0;
    if (at != null) box.setSelectionRange(at, at);
    else box.setSelectionRange(Math.min(from, end), Math.min(to, end));
    if (landed || told) edges();
    if (takesFocus(box)) box.focus({ preventScroll: true });
  } else if (takesFocus(line)) {
    line.focus({ preventScroll: true });
  }
  wantsText = false;
  placed = null;
  quiet = false;

  // The list follows the cursor when the cursor moves, and not otherwise: a note arriving from
  // elsewhere, or a head being written, must not pull the list from where it was scrolled to. A new
  // list with the cursor on the line starts at its top, though the list before it was scrolled, as
  // a phone's resting list can be.
  const under = view.cursor != null ? view.rows[view.cursor]?.id ?? '' : '';
  if (under !== list.dataset.under) {
    if (under) rows.get(under)?.scrollIntoView({ block: 'nearest', inline: 'nearest' });
    else list.scrollTop = 0;
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
  const way = ['back', 'select', 'type', 'close'].includes(last) ? hints.pop() : null;
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

// An undo or a redo that put text back in the head under the cursor leaves the row revealed, as
// it was being written; one that only moved the cursor, or put back no more than a task's box, as
// undoing a tick does, leaves it read. What each head was is taken before it: the field's for the
// row under the cursor, which the engine holds still, and the row's for the others, which the
// engine trims, so both are compared trimmed.
const unboxed = (head) => head.replace(/^\[[ xX]\] ?/, '');
function headsNow() {
  const heads = new Map(view.rows.map((row) => [row.id, row.head]));
  if (boxIn && view.field && !view.field.whole) heads.set(boxIn, headOf(view.field.text));
  return heads;
}
function putBack(before) {
  if (!boxIn || !view.field || view.field.whole || !before.has(boxIn)) return false;
  return unboxed(before.get(boxIn).trim()) !== unboxed(headOf(view.field.text).trim());
}

// A word in the foot, with the key that asks for the same written after it, which the stylesheet
// shows only where there is a pointer to read it with. A word does what its key does, after which
// the focus follows the cursor.
function word(hint) {
  const name = named(hint.action);
  const run = () => {
    if (name === 'undo' || name === 'redo') return undo(name);
    // A new note is asked for to be written in, so the keyboard comes for it, and it lands
    // revealed.
    if (name === 'write') wantsText = revealOnLanding = true;
    try {
      act(hint.action);
    } finally {
      revealOnLanding = false;
    }
  };
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
      run();
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
  const read = render ? render.head(text) : text;
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
  // thing to press. On a narrow screen a long path gives up its middle.
  set(path, JSON.stringify([view.path, narrow.matches, !!render]), () => {
    const steps = [space, ...view.path];
    const parts = [];
    steps.forEach((head, depth) => {
      const middle = narrow.matches && steps.length > 3 && depth > 0 && depth < steps.length - 1;
      if (depth > 0) parts.push(el('span', middle ? 'gone' : null, '›'));
      if (middle && depth === 1) parts.push(el('span', null, '…'));
      const step = el('button', middle ? 'gone' : null);
      step.append(depth > 0 ? readStep(head) : head);
      step.type = 'button';
      step.dataset.depth = depth;
      if (depth === steps.length - 1) step.setAttribute('aria-current', 'location');
      parts.push(step);
    });
    return parts;
  });
}

// A body read, beside the list or above the line, is painted when it has changed, and says
// whether it had. A long one is rendered at first only as far as either shows, and whole once the
// page is idle, so that the arrows walk onto a long document without waiting for all of it to be
// laid out.
const GLANCE = 5000;
function paintBody(node, says) {
  const key = `${readings} ${says}`;
  if (!set(node, key, () => [readBody(says, GLANCE)])) return false;
  if (render && says.length > GLANCE) {
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
function paintPane() {
  const field = view.field;
  const says =
    !wide.matches || field?.whole ? '' : field ? bodyOf(field.text) : view.here ? bodyOf(view.here.text) : '';
  paintBody(paneBody, says);
  paintPaneHead();
  const under = (view.cursor != null ? view.rows[view.cursor]?.id : view.here?.id) ?? '';
  if (pane.dataset.under !== under) {
    pane.dataset.under = under;
    pane.scrollTop = 0;
  }
}

function paintPaneHead() {
  const field = view.field;
  const row = wide.matches && field && !field.whole && boxIn ? rows.get(boxIn) : null;
  const read = row && !revealed ? row.querySelector('.edit > .head') : null;
  const cut = row && (read ? read.scrollWidth > read.clientWidth + 1 : box.scrollWidth > box.clientWidth + 1);
  paneHead.hidden = !cut;
  if (!cut) return set(paneHead, '', () => []);
  const listed = view.rows[view.cursor];
  // Read, it is the row's reading; written in, the head as typed, its box drawn while it still
  // begins with one.
  const text = revealed ? headOf(field.text) : (listed.shown ?? listed.head);
  const boxed = /^\[[ xX]\](?: |$)/.test(text);
  const task = revealed ? (listed.task != null && boxed ? /^\[[xX]\]/.test(text) : null) : listed.task;
  set(paneHead, JSON.stringify([text, task, readings, !!render]), () => [readWhole(text, task)]);
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
  head.append(render ? render.head(text) : text);
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
  $('go-in').hidden = !(view.cursor == null && view.hints.some((hint) => named(hint.action) === 'go_in'));
  $('dates').textContent = view.dates.length ? `→ ${view.dates.join(', ')}` : '';

  // The note you are in, when it says more than its head and the page has no room for it beside
  // the list, clamped to six lines; a tap gives the rest, until another note is gone into.
  const here = $('here');
  const told = view.here && !wide.matches ? bodyOf(view.here.text) : '';
  here.hidden = !told;
  const into = view.here?.id ?? '';
  const arrived = here.dataset.into !== into;
  if (arrived) {
    here.dataset.into = into;
    here.classList.remove('open');
  }
  if (paintBody(here, told) || arrived) clampHere();

  $('empty').hidden = !(view.notes === 0 && !zoomed && view.line === '' && view.cursor == null);

  paintRows();
  paintPane();
  // A body opened is read from its first line.
  if (whole && !wasWhole) sheet.scrollTop = 0;
  paintHints();
  paintFoot();
  caretMoved();
}

line.addEventListener('input', () => act({ type: line.value }));

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
// Where it goes is measured in a copy of the field, never seen: the text up to the caret, the
// letter at the caret in a span, and the rest. That is one read of the layout, made here and
// nowhere else, so the painting of the rows stays free of it.
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
// What the caret was last measured for. The block is a child of the field's own parent, so rows
// that move above it carry it with them; it is measured again only when something in the field
// has changed: its text, its selection, its row, its weight, or the dates that take width from
// the line. An event that changes none of it, such as the selection set again by the painter, is
// not measured twice.
let measured = null;
const inEditor = () => !!editor && held === editor.contentDOM;
// On a touch screen the caret is drawn where the cursor is whether or not the keyboard is up, still
// rather than breathing, so that the blank page has its cursor before a finger asks for the
// keyboard: in the box when the cursor is on a row, and on the line otherwise. A field without the
// focus keeps the selection last set in it, which the painter sets as the cursor lands.
const restingField = () =>
  coarse.matches && view && !view.field?.whole && !document.body.classList.contains('locked')
    ? (view.cursor != null && boxIn ? box : line)
    : null;
const caretField = () => (held && held === document.activeElement ? held : restingField());
const asMeasured = () => {
  const field = caretField();
  return (
    field &&
    [field, field.selectionStart, field.selectionEnd, field.value, field.parentElement, faceOf(field),
      $('dates').textContent, view?.cursor, composing, field === document.activeElement, revealed,
      inEditor() ? [editor.text().length, editor.selection().anchor, editor.selection().head, editor.mode,
        editor.composing, editor.dom.parentElement] : ''].join('\u0000')
  );
};
const caretMoved = () => {
  if (asMeasured() !== measured) placeCaret();
};

// The mirror is given the field's face, when the field, its class or the window's width has
// changed.
function dress(field) {
  const style = getComputedStyle(field);
  const face = `${field.id} ${faceOf(field)} ${innerWidth}`;
  if (mirror.dataset.face !== face) {
    mirror.dataset.face = face;
    for (const name of COPIED) mirror.style[name] = style[name];
    mirror.style.whiteSpace = field === box ? style.whiteSpace : 'pre';
    mirror.style.overflowWrap = field === box ? style.overflowWrap : 'normal';
  }
  return style;
}
// A field's class but for the fades at its edges, which change nothing that is measured.
const faceOf = (field) => field.className.replace(/ (before|after)\b/g, '');

// Where the lines of a field's words are on the screen, measured in the mirror, whose box lies
// over the field's as the caret's measure has it.
function wordsOf(field) {
  const style = dress(field);
  const padding = parseFloat(style.paddingLeft) + parseFloat(style.paddingRight);
  mirror.style.width = `${field.clientWidth - padding}px`;
  const words = document.createTextNode(field.value);
  mirror.replaceChildren(words);
  const range = document.createRange();
  range.selectNodeContents(words);
  const from = mirror.getBoundingClientRect();
  const to = field.getBoundingClientRect();
  const [dx, dy] = [to.left - from.left - field.scrollLeft, to.top - from.top - field.scrollTop];
  return [...range.getClientRects()].map((r) => ({
    left: r.left + dx,
    right: r.right + dx,
    top: r.top + dy,
    bottom: r.bottom + dy,
  }));
}

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
  const shown =
    field &&
    view &&
    !composing &&
    !document.body.classList.contains('locked') &&
    field.selectionStart === field.selectionEnd &&
    // A row not yet revealed has its caret drawn on the head as read, which is over the box (see
    // placeReadingCaret). In the sheet the box is the body's, revealed or not.
    (field === box ? revealed || !!view.field?.whole : view.cursor == null);
  measured = asMeasured();
  if (field && inEditor()) return placeEditorCaret();
  const read = field === box && !revealed && !!boxIn && !!view?.field && !view.field.whole;
  if (read && !composing && !document.body.classList.contains('locked')) return placeReadingCaret();
  if (!shown) {
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
  // At the end of the text, or of a line in it, there is no letter, so the block is a little over
  // half a letter wide, measured on a no-break space, which wraps with the line's last word rather
  // than hanging past the end of a full line as a space does. Otherwise it is over the whole letter
  // after the caret, an emoji or a letter and its accent.
  const end = at >= text.length || text[at] === '\n';
  const letter = end ? '' : text.slice(at, lettersOf(text.slice(at, at + 16))[1] + at);
  const span = el('span', null, end ? '\u00a0' : letter);
  mirror.replaceChildren(text.slice(0, at), span, text.slice(at + letter.length));
  const width = `${field.clientWidth - padding}px`;
  if (mirror.style.width !== width) mirror.style.width = width;

  // The read. The block is the height of the line, centred on the letter as the line's own leading
  // is, and never narrower than a third of a letter, so that it is still a block over an i. A head
  // being written on one line is scrolled to the caret before the field's place is taken.
  const { offsetLeft, offsetTop, offsetWidth, offsetHeight } = span;
  if (field === box && style.whiteSpace === 'pre') sidescroll(offsetLeft, size);
  const [fieldLeft, fieldTop] = [field.offsetLeft - field.scrollLeft, field.offsetTop - field.scrollTop];
  const tall = parseFloat(style.lineHeight) || offsetHeight;
  const wide = end ? 0.55 * size : Math.max(offsetWidth, 0.35 * size);
  const left = fieldLeft + offsetLeft - (wide - (end ? wide : offsetWidth)) / 2;
  const top = fieldTop + offsetTop - (tall - offsetHeight) / 2;

  const parent = field.parentElement;
  if (caret.parentElement !== parent) parent.append(caret);
  caret.classList.remove('bar');
  caret.classList.toggle('still', field !== document.activeElement);
  showCaret(left, top, wide, tall);
}

// A row not yet revealed is read, and its caret is a block at the end of the reading, or on its `…`
// where the row cuts it, where the terminal puts its cursor and where the box's caret is (see
// paintRows), so that the first key types or moves from where the block was. It is measured on
// the reading, which is what is shown, the size of the block at the end of a head being written.
function placeReadingCaret() {
  const frame = box.parentElement;
  const read = frame?.querySelector(':scope > .head');
  if (!read) {
    caret.hidden = true;
    caretWas = '';
    return;
  }
  const size = parseFloat(getComputedStyle(box).fontSize);
  const tall = parseFloat(getComputedStyle(box).lineHeight) || 1.5 * size;
  const wide = 0.55 * size;
  const words = document.createRange();
  words.selectNodeContents(read);
  const lines = [...words.getClientRects()].filter((r) => r.width || r.height);
  const shown = read.getBoundingClientRect();
  const last = lines[lines.length - 1] ?? shown;
  const cut = read.scrollWidth > read.clientWidth + 1;
  // The reading's end is the furthest right anything on its last line reaches, so that a head
  // ending in code has the block after the code's wash rather than inside it.
  const onLast = lines.filter((r) => r.bottom > last.top && r.top < last.bottom);
  const end = Math.max(...onLast.map((r) => r.right));
  const x = cut ? shown.right - wide : lines.length ? end : shown.left;
  const at = frame.getBoundingClientRect();
  const left = x - at.left - frame.clientLeft;
  const top = (last.top + last.bottom) / 2 - tall / 2 - at.top - frame.clientTop;
  if (caret.parentElement !== frame) frame.append(caret);
  caret.classList.remove('bar');
  caret.classList.toggle('still', box !== document.activeElement);
  showCaret(left, top, wide, tall);
}

// A head being written is one line, and scrolls sideways under the caret as any one-line field
// does: the text moves only when the caret would come within five letters of an edge, as vim's
// sidescrolloff keeps them, which a browser honouring the box's scroll-padding does for itself and
// this does for every browser. A press on the words that revealed the row keeps the letter pressed
// under the pointer, however much longer the source is than the reading was, as far as the five
// letters allow: a letter pressed by an edge, on the `…` or past where the box ends on the cursor,
// a row read off the cursor running further right than its box, is brought in by as much as it
// takes to be clear of the fade. Each edge that hides text fades. What it writes is kept, so that
// the scroll it causes is not taken for one the browser made.
let pressedAt = null;
let wrote = null;
function sidescroll(x, size) {
  const room = box.clientWidth;
  const max = box.scrollWidth - room;
  const context = Math.min(2.5 * size, room / 2);
  let to = pressedAt != null ? x - (pressedAt - box.getBoundingClientRect().left) : box.scrollLeft;
  to = Math.min(Math.max(to, x - room + context), x - context);
  // A pixel or two from an end is the end: the reading and the box can differ by less than a
  // pixel, and a head scrolled by one would fade its first letters.
  to = Math.max(0, Math.min(max, Math.round(to)));
  if (to <= 2) to = 0;
  else if (to >= max - 2) to = max;
  if (Math.abs(to - box.scrollLeft) > 1) {
    box.scrollLeft = to;
    wrote = box.scrollLeft;
  }
  edges();
}
function edges() {
  const sideways = !box.classList.contains('whole') && getComputedStyle(box).whiteSpace === 'pre';
  const over = box.scrollWidth - box.clientWidth;
  box.classList.toggle('before', sideways && box.scrollLeft > 1);
  box.classList.toggle('after', sideways && box.scrollLeft < over - 1);
}
// A scroll the browser makes itself, as a paste or a composition running past an edge does, moves
// the fades and the caret with it; one sidescroll made has placed the caret already.
box.addEventListener(
  'scroll',
  () => {
    edges();
    if (wrote != null && Math.abs(box.scrollLeft - wrote) <= 1) {
      wrote = null;
      return;
    }
    wrote = null;
    measured = null;
    placeCaret();
  },
  { passive: true },
);

function showCaret(left, top, wide, tall) {
  caret.style.cssText = `left:${left}px;top:${top}px;width:${wide}px;height:${tall}px`;
  caret.hidden = false;
  // A caret that has moved starts its breath again from solid.
  const now = `${left} ${top} ${wide}`;
  if (now !== caretWas) for (const breath of caret.getAnimations?.() ?? []) breath.currentTime = 0;
  caretWas = now;
}

// In the editor there is no copy to measure: the editor says where the caret is and where the
// letter after it ends. The block is sized and centred as it is in the box. In vim's insert mode
// it is a bar at the caret, as a terminal draws vim's. The editor is in the sheet, which scrolls,
// so the block is placed in what the sheet scrolls and moves with the text.
function placeEditorCaret() {
  const c = editor.caret();
  const parent = editor.dom.parentElement;
  if (!view || !parent || !c.empty || !c.rect || composing || editor.composing) {
    caret.hidden = true;
    caretWas = '';
    return;
  }
  const style = getComputedStyle(editor.contentDOM);
  const size = parseFloat(style.fontSize);
  const tall = parseFloat(style.lineHeight) || c.rect.bottom - c.rect.top;
  const bar = c.mode === 'insert';
  const letter = c.right ? c.right.left - c.rect.left : 0;
  const wide = bar ? 2 : c.right ? Math.max(letter, 0.35 * size) : 0.55 * size;
  const frame = parent.getBoundingClientRect();
  const inset = bar ? 1 : c.right ? (wide - letter) / 2 : 0;
  const left = c.rect.left - frame.left - parent.clientLeft + parent.scrollLeft - inset;
  const top =
    c.rect.top - frame.top - parent.clientTop + parent.scrollTop - (tall - (c.rect.bottom - c.rect.top)) / 2;
  if (caret.parentElement !== parent) parent.append(caret);
  caret.classList.toggle('bar', bar);
  showCaret(left, top, wide, tall);
}

for (const field of [line, box]) {
  field.addEventListener('focus', () => {
    held = field;
    if (!quiet) caretMoved();
  });
  field.addEventListener('blur', () => {
    if (held === field) held = null;
    placeCaret();
  });
  field.addEventListener('input', caretMoved);
  // Some browsers say a text field's selection changed to the field and not to the document.
  field.addEventListener('selectionchange', () => {
    if (held === field) caretMoved();
  });
  field.addEventListener('compositionstart', () => {
    composing = true;
    placeCaret();
  });
  field.addEventListener('compositionend', () => {
    composing = false;
    placeCaret();
  });
}
for (const moment of ['focus', 'blur', 'input']) secret.addEventListener(moment, () => placeCaret());
document.addEventListener('selectionchange', () => {
  if (held && document.activeElement === held) caretMoved();
});
window.addEventListener('resize', () => {
  placeCaret();
  clampHere();
  // Whether the head under the cursor is cut, and so read beside the list, follows the width.
  if (view && wide.matches) paintSoon();
});

const ctrlOnly = (event) => event.ctrlKey && !event.shiftKey && !event.metaKey && !event.altKey;

// The keys, wherever the focus has got to but the box and the sheet, which have their own. A key
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
  if (event.target === box || event.composedPath().includes(buffer)) return;
  const onButton = event.target instanceof HTMLButtonElement;
  if (onButton && (event.key === 'Enter' || event.key === ' ')) return;
  const onLine = view.cursor == null;
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
  // opens its body, and on the line does nothing; none asks for the browser's bookmark or file
  // dialog, its search or its history, either way (see chord). Ctrl-[ is Escape.
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
    // The note Enter writes from the line is still being written, so it lands revealed. On a row
    // Shift-Enter ticks; on the line it is Enter.
    case 'Enter':
      if (event.shiftKey && !onLine) {
        act('archive');
        break;
      }
      revealOnLanding = onLine;
      try {
        act('write');
      } finally {
        revealOnLanding = false;
      }
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
  if (view?.field?.whole && event.target.closest('.row')?.dataset.id === boxIn) event.preventDefault();
});
buffer.addEventListener('mousedown', (event) => {
  if (event.target === buffer || event.target === sheet) event.preventDefault();
});
// On a touch screen the body need not have the focus, the keyboard being down, so a press beside
// the sheet closes it here rather than by the blur that closes it on a desktop. A press on its row
// behind it, or on a word in the foot, does not, as on a desktop.
document.addEventListener('mousedown', (event) => {
  if (!coarse.matches || !view?.field?.whole || event.target.closest('#buffer, #foot')) return;
  if (event.target.closest('.row')?.dataset.id === boxIn) return;
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

// Whether a point is on the words of a head as read, a few pixels round each line of them
// counting, since a finger is not a point: false when it is not, and otherwise where in the head's
// text as typed, its source, or true, for the end, when that cannot be counted. A head read has its
// marks taken out, but every piece of text it shows is a piece of its source, in order: a link's
// words, an escaped letter, an address without its scheme. So each piece is found in the source
// after the last, from past a task's box, and the letter pressed is counted from where its piece
// was found, which puts a press on a link's word in that word. The address of a link written
// `[words](address)` is not shown, so a piece that follows the link is looked for past it, or a
// full stop after the link would be found inside the address.
const SLOP = 6;
const near = (r, x, y) => x >= r.left - SLOP && x <= r.right + SLOP && y >= r.top - SLOP && y <= r.bottom + SLOP;
function hitText(head, source, x, y) {
  if (!head) return false;
  const words = document.createRange();
  words.selectNodeContents(head);
  if (![...words.getClientRects()].some((r) => near(r, x, y))) return false;
  const at = letterAt(x, y);
  let from = Number(head.dataset.boxed ?? 0);
  let link = null;
  const pieces = document.createTreeWalker(head, NodeFilter.SHOW_TEXT);
  for (let piece = pieces.nextNode(); piece; piece = pieces.nextNode()) {
    const inLink = piece.parentElement?.closest('a') ?? null;
    if (link && inLink !== link && source.startsWith('](', from)) from = pastAddress(source, from + 2);
    link = inLink;
    const found = source.indexOf(piece.data, from);
    if (found < 0) return true;
    if (piece === at?.offsetNode) return found + at.offset;
    from = found + piece.data.length;
  }
  return true;
}

// Where the address of a link ends, from just inside its bracket: past the bracket that closes it,
// counting those opened inside it.
function pastAddress(source, at) {
  for (let depth = 1; at < source.length; at++) {
    if (source[at] === '(') depth++;
    else if (source[at] === ')' && --depth === 0) return at + 1;
  }
  return at;
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
  const writing = was != null && boxIn === was ? [box.selectionStart, box.selectionEnd] : null;
  holdFocus = true;
  try {
    act({ select: at });
    act('archive');
    const back = view.rows.findIndex((row) => row.id === was);
    if (was == null) act({ select: null });
    else if (was !== task && back >= 0) {
      act({ select: back });
      if (writing && boxIn === was) box.setSelectionRange(...writing);
    }
  } finally {
    holdFocus = false;
  }
  if (!had) return;
  wantsText = true;
  follow();
  wantsText = false;
}

// The text and the place in it under a point, by the standard's call or by WebKit's older one,
// which Safari before 18 has alone.
function letterAt(x, y) {
  if (document.caretPositionFromPoint) return document.caretPositionFromPoint(x, y);
  const range = document.caretRangeFromPoint?.(x, y);
  return range && { offsetNode: range.startContainer, offset: range.startOffset };
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
// tap on the words puts the cursor there too and writes in it, revealed, with the caret where it
// landed, which on a phone is what raises the keyboard. On the row under the cursor, not yet
// revealed, a press on the words is the same, a link's word included, which is not followed there;
// one beside them on a desktop does nothing, the note being picked already. The arrow on the
// selected row goes in, and a link in any other row opens and does nothing else.
list.addEventListener('click', (event) => {
  const item = event.target.closest('.row');
  if (dragged || (fromTick && !event.target.closest('.tick'))) {
    dragged = fromTick = false;
    return;
  }
  if (!item || event.target === box) return;
  const at = view.rows.findIndex((row) => row.id === item.dataset.id);
  if (at < 0) return;
  const veiled = at === view.cursor && !revealed && !view.field?.whole && event.target.closest('.edit');
  if (event.target.closest('a') && !veiled) return;
  const fresh = touched && Date.now() - touched.at < 1000;
  const [x, y] = fresh ? [touched.x, touched.y] : [event.clientX, event.clientY];
  const ticker = event.target.closest('.tick');
  const reach = (node) => node.getBoundingClientRect().right + 0.4 * parseFloat(getComputedStyle(node).fontSize);
  if (ticker && (!fresh || x <= reach(ticker))) return tick(at);
  if (veiled) {
    event.preventDefault();
    const hit = hitText(veiled.querySelector(':scope > .head'), box.value, x, y);
    if (hit !== false) revealAt(hit === true ? box.value.length : hit, x);
    return;
  }
  if (event.target.closest('.chev')) act('go_in');
  else if (at !== view.cursor) {
    wantsText = hitText(item.querySelector('.head'), view.rows[at].head, x, y);
    revealOnLanding = wantsText !== false;
    if (typeof wantsText === 'number') pressedAt = x;
    try {
      act({ select: at });
    } finally {
      revealOnLanding = false;
      pressedAt = null;
    }
  } else if (!view.field?.whole) act('open');
  follow();
  wantsText = false;
});
// A press on the head as read over the box keeps the focus in the box and starts no selection in
// the reading; the click says what it did. The middle and right buttons, anywhere in the head's
// frame, reveal the row with the focus in the box and the caret under the pointer before their
// default would run, so that they act on the box as they always did: the middle button's paste of
// what is selected elsewhere, and the menu with Paste in it. Their own default is not let run,
// since the element pressed is gone by then and it would give the focus to the page. A drag from
// the words, not from a task's box, which only ticks, reveals the row once it has gone a few
// pixels, from where it began, and selects in the box as far as the pointer goes, as a drag in the
// box always did; the click that ends it does nothing more. A press on a task's box that is let go
// off it does nothing, as a button let go off itself does, rather than landing on the words.
let dragFrom = null;
let dragged = false;
let fromTick = false;
list.addEventListener('mousedown', (event) => {
  dragged = false;
  dragFrom = null;
  fromTick = !!event.target.closest('.tick');
  const veiled = !revealed && event.target.closest('.edit.veiled');
  if (!veiled) return;
  event.preventDefault();
  const at = hitText(veiled.querySelector(':scope > .head'), box.value, event.clientX, event.clientY);
  if (event.button === 1 || event.button === 2) {
    return revealAt(typeof at === 'number' ? at : box.value.length, typeof at === 'number' ? event.clientX : null);
  }
  if (event.button !== 0 || (touched && Date.now() - touched.at < 1000)) return;
  if (event.target.closest('.tick')) return;
  if (at !== false) dragFrom = { x: event.clientX, y: event.clientY, at: at === true ? box.value.length : at, row: boxIn };
});
document.addEventListener('mousemove', (event) => {
  if (!dragFrom) return;
  if (!(event.buttons & 1)) {
    dragFrom = null;
    return;
  }
  if (!dragged) {
    if (Math.hypot(event.clientX - dragFrom.x, event.clientY - dragFrom.y) < 4) return;
    dragged = true;
    revealAt(dragFrom.at, dragFrom.x);
  }
  dragFrom.to = { x: event.clientX, y: event.clientY };
  dragTo();
});
// A drag ends with its button, and with a key or the window lost, either of which may move the
// cursor, and the box with it, to another row.
function endDrag() {
  dragFrom = null;
  cancelAnimationFrame(dragging);
}
document.addEventListener('mouseup', endDrag);
document.addEventListener('keydown', endDrag, true);
window.addEventListener('blur', endDrag);

// The drag began on the reading, whose press was not the box's, so the browser does not scroll the
// box as a drag past its edge would: the page does. While the pointer is past an edge that hides
// text the box scrolls towards it by as far as it is past, a frame at a time for as long as it is
// held there, and the selection's moving end is the letter just inside that edge, so that it is
// always in sight and nothing hidden is selected without being shown.
let dragging = 0;
function dragTo() {
  if (!dragFrom?.to || !dragged) return;
  // A drag belongs to the row it began in: one whose box has gone to another row is over.
  if (boxIn !== dragFrom.row || !box.isConnected || box.classList.contains('whole')) return endDrag();
  const { x, y } = dragFrom.to;
  const r = box.getBoundingClientRect();
  const sideways = getComputedStyle(box).whiteSpace === 'pre';
  const over = sideways ? (x > r.right ? x - r.right : x < r.left ? x - r.left : 0) : 0;
  if (over) box.scrollLeft = Math.max(0, Math.min(box.scrollWidth - box.clientWidth, box.scrollLeft + over));
  const inside = sideways ? Math.max(r.left + 1, Math.min(r.right - 1, x)) : x;
  const to = offsetInBox(inside, sideways ? (r.top + r.bottom) / 2 : y);
  const from = dragFrom.at;
  box.setSelectionRange(Math.min(from, to), Math.max(from, to), to < from ? 'backward' : 'forward');
  caretMoved();
  cancelAnimationFrame(dragging);
  if (over) dragging = requestAnimationFrame(dragTo);
}

// Where in the box's text a point is, by the browser where it says so for a text box, and
// otherwise its start or its end, by which side of the box the point is on.
function offsetInBox(x, y) {
  const at = letterAt(x, y);
  if (at?.offsetNode === box) return at.offset;
  const r = box.getBoundingClientRect();
  return y < r.top || (y <= r.bottom && x < r.left) ? 0 : box.value.length;
}
// A browser takes a tap near a text box into the box, and the head under the cursor is a box as
// wide as the row, so a second tap beside its words, on its own line or on the date under it,
// would only place the caret. So where the finger landed is read from the touch, which neither
// browser moves, and judged as the first tap is: off the box, or in it past the lines of its
// words, it opens the body. It is heard on the list, since WebKit sends a touch to what is under
// the finger and Chromium to the box. While the head is being written a tap in it is the caret's,
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
  if (!view?.field || view.field.whole || !boxIn || (revealed && document.activeElement === box)) return false;
  const row = rows.get(boxIn);
  const within = (r) => x >= r.left && x <= r.right && y >= r.top && y <= r.bottom;
  if (!row || !within(row.getBoundingClientRect())) return false;
  const chev = row.querySelector('.chev');
  if (chev && within(chev.getBoundingClientRect())) return false;
  // Not yet revealed, the words are those of the head as read, a task's box among them.
  const read = !revealed && row.querySelector('.edit > .head');
  if (read) return hitText(read, box.value, x, y) === false;
  if (!near(box.getBoundingClientRect(), x, y)) return true;
  return box.value !== '' && !wordsOf(box).some((r) => near(r, x, y));
}

list.addEventListener('animationend', (event) => event.target.classList.remove('in'));

// A tap on the note you are in gives the rest of it, unless it was on a link, which opens and does
// nothing else.
$('here').addEventListener('click', (event) => {
  if (!event.target.closest('a')) $('here').classList.add('open');
});

// Each step of the path goes back to where it was.
$('path').addEventListener('click', (event) => {
  const depth = event.target.dataset?.depth;
  if (depth === undefined) return;
  for (let steps = view.path.length - Number(depth); steps > 0; steps--) act('back');
  follow();
});

// A control that is not text never takes the focus: the words in the foot, the cross and the
// arrows, a step of the path act on where the cursor is and leave the focus, and so a phone's
// keyboard, where it was; "put back" is about the note, and an open body is not closed first. On
// a touch screen a press on a row while a field has the focus leaves it there too, so that the
// keyboard stays up while the cursor moves (see the list's click).
for (const part of [$('top'), $('path'), $('foot'), list]) {
  part.addEventListener('mousedown', (event) => {
    if (event.target.closest('button')) event.preventDefault();
    else if (part === list && coarse.matches && typing() && event.target !== box) event.preventDefault();
  });
}

// A page put away with the cursor on a row writes what the row holds, if it changed. Being hidden
// comes before being unloaded and leaves more time for the line to be kept and sent, so it is
// heard too. A hidden page keeps the field and stays, an open body open, since switching tabs to
// look something up and coming back is not leaving it; only a page that is going leaves the row,
// and a second time with nothing changed does nothing. Writing may change the head under the
// caret, as a new note written inside a tag gets the tag, so the box takes it at once: a hidden
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
  (!!view?.field?.whole && view.hints.some((hint) => named(hint.action) === 'undo'));

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
  requestAnimationFrame(prefetchOnPause);
  document.addEventListener('keydown', prefetchOnPause, true);
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
