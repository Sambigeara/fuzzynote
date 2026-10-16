// The browser face: a blank page with one cursor on it.
//
// The engine is the app. It runs here as WebAssembly and is told what was asked for, as an
// action, and says what to show. This file is what a page adds to it: which key or tap asks
// for what, painting the view, the socket to the relay this page came from, the other tabs open
// on the space, and somewhere to keep lines between visits.
//
// There is one cursor on the page and it moves. On the line it is the caret, and typing finds. Down
// puts it on a row, whose head becomes a text box with the caret at its end, so typing there
// changes the head and nothing has to be pressed to begin or to keep it: the engine writes each
// change as it is typed. Enter with text on the line writes it and puts the box in the note it
// wrote. Enter on a row opens a new note under it, its empty head the box, so that Enter, type,
// Enter, type writes notes in a run, as the original's Enter did. Ctrl-O opens the body, the
// original's chord for it, in the sheet: a bounded overlay over the dimmed page, scrolled on its
// own, where the box holds the whole note, or an editor takes its place once one has been fetched
// (see loadEditor). The arrows stay inside it, and Escape closes it, or with vim's keys `:q`;
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
// gets one.
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

// A space is asked for by name: /?space=work. A name has to be a plain one.
const asked = new URLSearchParams(location.search).get('space') || '';
const space = /^[A-Za-z0-9_-][A-Za-z0-9._-]{0,63}$/.test(asked) ? asked : 'main';
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
if (given) {
  keep(TOKEN, given[1] ? `${given[1]}:${given[2]}` : given[2]);
  history.replaceState(history.state, '', location.pathname + location.search);
}
const credential = kept(TOKEN) || '';
const whose = credential.includes(':') ? credential.slice(0, credential.indexOf(':')) : '';
const token = credential.slice(credential.indexOf(':') + 1);

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
  status.classList.remove('faded');
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
    // A build given up on is carried into each new entry, so that stepping in does not ask again.
    while (depth < want) history.pushState(gaveUp ? { depth: ++depth, kept: { gaveUp } } : { depth: ++depth }, '');
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

// The key that asks for something, where one does. Nothing shows a key on the page; a word in
// the foot says it when the pointer rests on it.
function keyFor(action) {
  const name = named(action);
  // Inside the body box Ctrl-D is the browser's, so the word does not claim it there.
  if (name === 'delete' && view?.field?.whole) return '';
  // With vim's keys Escape is vim's, and the body closes as vim's quits do.
  if (name === 'close' && view?.field?.whole && editor?.vimOn) return ':q';
  if (name === 'shift') return action.shift < 0 ? 'Ctrl ↑' : 'Ctrl ↓';
  return (
    { write: 'Enter', open: 'Ctrl O', close: 'Esc', go_in: 'Tab', back: 'Esc', select: 'Esc', delete: 'Ctrl D' }[name] ??
    ''
  );
}


const narrow = window.matchMedia('(max-width: 640px)');
narrow.addEventListener('change', () => view && paintSoon());
// A touch screen: a coarse pointer and nothing to hover with. A field with the focus there is a
// keyboard up.
const coarse = window.matchMedia('(hover: none) and (pointer: coarse)');
const typing = () => [line, box, $('secret')].includes(document.activeElement) || !!editor?.hasFocus;
// The keyboard there is the person's: the page moves the focus while a field has it, and
// otherwise only for a press that asked to write, which sets this until the painting that
// follows has given the field the focus: true, or where in the head a press on its words landed,
// which may be 0. A key pressed asks to write too: on a touch screen it is
// a keyboard joined to the device, and raises nothing. A phone shows no keyboard for a focus
// nobody asked for, leaving the page typing into nothing, or raises one under the hands; and a
// focus given inside the press's own event is the only kind for which iOS raises one.
let wantsText = false;
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
    placeCaret();
    // The head being written stays above the keyboard as it comes up.
    if (document.activeElement === box && !view?.field?.whole) box.scrollIntoView({ block: 'nearest' });
  });
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

const headOf = (text) => {
  const at = text.indexOf('\n');
  return at < 0 ? text : text.slice(0, at);
};

// What the box should hold for the field as it now is.
const boxText = (field) => (field.whole ? field.text : headOf(field.text));

// Its size follows its text: by the browser where it can, and otherwise from the number of lines,
// each counted 72 letters wide, since measuring would cost the keystroke.
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
  // The body is the box's own, but for Escape, which closes it, and Ctrl-O, which is swallowed so
  // that the browser's file dialog does not open in the middle of writing.
  // Tab is a tab character in it, as it is in the editor that takes its place.
  if (view.field.whole) {
    if (ctrlOnly(event) && (event.key === 'o' || event.key === 'O')) return event.preventDefault();
    if (event.key === 'Tab' && !event.ctrlKey && !event.metaKey && !event.altKey) {
      // Inserted as typing is, so that the browser's undo takes it back as it would a letter.
      if (!event.shiftKey) document.execCommand('insertText', false, '\t');
      return event.preventDefault();
    }
    if (event.key !== 'Escape') return;
    act('close');
    event.preventDefault();
    return;
  }
  switch (event.key) {
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
    case 'Enter':
      if (event.ctrlKey || event.metaKey || event.altKey) return;
      act('write');
      break;
    // Backspace on an empty head takes the row away and steps up, as it would an empty line. A new
    // row with only the labels it opened with is empty while the caret is where it landed, after a
    // task's box and before the rest.
    case 'Backspace': {
      const at = view.field.fresh ?? (box.value === '' ? 0 : -1);
      if (!plain || box.selectionStart !== at || box.selectionEnd !== at) return;
      act('erase');
      break;
    }
    case 'Escape':
      act({ select: null });
      break;
    case 'd':
    case 'D':
      if (!ctrlOnly(event)) return;
      act('delete');
      break;
    case 'o':
    case 'O':
      if (!ctrlOnly(event)) return;
      act('open');
      break;
    default:
      return;
  }
  event.preventDefault();
  follow();
});

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
      // The sheet is the editor's but for Ctrl-O, which never reaches the browser's file dialog,
      // from the text or from vim's prompt, and Escape, which closes it without vim's keys. With
      // them Escape is vim's alone, since hands that press it twice to be sure of normal mode
      // would close the note under themselves; `:q`, `ZZ` and the word in the foot close it.
      buffer.addEventListener('keydown', (event) => {
        if (event.isComposing || editor.composing || !view?.field?.whole) return;
        if (ctrlOnly(event) && (event.key === 'o' || event.key === 'O')) return event.preventDefault();
        if (event.key !== 'Escape' || event.defaultPrevented || editor.vimOn) return;
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
  const updated = view.field?.updated;
  if (!updated) return time?.remove();
  const text = day(updated);
  if (!time) {
    time = el('time', null, text);
    main.insertBefore(time, main.querySelector(':scope > .chev'));
  } else if (time.textContent !== text) time.textContent = text;
}

function fill(li, row, selected) {
  const field = selected ? view.field : null;
  const main = el('div', 'main');
  if (view.scores_shown && row.score != null) main.append(el('span', 'score', row.score.toFixed(2)));
  // Off the cursor a row reads as the engine shows it, without the place's labels at its end.
  const head = field ? headOf(field.text) : row.shown ?? row.head;
  main.append(field && !field.whole ? box : readHead(head, row.task));
  const tail = el('span', 'tail');
  if (row.archived) tail.append(el('span', null, 'archived'));
  if (row.has_body) tail.append(el('span', null, '¶'));
  if (row.links.length) tail.append(el('span', null, `→ ${row.links.join(', ')}`));
  if (row.reasons.length) tail.append(el('span', null, row.reasons.join(' · ')));
  main.append(tail);
  if (selected) {
    const chev = el('button', 'chev', '›');
    chev.type = 'button';
    chev.tabIndex = -1;
    chev.setAttribute('aria-label', 'Go in');
    main.append(chev);
  }
  li.replaceChildren(main);
}

// The rows made are those that can be seen and a screenful more, and more as the list is scrolled
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
  if (landed && boxIn) {
    const left = rows.get(boxIn);
    if (left) left.dataset.k = '';
    box.remove();
    editor?.dom.remove();
  }
  boxIn = fieldId;

  const asking = JSON.stringify([view.line, view.path]);
  if (asking !== listFor) {
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
    if (selected) dateRow(li);
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
  // person's caret is left alone while they type. On landing, and when the body closes, the caret
  // goes to the end of the head; when the body opens it stays where it was, at the end of the head,
  // so that Down goes to the second line; otherwise it is kept where it was, as far as the text
  // still reaches. A new row with only the labels it opened with has its caret where the engine
  // says instead, after a task's box and before the rest, wherever it would go to the end, and when
  // it is put back to them.
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
    if (toggled) editor.open(field.text, field.fresh ?? headOf(field.text).length);
    else if (!hadEditor) editor.open(field.text, Math.min(from, field.text.length));
    else if (kept(editor.text()) !== kept(field.text)) editor.setText(field.text);
    if (takesFocus(editor.contentDOM)) editor.focus();
  } else if (field && boxIn) {
    box.classList.toggle('whole', field.whole);
    const want = boxText(field);
    const toggled = field.whole !== wasWhole;
    const told = box.value !== want;
    if (told) {
      box.value = want;
      fit();
    }
    const end = box.value.length;
    // A press on a head's words puts the caret where it landed.
    const at = field.fresh ?? (landed && typeof wantsText === 'number' ? Math.min(wantsText, end) : end);
    if (landed || (toggled && !field.whole) || (told && field.fresh != null)) box.setSelectionRange(at, at);
    else box.setSelectionRange(Math.min(from, end), Math.min(to, end));
    if (takesFocus(box)) box.focus({ preventScroll: true });
  } else if (takesFocus(line)) {
    line.focus({ preventScroll: true });
  }
  wantsText = false;
  quiet = false;

  // The list follows the cursor when the cursor moves, and not otherwise: a note arriving from
  // elsewhere, or a head being written, must not pull the list from where it was scrolled to.
  const under = view.cursor != null ? view.rows[view.cursor]?.id ?? '' : '';
  if (under !== list.dataset.under) {
    if (under) rows.get(under)?.scrollIntoView({ block: 'nearest', inline: 'nearest' });
    else list.scrollTop = 0;
  }
  list.dataset.under = under;
}

// The words in the foot: what can be asked for where the cursor is. Moving has the arrows and a
// tap on a row, and the editor is the row itself, so neither gets a word. On the line, clearing it
// is the cross and going in is the arrow after it, so neither does either.
function paintHints() {
  const unsaid = view.cursor == null ? ['move', 'editor', 'type', 'go_in'] : ['move', 'editor'];
  const hints = view.hints.filter((hint) => !unsaid.includes(named(hint.action)));
  // Vim's keys are this browser's to ask for, and only where they apply: in an open body, once
  // the editor is there to have them.
  const vimWord = view.field?.whole && editor ? (vimOn ? 'no vim' : 'vim') : null;
  // On a phone the way back, which the engine offers last, is pinned at the right of the bar,
  // where a thumb finds it without reading; the rest scroll.
  const last = hints.length ? named(hints[hints.length - 1].action) : '';
  const way = narrow.matches && ['back', 'select', 'close'].includes(last) ? hints.pop() : null;
  set($('way'), JSON.stringify(way), () => (way ? [word(way)] : []));
  const words = $('hints');
  const changed = set(words, JSON.stringify([hints, vimWord]), () => {
    const words = hints.map(word);
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
  // New words, and the words of another row, start from the left, and a fade at the right says
  // when there are more than fit.
  const under = view.cursor != null ? (view.rows[view.cursor]?.id ?? '') : '';
  const moved = words.dataset.under !== under;
  words.dataset.under = under;
  if ((changed || moved) && narrow.matches) {
    words.scrollLeft = 0;
    words.classList.toggle('more', words.scrollWidth > words.clientWidth + 1);
  } else if (changed) words.classList.remove('more');
}

function word(hint) {
  const button = el('button', 'word', hint.label);
  button.type = 'button';
  const key = keyFor(hint.action);
  if (key) button.title = key;
  button.onclick = () => {
    // A new note is asked for to be written in, so the keyboard comes for it.
    if (named(hint.action) === 'write') wantsText = true;
    act(hint.action);
    follow();
    wantsText = false;
  };
  return button;
}

let status = '';
let fading = 0;
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
    node.classList.remove('faded');
    node.classList.toggle('trouble', !!refused && status === refused);
    clearTimeout(fading);
    if (status && status !== refused) fading = setTimeout(() => node.classList.add('faded'), 5000);
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
// from its top. With the body open the sheet has it, and the pane says nothing.
function paintPane() {
  const field = view.field;
  const says =
    !wide.matches || field?.whole ? '' : field ? bodyOf(field.text) : view.here ? bodyOf(view.here.text) : '';
  paintBody(pane, says);
  const under = (view.cursor != null ? view.rows[view.cursor]?.id : view.here?.id) ?? '';
  if (pane.dataset.under !== under) {
    pane.dataset.under = under;
    pane.scrollTop = 0;
  }
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
    [field, field.selectionStart, field.selectionEnd, field.value, field.parentElement, field.className,
      $('dates').textContent, view?.cursor, composing, field === document.activeElement,
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
  const face = `${field.id} ${field.className} ${innerWidth}`;
  if (mirror.dataset.face !== face) {
    mirror.dataset.face = face;
    for (const name of COPIED) mirror.style[name] = style[name];
    mirror.style.whiteSpace = field === box ? 'pre-wrap' : 'pre';
    mirror.style.overflowWrap = field === box ? 'anywhere' : 'normal';
  }
  return style;
}

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
    (field === box || view.cursor == null);
  measured = asMeasured();
  if (field && inEditor()) return placeEditorCaret();
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
  // than hanging past the end of a full line as a space does.
  const end = at >= text.length || text[at] === '\n';
  const span = el('span', null, end ? '\u00a0' : text[at]);
  mirror.replaceChildren(text.slice(0, at), span, end ? text.slice(at) : text.slice(at + 1));
  const width = `${field.clientWidth - padding}px`;
  const [fieldLeft, fieldTop] = [field.offsetLeft - field.scrollLeft, field.offsetTop - field.scrollTop];
  if (mirror.style.width !== width) mirror.style.width = width;

  // The read. The block is the height of the line, centred on the letter as the line's own leading
  // is, and never narrower than a third of a letter, so that it is still a block over an i.
  const { offsetLeft, offsetTop, offsetWidth, offsetHeight } = span;
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
  // Ctrl-D clears the line, and on a row deletes the note; the browser's bookmark is not asked
  // for either way.
  if (ctrlOnly(event) && (event.key === 'd' || event.key === 'D')) {
    act(onLine ? { type: '' } : 'delete');
    event.preventDefault();
    follow();
    return;
  }
  // Ctrl-O on a row opens its body, and on the line does nothing; the browser's file dialog is not
  // asked for either way.
  if (ctrlOnly(event) && (event.key === 'o' || event.key === 'O')) {
    if (!onLine) act('open');
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
  if (event.ctrlKey || event.metaKey || event.altKey) return;
  switch (event.key) {
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
    case 'Enter':
      act('write');
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

// Whether a point is on the words of a head, a few pixels round each line of them counting, since
// a finger is not a point: false when it is not, and otherwise where in the head's text, or true,
// for the end, when that cannot be counted, as in a head read with its marks.
const SLOP = 6;
const near = (r, x, y) => x >= r.left - SLOP && x <= r.right + SLOP && y >= r.top - SLOP && y <= r.bottom + SLOP;
function hitText(head, x, y) {
  if (!head) return false;
  const words = document.createRange();
  words.selectNodeContents(head);
  if (![...words.getClientRects()].some((r) => near(r, x, y))) return false;
  const parts = [...head.childNodes].filter((node) => !node.classList?.contains('tick'));
  if (!parts.every((node) => node.nodeType === Node.TEXT_NODE)) return true;
  const at = letterAt(x, y);
  let before = Number(head.dataset.boxed ?? 0);
  for (const node of parts) {
    if (node === at?.offsetNode) return before + at.offset;
    before += node.length;
  }
  return true;
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

// A tap beside a head's words puts the cursor on the note, and a second there, the head being the
// box's by then, opens its body; a tap on the words puts the cursor there too and writes in it,
// with the caret where it landed, which on a phone is what raises the keyboard. The arrow on the
// selected row goes in, and a link in a row opens and does nothing else.
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

list.addEventListener('click', (event) => {
  const item = event.target.closest('.row');
  if (!item || event.target === box || event.target.closest('a')) return;
  const at = view.rows.findIndex((row) => row.id === item.dataset.id);
  if (at < 0) return;
  const fresh = touched && Date.now() - touched.at < 1000;
  const [x, y] = fresh ? [touched.x, touched.y] : [event.clientX, event.clientY];
  const ticker = event.target.closest('.tick');
  const reach = (node) => node.getBoundingClientRect().right + 0.4 * parseFloat(getComputedStyle(node).fontSize);
  if (ticker && (!fresh || x <= reach(ticker))) return tick(at);
  if (event.target.closest('.chev')) act('go_in');
  else if (at !== view.cursor) {
    wantsText = hitText(item.querySelector('.head'), x, y);
    act({ select: at });
  } else if (!view.field?.whole) act('open');
  follow();
  wantsText = false;
});
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
  if (!view?.field || view.field.whole || !boxIn || document.activeElement === box) return false;
  const row = rows.get(boxIn);
  const within = (r) => x >= r.left && x <= r.right && y >= r.top && y <= r.bottom;
  if (!row || !within(row.getBoundingClientRect())) return false;
  const chev = row.querySelector('.chev');
  if (chev && within(chev.getBoundingClientRect())) return false;
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
  if (Date.now() - lastAsked < 30000 || Date.now() - (remembered().at ?? 0) < 60000) return;
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
      list.scrollTop = scroll;
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
