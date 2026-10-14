// The browser face: one line, one list, one note.
//
// The engine is the app. It runs here as WebAssembly and is told what was asked for, as an
// action, and says what to show. This file is what a page adds to it: which key or tap asks
// for what, painting the view, the socket to the relay this page came from, and somewhere to
// keep lines between visits.
//
// Typing always goes to the line. Down moves onto the list, Up and Escape come back, Enter
// writes the line or goes into the note under the cursor, and Escape steps back out of a note.
// Everything else that can be asked for is a button at the foot of the page, reached with Tab
// or a tap, and only what applies where the cursor is gets one.

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

// What this browser keeps for a space, besides its lines: the writer id, and the key.
const WRITER = `fzn.writer.${space}`;
const KEY = `fzn.key.${space}`;

function kept(name) {
  try {
    return localStorage.getItem(name);
  } catch {
    return null;
  }
}

function keep(name, value) {
  try {
    if (value == null) localStorage.removeItem(name);
    else localStorage.setItem(name, value);
  } catch {
    // Nowhere to keep it, so it is asked for or made again next time.
  }
}

// This browser's writer id for the space: its name in the versions it writes, and so the name
// of its log. Made once and kept, since two logs under one name are a fork. A browser from
// before writer ids were kept a space at a time had one for every space, and each space takes
// that one over.
function writer() {
  const fresh = () =>
    [...crypto.getRandomValues(new Uint8Array(4))].map((b) => b.toString(16).padStart(2, '0')).join('');
  const plain = (id) => /^[0-9a-f]{8}$/.test(id || '');
  let id = kept(WRITER);
  if (!plain(id)) {
    const old = kept('fzn.device');
    id = plain(old) ? old : fresh();
    keep(WRITER, id);
  }
  return id;
}

// The space's key, from what was kept or from the passphrase. The form stays until there is
// one. Argon2 holds the page for a moment, so the form says what it is doing first.
function key(why = '') {
  const found = kept(KEY);
  if (found) return Promise.resolve(found);
  const form = $('unlock');
  const secret = $('secret');
  const unlocking = $('unlocking');
  $('space').textContent = space;
  unlocking.textContent = why;
  form.hidden = false;
  document.body.classList.add('locked');
  secret.focus();
  return new Promise((resolve) => {
    form.onsubmit = (event) => {
      event.preventDefault();
      if (!secret.value) return;
      form.classList.add('busy');
      unlocking.textContent = 'deriving the key…';
      // Two frames, so that the words are on the screen before Argon2 has the page.
      requestAnimationFrame(() =>
        requestAnimationFrame(() => {
          const derived = deriveKey(secret.value, space);
          secret.value = '';
          keep(KEY, derived);
          form.hidden = true;
          form.classList.remove('busy');
          document.body.classList.remove('locked');
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
  return {
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
    keep: ({ log, from, lines }) => {
      const store = db.transaction('lines', 'readwrite').objectStore('lines');
      lines.forEach((line, i) => store.put({ log, n: from + i, line }));
    },
    // Every line of a log. Transactions on one store run in the order they were made, so this
    // comes after whatever was kept before it was asked for.
    forget: (log) => {
      const store = db.transaction('lines', 'readwrite').objectStore('lines');
      store.delete(IDBKeyRange.bound([log, 0], [log, Infinity]));
    },
  };
}

let fzn;
let store = null;
let view = null;
let socket = null;
let wait = 1000;
let synced = 'connecting';
let editing = null;
let standing = true;

const line = $('line');

function act(action) {
  carryOut(fzn.act(JSON.stringify(action)));
}

// Does what the engine wants done, in its order: lines to keep, then logs to delete, which a
// compaction has retired, then a new writer id to keep, then messages for the relay, and the
// three things it leaves to whoever is showing the notes. Keeping comes before deleting so that
// a page closed between the two still has the notes somewhere.
function carryOut(json) {
  const todo = JSON.parse(json);
  for (const lines of todo.keep) {
    try {
      store?.keep(lines);
    } catch {
      fzn.say('could not keep that in this browser; it will be asked of the relay again');
    }
  }
  for (const log of todo.delete) {
    try {
      store?.forget(log);
    } catch {
      // Left where it is, it is deleted the next time the page opens.
    }
  }
  if (todo.device) keep(WRITER, todo.device);
  for (const text of todo.send) if (socket?.readyState === WebSocket.OPEN) socket.send(text);
  if (todo.edit) edit(todo.edit);
  if (todo.copy != null) copy(todo.copy);
  if (todo.open) window.open(todo.open, '_blank', 'noopener');
  look();
}

// Reads what there is to show. This is done at once, so that the next key is judged by where
// the cursor is now and the line holds what the engine made of it. The painting can wait for
// the next frame.
function look() {
  view = JSON.parse(fzn.view());
  // The line is the person's while they type. It is only set when the engine has changed it,
  // as it does on the way into a note and out of one, and when the line is written.
  if (line.value !== view.line) line.value = view.line;
  paintSoon();
}

function connect() {
  if (!standing || socket) return;
  const scheme = location.protocol === 'https:' ? 'wss' : 'ws';
  const opened = new WebSocket(`${scheme}://${location.host}/1/${space}`);
  socket = opened;
  opened.onopen = () => {
    wait = 1000;
    synced = 'syncing';
    carryOut(fzn.connected());
  };
  opened.onmessage = (event) => carryOut(fzn.received(event.data));
  opened.onclose = () => {
    if (socket !== opened) return;
    socket = null;
    fzn.disconnected();
    synced = 'not syncing';
    look();
    if (standing) setTimeout(connect, wait);
    wait = Math.min(wait * 2, 30000);
  };
}

async function copy(text) {
  try {
    await navigator.clipboard.writeText(text);
  } catch {
    // A page not served securely has no clipboard to ask, so the old way.
    const scratch = el('textarea');
    scratch.value = text;
    scratch.className = 'scratch';
    document.body.append(scratch);
    scratch.select();
    document.execCommand('copy');
    scratch.remove();
    line.focus();
  }
}

// A note being edited takes the place of the note being read. Ctrl-Enter saves and Escape
// leaves it as it was.
function edit({ id, text }) {
  editing = { id };
  const area = el('textarea', 'editor');
  area.value = text;
  area.setAttribute('aria-label', 'The note');
  area.spellcheck = false;
  const save = el('button', null, 'Save');
  const cancel = el('button', null, 'Cancel');
  const done = (saved) => {
    editing = null;
    document.body.classList.remove('editing-now');
    if (saved) act({ save: { id, text: area.value } });
    look();
    line.focus();
  };
  save.type = cancel.type = 'button';
  save.append(el('kbd', null, 'Ctrl Enter'));
  cancel.append(el('kbd', null, 'Esc'));
  save.onclick = () => done(true);
  cancel.onclick = () => done(false);
  area.onkeydown = (event) => {
    if (event.key === 'Enter' && (event.ctrlKey || event.metaKey)) {
      event.preventDefault();
      done(true);
    } else if (event.key === 'Escape') {
      event.preventDefault();
      done(false);
    }
  };
  const buttons = el('div', 'editing');
  buttons.append(save, cancel);
  $('note').replaceChildren(area, buttons);
  // Whatever was being read here is gone, so the next painting puts it back.
  delete $('note').dataset.shows;
  document.body.classList.add('editing-now');
  area.focus();
  area.setSelectionRange(text.length, text.length);
}

let waiting = false;
function paintSoon() {
  if (waiting) return;
  waiting = true;
  requestAnimationFrame(() => {
    waiting = false;
    paint();
  });
}

const day = (moment) => (moment ? moment.slice(0, 10) : '');
const WEEKDAYS = ['Thu', 'Fri', 'Sat', 'Sun', 'Mon', 'Tue', 'Wed'];
// A day is counted from the first of January 1970, which was a Thursday.
const dated = (n) => `${WEEKDAYS[((n % 7) + 7) % 7]} ${new Date(n * 86400000).toISOString().slice(0, 10)}`;

// The key that asks for something, where a key does. The rest are asked for with their button.
function keyFor(action) {
  if (action === 'write' || action === 'go_in') return 'Enter';
  if (action === 'back' || (action.select !== undefined && action.select === null)) return 'Esc';
  if (action.move !== undefined) return '↓';
  return null;
}

const narrow = window.matchMedia('(max-width: 820px)');
narrow.addEventListener('change', () => view && paintSoon());

// The note under the cursor, for reading: when it was written, and then what it says.
function reading(note, row, withHead) {
  const about = [`created ${day(note.created) || '?'}`, `updated ${day(note.updated) || '?'}`];
  if (note.archived) about.push('archived');
  if (row?.links.length) about.push(`links ${row.links.join(', ')}`);
  const [head, ...body] = note.text.split('\n');
  const parts = [el('div', 'meta', about.join(' · '))];
  if (withHead) parts.push(el('h1', null, head.trim()));
  const said = body.join('\n').replace(/^\n+/, '');
  if (said) parts.push(el('div', 'body', said));
  return parts;
}

// Sets what a part of the page holds, but only when that has changed. Buttons that are put back
// the same as they were would lose the focus for nothing.
function set(node, key, make) {
  if (node.dataset.shows === key) return;
  node.dataset.shows = key;
  node.replaceChildren(...make());
}

function paint() {
  if (!view) return;
  const searching = view.line.trim() !== '';
  const zoomed = view.path.length > 0;

  // Where you are. Each step back along the path is a way back to it.
  set($('path'), JSON.stringify(view.path), () => {
    const path = [el('button', null, title)];
    view.path.forEach((head, depth) => {
      path.push(el('span', 'sep', '›'), el('button', null, head));
      path[path.length - 1].dataset.depth = depth + 1;
    });
    path[0].dataset.depth = 0;
    path[path.length - 1].setAttribute('aria-current', 'location');
    return path;
  });

  line.placeholder = zoomed ? `write about ${view.path[view.path.length - 1]}, or narrow` : '';
  $('clear').hidden = view.line === '';
  $('dates').textContent = view.dates.length ? `→ ${view.dates.map(dated).join(', ')}` : '';

  const linked = view.rows.filter((row) => row.linked).length;
  const shown = view.archived_shown ? ' · archived shown' : '';
  $('title').textContent = zoomed
    ? searching
      ? `${view.rows.length} matching${shown}`
      : `${linked} linked · ${view.rows.length - linked} suggested${shown}`
    : `${searching ? 'related' : 'recent'} · ${view.rows.length}${shown}`;

  const rows = view.rows.map((row, i) => {
    const item = el('li', 'row');
    item.setAttribute('role', 'option');
    item.setAttribute('aria-selected', i === view.cursor);
    item.dataset.row = i;
    if (row.archived) item.classList.add('archived');
    // What the engine only suggests is set a little apart from what is linked.
    if (zoomed && !searching && !row.linked && i > 0 && view.rows[i - 1].linked) {
      item.classList.add('suggested');
    }
    const main = el('div', 'main');
    if (view.scores_shown && row.score != null) main.append(el('span', 'score', row.score.toFixed(2)));
    main.append(el('span', 'mark', row.has_body ? '▸' : ''), el('span', 'head', row.head));
    if (row.links.length) main.append(el('span', 'links', `→ ${row.links.join(', ')}`));
    main.append(el('time', null, day(row.updated)));
    item.append(main);
    if (row.reasons.length) item.append(el('div', 'why', row.reasons.join(' · ')));
    return item;
  });
  // On a narrow screen there is no room beside the list, so the note under the cursor opens
  // where it is, under its own row.
  const note = view.preview;
  if (narrow.matches && view.cursor != null && note && rows[view.cursor]) {
    const open = el('div', 'reading');
    open.append(...reading(note, view.rows[view.cursor], false));
    rows[view.cursor].append(open);
  }
  $('list').replaceChildren(...rows);
  // The list follows the cursor when the cursor moves, and not otherwise: a note arriving from
  // elsewhere must not pull the list back from where it was scrolled to.
  const under = view.cursor != null ? view.rows[view.cursor].id : '';
  if (under && under !== $('list').dataset.under) {
    rows[view.cursor].scrollIntoView({ block: 'nearest', inline: 'nearest' });
  }
  $('list').dataset.under = under;

  // The note being read is only put on the page again when it has changed, so that reading
  // far down a long one is not sent back to its top by something arriving from elsewhere.
  if (!editing) {
    const row = view.cursor != null ? view.rows[view.cursor] : view.rows[0];
    const shows = note ? JSON.stringify([note.id, note.stamp, note.archived, row?.links]) : '';
    const another = note?.id !== $('note').dataset.note;
    set($('note'), shows, () => (note ? reading(note, row, true) : []));
    if (another) $('note').scrollTop = 0;
    $('note').dataset.note = note?.id ?? '';
    $('note').classList.toggle('glance', view.cursor == null);
  }

  // What can be asked for where the cursor is, each a button, with its key where it has one.
  set($('hints'), JSON.stringify(view.hints), () =>
    view.hints.map((hint) => {
      const button = el('button');
      button.type = 'button';
      const key = keyFor(hint.action);
      if (key) button.append(el('kbd', null, key));
      button.append(hint.label);
      button.onclick = () => {
        act(hint.action);
        if (!editing) line.focus();
      };
      return button;
    }),
  );

  // A key that opens nothing here writes nothing, and the way out is to type the passphrase
  // again, so that is said for as long as it is so.
  const shut = !fzn.key_verified();
  $('state').classList.toggle('shut', shut);
  $('status').textContent = view.status || (shut ? 'the passphrase does not open this space' : '');
  $('count').textContent = view.notes === 1 ? '1 note' : `${view.notes} notes`;
  $('sync').textContent = synced;
  $('sync').dataset.state = synced;
}

line.addEventListener('input', () => act({ type: line.value }));

// The keys, wherever the focus has got to. Typing always goes to the line, so a key that is a
// letter takes the focus there before it lands. A button that has the focus keeps Enter and
// Space for itself.
document.addEventListener('keydown', (event) => {
  if (event.isComposing || !view || editing || !standing) return;
  const onButton = event.target instanceof HTMLButtonElement;
  if (onButton && (event.key === 'Enter' || event.key === ' ')) return;
  if (event.key === 'Tab') return;
  const typed = event.key.length === 1 && !event.ctrlKey && !event.metaKey && !event.altKey;
  if (typed) {
    if (event.target !== line) line.focus();
    return;
  }
  const onLine = view.cursor == null;
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
      act(onLine ? 'write' : 'go_in');
      break;
    case 'Escape':
      if (!onLine) act({ select: null });
      else if (view.path.length) act('back');
      else return;
      break;
    default:
      return;
  }
  event.preventDefault();
  if (event.target !== line) line.focus();
});

$('clear').onclick = () => {
  act({ type: '' });
  line.focus();
};

// A click or a tap puts the cursor on a note, and a second one goes into it.
$('list').addEventListener('click', (event) => {
  const item = event.target.closest('.row');
  if (!item) return;
  const row = Number(item.dataset.row);
  act(row === view.cursor ? 'go_in' : { select: row });
  line.focus();
});

// Each step of the path goes back to where it was.
$('path').addEventListener('click', (event) => {
  const depth = event.target.dataset?.depth;
  if (depth === undefined) return;
  for (let steps = view.path.length - Number(depth); steps > 0; steps--) act('back');
  line.focus();
});

// Only one tab at a time is this device. A tab that opens says so, and the one that was open
// stands down: two of them writing one log would fork it.
const tabs = 'BroadcastChannel' in window ? new BroadcastChannel(`fzn:${space}`) : null;
function standDown() {
  standing = false;
  socket?.close();
  $('elsewhere').hidden = false;
  document.body.classList.add('stood-down');
}
$('here').onclick = () => location.reload();

// For a passphrase mistyped: the key is forgotten and the page asks again.
$('passphrase').onclick = () => {
  keep(KEY, null);
  location.reload();
};

async function start() {
  await init();
  const id = writer();
  let why = '';
  for (;;) {
    try {
      fzn = new Fzn(id, await key(why));
      break;
    } catch (cause) {
      keep(KEY, null);
      why = `${cause}; type the passphrase again`;
    }
  }
  $('passphrase').hidden = false;
  if (tabs) {
    tabs.onmessage = (event) => event.data === 'here' && standDown();
    tabs.postMessage('here');
  }
  store = await openStore().catch(() => null);
  if (store) for (const [log, lines] of await store.load()) carryOut(fzn.load(log, lines));
  carryOut(fzn.opened());
  look();
  line.focus();
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
  $('status').textContent = `fzn could not start: ${why}`;
});
