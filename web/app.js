// The browser face: a blank page with one cursor on it.
//
// The engine is the app. It runs here as WebAssembly and is told what was asked for, as an
// action, and says what to show. This file is what a page adds to it: which key or tap asks
// for what, painting the view, the socket to the relay this page came from, and somewhere to
// keep lines between visits.
//
// There is one cursor on the page and it moves. On the line it is the caret, and typing finds.
// Down puts it on a row, whose head becomes a text box with the caret at its end, so typing there
// changes the head and nothing has to be pressed to begin or to keep it: the engine writes each
// change as it is typed. Enter with text on the line writes it and puts the box in the note it
// wrote. Enter on a row opens a new note under it, its empty head the box, so that Enter, type,
// Enter, type writes notes in a run, as the original's Enter did. Ctrl-O opens the body, the
// original's chord for it, in the sheet: a bounded overlay over the dimmed page, scrolled on its
// own, where the box holds the whole note, or an editor takes its place once one has been fetched
// (see loadEditor). The arrows stay inside it, and Escape closes it, or with vim's keys `:q`;
// leaving it for anywhere else on the page closes it too. Touch has no chord, so a second tap on
// the row opens it, off its head: the head is the box by then, and a tap there is the caret's. A
// body is never shown under its row. On a wide page the body of what the cursor is on is read
// beside the list: the note under it, or from the line the note you are in. On a narrower one a
// note's body is read by opening it, and the note you are in is read above the line. The arrow to
// the right at the end of a head goes into the note, and at the end of the line into what it names;
// Escape comes back a rung at a time, off the row, off the text on the line, out of the place.
// Control with an arrow moves the note on the row up or down the list. Everything else is a word
// in the foot, moving a note included, and only what applies right now gets one.
//
// The painting is kept cheap because it runs on every keystroke: rows are keyed by id and their
// nodes reused, nothing measures the page but the caret, once at the end, and only a row that is
// new fades in.

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

// The space's key, from what was kept or from the passphrase. The field is where the line will
// be, so that the page does not change shape when it opens. Argon2 holds the page for a moment,
// so the page says what it is doing first.
function key(why = '') {
  const found = kept(KEY);
  if (found) return Promise.resolve(found);
  const form = $('unlock');
  const secret = $('secret');
  const told = $('unlocking');
  const asking = $('for');
  $('space').textContent = space;
  const ask = asking.innerHTML;
  if (why) {
    told.textContent = why;
    told.classList.add('trouble');
  }
  form.hidden = false;
  document.body.classList.add('locked');
  secret.disabled = false;
  secret.focus();
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
          asking.innerHTML = ask;
          form.hidden = true;
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
let standing = true;

const line = $('line');
const list = $('list');
const pane = $('pane');
// The sheet, and in it the scroll that the body is written in and, at its foot, where vim's
// prompt for a command goes, under the text and never over it.
const buffer = $('buffer');
const sheet = $('sheet');

function act(action) {
  if (!fzn || !standing) return;
  carryOut(fzn.act(JSON.stringify(action)));
}

// Does what the engine wants done, in its order: lines to keep, then logs to delete, which a
// compaction has retired, then a new writer id to keep, then messages for the relay, and the
// things it leaves to whoever is showing the notes. Keeping comes before deleting so that a page
// closed between the two still has the notes somewhere.
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
  if (todo.copy != null) copy(todo.copy);
  if (todo.open) window.open(todo.open, '_blank', 'noopener');
  look();
  // The page has no editor but the sheet, so a note handed to an editor has its body opened
  // there, if it is on the list.
  if (todo.editor) {
    const at = view.rows.findIndex((row) => row.id === todo.editor.id);
    if (at >= 0 && !view.field?.whole) queueMicrotask(() => (act({ select: at }), act('edit')));
  }
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
  if (!editor) {
    if (view.field?.whole) loadEditor();
    else if (view.cursor != null && !prefetched) {
      prefetched = true;
      soon(loadEditor);
    }
  }
  if (place(view) !== was) safely();
  else paintSoon();
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

// A painting that fails says so in the foot, where it can be seen, rather than leaving the page
// as it was with no word of why.
function safely() {
  try {
    paint();
  } catch (cause) {
    const status = $('status');
    status.textContent = `fzn could not show that: ${cause}`;
    status.classList.add('trouble');
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

// Whether a body runs past six lines and so is clamped. Counted from the text, since measuring
// the page in the middle of a paint would cost the keystroke.
const long = (body) => body.split('\n').reduce((n, part) => n + 1 + Math.floor(part.length / 72), 0) > 6;

// The key that asks for something, where one does. Nothing shows a key on the page; a word in
// the foot says it when the pointer rests on it.
function keyFor(action) {
  const name = typeof action === 'string' ? action : Object.keys(action)[0];
  // Inside the body box Ctrl-D is the browser's, so the word does not claim it there.
  if (name === 'delete' && view?.field?.whole) return '';
  // With vim's keys Escape is vim's, and the body closes as vim's quits do.
  if (name === 'done' && view?.field?.whole && editor?.vimOn) return ':q';
  if (name === 'shift') return action.shift < 0 ? 'Ctrl ↑' : 'Ctrl ↓';
  return (
    { write: 'Enter', edit: 'Ctrl O', done: 'Esc', go_in: '→', back: 'Esc', select: 'Esc', delete: 'Ctrl D' }[name] ??
    ''
  );
}

const named = (action) => (typeof action === 'string' ? action : Object.keys(action)[0]);

const narrow = window.matchMedia('(max-width: 640px)');
narrow.addEventListener('change', () => view && paintSoon());
const still = window.matchMedia('(prefers-reduced-motion: reduce)');

// Sets what a part of the page holds, but only when that has changed. Buttons that are put back
// the same as they were would lose the focus for nothing.
function set(node, key, make) {
  if (node.dataset.shows === key) return;
  node.dataset.shows = key;
  node.replaceChildren(...make());
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
// counted as long() counts them, since measuring would cost the keystroke.
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
// a head does nothing: the field stays the cursor's, so that a word in the foot reached by Tab is
// about the note, and a click on the line or a row says where the cursor goes.
box.addEventListener('blur', (event) => leftFor(event));

function leftFor(event) {
  if (quiet || !view?.field?.whole) return;
  const to = event.relatedTarget;
  // A word in the foot or a step of the path reached by Tab is about the note as it is being
  // written, so the body stays open for it, and anywhere in the sheet is still in it.
  if (to?.closest?.('#hints, #path, #buffer')) return;
  if (!(to ? document.body.contains(to) : document.hasFocus())) return;
  leaving = true;
  try {
    act('done');
  } finally {
    leaving = false;
  }
}

box.addEventListener('keydown', (event) => {
  if (event.isComposing || !view?.field) return;
  const plain = !event.shiftKey && !event.ctrlKey && !event.metaKey && !event.altKey;
  // The body is the box's own, but for Escape, which closes it, and Ctrl-O, which is swallowed so
  // that the browser's file dialog does not open in the middle of writing.
  if (view.field.whole) {
    if (ctrlOnly(event) && (event.key === 'o' || event.key === 'O')) return event.preventDefault();
    if (event.key !== 'Escape') return;
    act('done');
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
    // The arrow goes in only from the end of the head, where the caret is put on landing; short
    // of it, it is the caret's.
    case 'ArrowRight':
      if (!plain || box.selectionStart !== box.selectionEnd || box.selectionEnd !== box.value.length) return;
      act('go_in');
      break;
    case 'Enter':
      if (event.ctrlKey || event.metaKey || event.altKey) return;
      act('write');
      break;
    // Backspace on an empty head takes the row away and steps up, as it would an empty line. A new
    // row with only the label it opened with is empty while the caret is before the label.
    case 'Backspace':
      if (!plain || box.selectionEnd !== 0 || (box.value !== '' && !view.field.fresh)) return;
      act('erase');
      break;
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
      act('edit');
      break;
    default:
      return;
  }
  event.preventDefault();
  follow();
});

// The focus goes where the cursor is: the box when it is on a row, the line when it is not.
function follow() {
  if (!view) return;
  if (view.cursor != null && boxIn && view.field?.whole && editor) {
    if (document.activeElement !== editor.contentDOM) editor.focus();
    return;
  }
  const target = view.cursor != null && boxIn ? box : line;
  if (document.activeElement !== target) target.focus({ preventScroll: true });
}

// The body's editor, which takes the box's place while a body is open. It is a module of its own,
// CodeMirror with vim's keys, fetched the first time a body opens, or sooner when the page is
// idle once the cursor is on a row, and never at first paint. Until it is here, and for good if
// it cannot be fetched, the box is the body as it always was.
let editor = null;
let editorSoon = null;
let prefetched = false;
let vimOn = kept('fzn.vim') === 'on';
// Set while `:w` closes the body and opens it again, so that the editor is left as it is.
let keeping = false;

const soon = (task) =>
  window.requestIdleCallback ? requestIdleCallback(() => task()) : setTimeout(task, 1500);

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
        act('done');
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
  // A body is written when it closes, so vim's write closes it and opens it again under the
  // hands, if the note is still there, scrolled as it was, which closing lost; its quits close
  // it, and the one with a bang first puts the note back as it was when the cursor landed on it.
  onEx: (what) => {
    if (what === 'write') {
      const was = boxIn;
      const scrolled = sheet.scrollTop;
      keeping = true;
      try {
        act('done');
        if (view.cursor != null && view.rows[view.cursor]?.id === was) act('edit');
      } finally {
        keeping = false;
      }
      sheet.scrollTop = scrolled;
    } else {
      // Put back is asked for only when the engine offers it, since on a body that has not
      // changed undo would take back the last version written instead, which may be another
      // note's.
      if (what === 'discard' && view.hints.some((hint) => named(hint.action) === 'undo')) act('undo');
      act('done');
    }
    follow();
  },
};

function rowKey(row, selected, apart) {
  const field = selected ? view.field : null;
  return JSON.stringify([
    // The head of the row under the cursor is the box's, so it is left out here: a row is not
    // filled again, taking the box out and putting it back, for a letter typed into it. With the
    // body open in the sheet the row reads its head as it is being written. Nor is its date in
    // the key, which a new note's first letter gives it: the date is set in place, since filling
    // the row would take the box out under a composition the first letter began.
    field && !field.whole ? null : field ? headOf(field.text) : row.head,
    row.links, row.reasons, row.archived, row.has_body, apart, selected,
    view.scores_shown ? row.score : null,
  ]);
}

// Sets the date at the end of the selected row's head in place, making it or taking it away.
function dateRow(li, row) {
  const main = li.querySelector(':scope > .main');
  if (!main) return;
  let time = main.querySelector(':scope > time');
  if (!row.updated) return time?.remove();
  const text = day(row.updated);
  if (!time) {
    time = el('time', null, text);
    main.insertBefore(time, main.querySelector(':scope > .chev'));
  } else if (time.textContent !== text) time.textContent = text;
}

function fill(li, row, selected) {
  const field = selected ? view.field : null;
  const main = el('div', 'main');
  if (view.scores_shown && row.score != null) main.append(el('span', 'score', row.score.toFixed(2)));
  main.append(field && !field.whole ? box : el('span', 'head', field ? headOf(field.text) : row.head));
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

function paintRows() {
  const zoomed = view.path.length > 0;
  const searching = view.line.trim() !== '';
  const field = view.field;
  const fieldId = view.cursor != null && field ? view.rows[view.cursor]?.id ?? null : null;
  const focused = document.activeElement === box || !!editor?.hasFocus;
  const [from, to] = [box.selectionStart, box.selectionEnd];
  // The box's class says whether the field was the whole note last time, whichever of the two
  // held it.
  const wasWhole = box.classList.contains('whole');
  const hadEditor = !!editor?.dom.isConnected;
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

  const seen = new Set(view.rows.map((row) => row.id));
  for (const [id, li] of rows) {
    if (!seen.has(id)) {
      li.remove();
      rows.delete(id);
    }
  }

  let newcomers = 0;
  const wanted = view.rows.map((row, i) => {
    let li = rows.get(row.id);
    if (!li) {
      li = el('li', 'row');
      li.setAttribute('role', 'option');
      li.dataset.id = row.id;
      li.dataset.k = '';
      rows.set(row.id, li);
      if (!still.matches && !field?.whole) {
        li.classList.add('in');
        li.style.setProperty('--i', Math.min(newcomers++, 8));
      }
    }
    const selected = i === view.cursor;
    if (li.getAttribute('aria-selected') !== String(selected)) li.setAttribute('aria-selected', selected);
    // In a place, what the line finds in it is set apart from what it only finds there.
    const apart = zoomed && searching && !row.linked && i > 0 && view.rows[i - 1].linked;
    li.classList.toggle('apart', apart);
    li.classList.toggle('archived', row.archived);
    // A new note with nothing in it has nowhere to go in to, so its row has no arrow.
    li.classList.toggle('blank', selected && !!field?.fresh);
    const k = rowKey(row, selected, apart);
    if (li.dataset.k !== k) {
      li.dataset.k = k;
      fill(li, row, selected);
    }
    if (selected) dateRow(li, row);
    return li;
  });

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
  // still reaches. A new row with only the label it opened with has its caret at the start instead,
  // before the label, wherever it would go to the end, and when it is put back to the label.
  if (field && boxIn && field.whole && editor) {
    box.classList.add('whole');
    // The editor is given a fresh state when the body opens, its caret where the box's would be,
    // and when it takes over from the box, with the box's caret. While `:w` closes and opens again
    // it is left as it is, so the caret and the step undo survive the write. Otherwise it is told
    // the text only when the engine's differs from it: put back, a date word expanded, a version
    // from elsewhere. A newline just made at the end is not a difference, as it is not one to the
    // engine, which keeps the note without it: told the kept text, the editor would take the new
    // line away under the hands.
    const toggled = !wasWhole;
    const kept = (text) => text.replace(/[\r\n]+$/, '');
    if (!keeping && toggled) editor.open(field.text, field.fresh ? 0 : headOf(field.text).length);
    else if (!keeping && !hadEditor) editor.open(field.text, Math.min(from, field.text.length));
    else if (kept(editor.text()) !== kept(field.text)) editor.setText(field.text);
    const lost = [document.body, line].includes(document.activeElement);
    if (!leaving && (landed || toggled || focused || lost)) editor.focus();
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
    const at = field.fresh ? 0 : end;
    if (landed || (toggled && !field.whole) || (told && field.fresh)) box.setSelectionRange(at, at);
    else box.setSelectionRange(Math.min(from, end), Math.min(to, end));
    // A word in the foot reached by Tab keeps the focus. The box takes it when the cursor has
    // just arrived, when the body opens or closes, and when it had it before the painting.
    const lost = [document.body, line].includes(document.activeElement);
    if (!leaving && (landed || toggled || focused || lost)) box.focus({ preventScroll: true });
  } else if (landed && (focused || document.activeElement === document.body)) {
    line.focus({ preventScroll: true });
  }
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
  set($('hints'), JSON.stringify([hints, vimWord]), () => {
    const words = hints.map((hint) => {
      const button = el('button', 'word', hint.label);
      button.type = 'button';
      const key = keyFor(hint.action);
      if (key) button.title = key;
      button.onclick = () => {
        act(hint.action);
        follow();
      };
      return button;
    });
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
}

let status = '';
let fading = 0;
function paintFoot() {
  // What just happened is said, and goes after five seconds. What is still true stays: that the
  // relay cannot be reached, or that this device's clock is far from the relay's, and that the
  // key opens nothing here.
  if (view.status !== status) {
    status = view.status;
    const node = $('status');
    node.textContent = status;
    node.classList.remove('faded', 'trouble');
    clearTimeout(fading);
    if (status) fading = setTimeout(() => node.classList.add('faded'), 5000);
  }
  const shut = !fzn.key_verified();
  $('trouble').hidden = !shut;
  $('passphrase').hidden = shut;
  const sync = $('sync');
  const said = synced === 'not syncing' ? synced : (fzn.clock() ?? '');
  if (sync.textContent !== said) sync.textContent = said;
  sync.hidden = !said;
}

function paintPath() {
  const path = $('path');
  const zoomed = view.path.length > 0;
  if (zoomed && path.hidden && !still.matches) {
    path.classList.add('in');
    path.addEventListener('animationend', () => path.classList.remove('in'), { once: true });
  }
  path.hidden = !zoomed;
  // Each step back along the path is a way back to it. On a narrow screen a long path gives up
  // its middle.
  set(path, JSON.stringify([view.path, narrow.matches]), () => {
    const steps = [title, ...view.path];
    const parts = [];
    steps.forEach((head, depth) => {
      const middle = narrow.matches && steps.length > 3 && depth > 0 && depth < steps.length - 1;
      if (depth > 0) parts.push(el('span', middle ? 'gone' : null, '›'));
      if (middle && depth === 1) parts.push(el('span', null, '…'));
      const step = el('button', middle ? 'gone' : null, head);
      step.type = 'button';
      step.dataset.depth = depth;
      if (depth === steps.length - 1) step.setAttribute('aria-current', 'location');
      parts.push(step);
    });
    return parts;
  });
}

// The body of what the cursor is on, beside the list, which the page shows only when it is wide
// enough to have room for it: the note under the cursor as it is being written, and on the line
// the note you are in, which is then not read above the line as well. A note landed on is read
// from its top. With the body open the sheet has it, and the pane says nothing.
let paneSays = '';
function paintPane() {
  const field = view.field;
  const says = field ? (field.whole ? '' : bodyOf(field.text)) : view.here ? bodyOf(view.here.text) : '';
  if (says !== paneSays) pane.textContent = paneSays = says;
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

  // The note you are in, when it says more than its head, clamped to six lines; a tap gives the
  // rest.
  const here = $('here');
  const told = view.here ? bodyOf(view.here.text) : '';
  if (here.dataset.shows !== told) {
    here.dataset.shows = told;
    here.textContent = told;
    here.classList.toggle('clamp', long(told));
    here.classList.remove('open');
  }
  here.hidden = !told;

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
const asMeasured = () =>
  held &&
  [held, held.selectionStart, held.selectionEnd, held.value, held.parentElement, held.className,
    $('dates').textContent, view?.cursor, composing, standing,
    inEditor() ? [editor.text().length, editor.selection().anchor, editor.selection().head, editor.mode,
      editor.composing, editor.dom.parentElement] : ''].join('\u0000');
const caretMoved = () => {
  if (asMeasured() !== measured) placeCaret();
};

function placeCaret() {
  const field = held === document.activeElement ? held : null;
  const shown =
    field &&
    view &&
    standing &&
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
  const style = getComputedStyle(field);
  for (const name of COPIED) mirror.style[name] = style[name];
  mirror.style.whiteSpace = field === box ? 'pre-wrap' : 'pre';
  mirror.style.overflowWrap = field === box ? 'anywhere' : 'normal';
  const size = parseFloat(style.fontSize);
  const padding = parseFloat(style.paddingLeft) + parseFloat(style.paddingRight);
  const at = field.selectionStart;
  const text = field.value;
  // At the end of the text, or of a line in it, there is no letter, so the block is a little over
  // half a letter wide.
  const end = at >= text.length || text[at] === '\n';
  const span = el('span', null, end ? ' ' : text[at]);
  mirror.replaceChildren(text.slice(0, at), span, end ? text.slice(at) : text.slice(at + 1));
  mirror.style.width = `${field.clientWidth - padding}px`;

  // The read. The block is the height of the line, centred on the letter as the line's own leading
  // is, and never narrower than a third of a letter, so that it is still a block over an i.
  const tall = parseFloat(style.lineHeight) || span.offsetHeight;
  const wide = end ? 0.55 * size : Math.max(span.offsetWidth, 0.35 * size);
  const left = field.offsetLeft + span.offsetLeft - field.scrollLeft - (wide - (end ? wide : span.offsetWidth)) / 2;
  const top = field.offsetTop + span.offsetTop - field.scrollTop - (tall - span.offsetHeight) / 2;

  const parent = field.parentElement;
  if (caret.parentElement !== parent) parent.append(caret);
  caret.classList.remove('bar');
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
  if (!view || !standing || !parent || !c.empty || !c.rect || composing || editor.composing) {
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
document.addEventListener('selectionchange', () => {
  if (held && document.activeElement === held) caretMoved();
});
window.addEventListener('resize', placeCaret);

const ctrlOnly = (event) => event.ctrlKey && !event.shiftKey && !event.metaKey && !event.altKey;

// The keys, wherever the focus has got to but the box and the sheet, which have their own. A key
// that closes the sheet takes what it was pressed in out of the page, so the sheet is looked for
// on the way the key came rather than round where it was pressed. Typing goes to the field the
// cursor is in, so a letter pressed on a word in the foot takes the focus there before it lands.
// A button that has the focus keeps Enter and Space.
document.addEventListener('keydown', (event) => {
  if (event.isComposing || !view || !standing || !fzn) return;
  if (event.target === box || event.composedPath().includes(buffer)) return;
  const onButton = event.target instanceof HTMLButtonElement;
  if (onButton && (event.key === 'Enter' || event.key === ' ')) return;
  if (event.key === 'Tab') return;
  const onLine = view.cursor == null;
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
    if (!onLine) act('edit');
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
    // The arrow goes in from the end of the line, as from the end of a head, and the session says
    // whether the line names anywhere to go; short of the end, or with Shift held, it is the
    // caret's.
    case 'ArrowRight':
      if (!onLine || event.shiftKey || event.target !== line || line.selectionStart !== line.selectionEnd) return;
      if (line.selectionEnd !== line.value.length) return;
      act('go_in');
      break;
    // Off the row, then off the text on the line, then out of the place.
    case 'Escape':
      if (!onLine) act(view.field?.whole ? 'done' : { select: null });
      else if (view.line !== '') act({ type: '' });
      else if (view.path.length) act('back');
      else return;
      break;
    default:
      return;
  }
  event.preventDefault();
  follow();
});

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

// A tap puts the cursor on a note, and a second, off the head, which is the box's, opens its body.
// The arrow on the selected row goes in.
list.addEventListener('click', (event) => {
  const item = event.target.closest('.row');
  if (!item || event.target === box) return;
  const at = view.rows.findIndex((row) => row.id === item.dataset.id);
  if (at < 0) return;
  if (event.target.closest('.chev')) act('go_in');
  else if (at !== view.cursor) act({ select: at });
  else if (!view.field?.whole) act('edit');
  follow();
});
list.addEventListener('animationend', (event) => event.target.classList.remove('in'));

$('here').addEventListener('click', () => $('here').classList.add('open'));

// Each step of the path goes back to where it was.
$('path').addEventListener('click', (event) => {
  const depth = event.target.dataset?.depth;
  if (depth === undefined) return;
  for (let steps = view.path.length - Number(depth); steps > 0; steps--) act('back');
  follow();
});

// A word in the foot is pressed without taking the focus from the box, so that "put back" is
// about the note and an open body is not closed first.
$('hints').addEventListener('mousedown', (event) => {
  if (view?.field && event.target.closest('button')) event.preventDefault();
});

// A page put away with the cursor on a row writes what the row holds, if it changed, and closes
// a body that is open. Being hidden comes before being unloaded and leaves more time for the line
// to be kept and sent, so it is heard too; a second `done` with nothing changed does nothing.
// Writing may change the head under the caret, as a new note written inside a tag gets the tag, so
// the box takes it at once: a hidden page has no next frame to paint it in, and a key pressed on
// coming back would otherwise be typed into the head as it was and take the tag away again.
function putAway() {
  if (!view?.field) return;
  act('done');
  safely();
}
window.addEventListener('pagehide', putAway);
document.addEventListener('visibilitychange', () => {
  if (document.visibilityState === 'hidden') putAway();
});

// Only one tab at a time is this device. A tab that opens says so, and the one that was open
// stands down: two of them writing one log would fork it.
const tabs = 'BroadcastChannel' in window ? new BroadcastChannel(`fzn:${space}`) : null;
function standDown() {
  standing = false;
  socket?.close();
  $('elsewhere').hidden = false;
  document.body.classList.add('elsewhere');
  placeCaret();
}
$('takeover').onclick = () => location.reload();

// For a passphrase mistyped: the key is forgotten and the page asks again.
const retype = () => {
  keep(KEY, null);
  location.reload();
};
$('passphrase').onclick = retype;
$('again').onclick = retype;

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
      why = `${cause}. Type the passphrase again.`;
    }
  }
  document.body.classList.remove('locked');
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
  const status = $('status');
  status.textContent = `fzn could not start: ${why}`;
  status.classList.add('trouble');
});
