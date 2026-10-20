// The open body read wherever it is not being written: the row's rule made line by line.
//
// A row under the cursor reads until it is written in, so that walking the list moves no text, and
// the body does the same a line at a time. Every line reads as the body beside the list reads, its
// marks taken out, a quote ruled, a bullet and a task's box drawn, a rule drawn and a fence's
// backticks gone from its band, until it is written in or the caret is put in it; it then shows its
// source until the caret leaves it. Walking the body with Up, Down, the pages or vim's j and k
// shows nothing, so the caret lands where the eye was, on the line as it reads. A letter,
// Backspace, Delete, a paste, a composition, Left, Right, Home and End, vim's motions along a line,
// a selection and a press show the line, and what showed it lands as it would have.
//
// Nothing here changes the text. Decorations hide marks and draw in their place, so the note, its
// undo, a copy and a paste are the source. Only marks are hidden, never words, and each only within
// its own line, since a decoration given by a plugin may not cover a line break. A line read is as
// high as written but for what its marks take once wrapped, a heading keeping its size and a
// fence's line its band, so that showing a line moves what is below it by no more than its marks.
// Tables and what a block of code holds stay as they are styled, being read best as typed, and raw
// HTML, a link's definition and `[[Boiler]]` stay as typed, as they are read. A box drawn here is
// not pressed: a press on it puts the caret beside it, as a press anywhere does.
//
// What showed a line is told by where it came from, since the editor's own commands all say only
// that they selected: Up, Down and the pages, and vim's j and k, say they walk (see `walk`). A
// press is followed on the pointer, and the line it lands on is shown only once the pointer is let
// go, so that nothing moves under a pointer held down or dragging a selection; a finger's caret
// arrives after it lifts, so what the selection does just after counts as the tap's. A key that
// writes shows the line before it lands, so that a composition the key starts begins in the line as
// typed and is never redrawn under the input method. In vim's normal and visual mode a letter is a
// command and writes nothing, so there it shows nothing until it changes the text.

import { Annotation, EditorState, Prec, StateEffect, StateField } from '@codemirror/state';
import { Decoration, EditorView, ViewPlugin, WidgetType } from '@codemirror/view';
import { syntaxTree } from '@codemirror/language';
import { getCM } from '@replit/codemirror-vim';
import { between, follows } from './render.js';

// A move made by walking, a move made while a pointer is held on the text, and one made by a tap
// just after the finger lifted. And what a pointer let go or a key that writes asks: show the lines
// the selection is on.
const walked = Annotation.define();
const held = Annotation.define();
const tapped = Annotation.define();
const show = StateEffect.define();

// There is one editor on the page, so what the keys and the pointer are doing is kept here and read
// as each transaction is made.
let walking = false;
let pressing = null;
let lifted = 0;
// Whether the caret moved while the pointer was held, as a pen's or a long press's does, so that
// letting go shows where it went.
let moved = false;
// How long after a finger lifts its caret may take to arrive.
const TAP = 600;

// A move made now walks, and so does every move made before this task ends, since vim moves the
// caret after its motion has said where to.
export function walk() {
  walking = true;
  queueMicrotask(() => {
    walking = false;
  });
}

// A command of the editor's that walks.
export const walks = (command) => (view) => {
  walk();
  return command(view);
};

// The whole lines a selection is on, from the start of its first to the end of its last.
function linesOf(state) {
  const { doc, selection } = state;
  let from = doc.length;
  let to = 0;
  for (const range of selection.ranges) {
    from = Math.min(from, doc.lineAt(range.from).from);
    to = Math.max(to, doc.lineAt(range.to).to);
  }
  return { from, to };
}
const lineAt = (state, pos) => {
  const line = state.doc.lineAt(pos);
  return { from: line.from, to: line.to };
};
const same = (a, b) => a === b || (!!a && !!b && a.from === b.from && a.to === b.to);

// The lines shown as typed, or null while every line reads: the caret's line, or every line a
// selection is on. A body opens with none. Whatever changes the text, the person's typing, an undo
// or a version put in from elsewhere, shows the line the caret ends on, as a row changed in place
// is revealed.
const written = StateField.define({
  create: () => null,
  update(lines, tr) {
    const next = after(lines, tr);
    return same(lines, next) ? lines : next;
  },
});

function after(lines, tr) {
  const { state } = tr;
  if (tr.docChanged || tr.effects.some((effect) => effect.is(show))) return linesOf(state);
  if (!tr.selection || tr.annotation(held)) return lines;
  const main = state.selection.main;
  const selected = !main.empty || state.selection.ranges.length > 1;
  if (selected || tr.annotation(tapped)) return linesOf(state);
  // A caret still on a line shown keeps it shown, its own line alone once a selection over several
  // has gone.
  if (lines && main.head >= lines.from && main.head <= lines.to) return lineAt(state, main.head);
  // A walk reads the line it reaches, and so does a move to another line by anything else; a move
  // along the caret's own line shows it.
  const before = tr.startState.selection.main.head;
  if (main.head === before) return lines;
  if (tr.annotation(walked)) return null;
  const along = state.doc.lineAt(before).from === state.doc.lineAt(main.head).from;
  return along ? lineAt(state, main.head) : null;
}

// Whether an update showed a line or put one back to reading, which moves the caret's letters.
export const writtenChanged = (update) =>
  update.startState.field(written, false) !== update.state.field(written, false);

// A move is marked as it is made by what is happening then: a pointer held, a walk, a tap.
const marked = EditorState.transactionExtender.of((tr) => {
  if (tr.docChanged || !tr.selection) return null;
  if (pressing) {
    moved = true;
    return { annotations: held.of(true) };
  }
  if (walking) return { annotations: walked.of(true) };
  if (Date.now() - lifted < TAP) return { annotations: tapped.of(true) };
  return null;
});

// Shows the lines the selection is on, unless they are shown already or a composition is under
// way, which a redrawn line would break.
function showHere(view) {
  if (view.composing || same(view.state.field(written), linesOf(view.state))) return;
  view.dispatch({ effects: show.of(null) });
}

// Whether the keys write here: always, but in vim's normal and visual mode.
function keysWrite(view) {
  const state = getCM(view)?.state.vim;
  return !state || !!state.insertMode;
}

// The keys that write, and so show the line before they land. A key held for an input method says
// it is processing, 229, and a dead key waits for the letter it starts.
const WRITING = new Set(['Backspace', 'Delete', 'Process', 'Dead', 'Unidentified']);
const writes = (event) =>
  event.keyCode === 229 ||
  WRITING.has(event.key) ||
  (event.key.length === 1 && !event.ctrlKey && !event.metaKey);

const listening = Prec.highest(
  EditorView.domEventHandlers({
    pointerdown(event) {
      if (event.button === 0) pressing = event.pointerType || 'mouse';
      lifted = 0;
      return false;
    },
    keydown(event, view) {
      lifted = 0;
      if (writes(event) && keysWrite(view)) showHere(view);
      return false;
    },
    compositionstart(event, view) {
      if (keysWrite(view)) showHere(view);
      return false;
    },
    // What writes without a key: dictation, a word the spelling offers, a character picked.
    beforeinput(event, view) {
      if (keysWrite(view)) showHere(view);
      return false;
    },
  }),
);

// A pointer let go anywhere ends the press. A mouse's caret was put where it pressed, so its line
// is shown now; a finger's arrives after it lifts, and is shown as it comes (see `marked`). A press
// that became a scroll shows nothing.
const letGo = ViewPlugin.define((view) => {
  const release = (event) => {
    if (!pressing) return;
    const mouse = pressing === 'mouse';
    const went = moved;
    pressing = null;
    moved = false;
    if (event.type === 'pointercancel') return;
    if (!mouse) lifted = Date.now();
    if ((mouse || went) && view.dom.isConnected) showHere(view);
  };
  window.addEventListener('pointerup', release, true);
  window.addEventListener('pointercancel', release, true);
  return {
    destroy() {
      window.removeEventListener('pointerup', release, true);
      window.removeEventListener('pointercancel', release, true);
      pressing = null;
      moved = false;
    },
  };
});

class Bullet extends WidgetType {
  eq() {
    return true;
  }
  toDOM() {
    const bullet = document.createElement('span');
    bullet.className = 'md-bullet';
    bullet.textContent = '•';
    return bullet;
  }
  ignoreEvent() {
    return false;
  }
}

class Box extends WidgetType {
  constructor(done) {
    super();
    this.done = done;
  }
  eq(other) {
    return other.done === this.done;
  }
  toDOM() {
    const box = document.createElement('span');
    box.className = this.done ? 'md-check done' : 'md-check';
    return box;
  }
  ignoreEvent() {
    return false;
  }
}

const hidden = Decoration.replace({});
const bullet = Decoration.replace({ widget: new Bullet() });
const open = Decoration.replace({ widget: new Box(false) });
const ticked = Decoration.replace({ widget: new Box(true) });
const quoteLine = Decoration.line({ class: 'md-quote' });
const ruleLine = Decoration.line({ class: 'md-rule' });
const doneLine = Decoration.line({ class: 'md-done' });
const SCHEME = /^(?:https?:\/\/|mailto:)/i;

// What reads, as far as the view reaches: every mark on a line not shown, hidden or drawn.
function reading(view) {
  const { state } = view;
  const { doc } = state;
  const lines = state.field(written);
  const shown = (pos) => !!lines && pos >= lines.from && pos <= lines.to;
  const out = [];
  const hide = (from, to, how = hidden) => {
    if (to <= from || shown(from) || doc.lineAt(from).number !== doc.lineAt(to).number) return;
    out.push(how.range(from, to));
  };
  const mark = (pos, how) => {
    if (!shown(pos)) out.push(how.range(doc.lineAt(pos).from));
  };
  for (const { from, to } of view.visibleRanges) {
    syntaxTree(state).iterate({
      from,
      to,
      enter(ref) {
        const name = ref.name;
        if (/^ATXHeading\d$/.test(name)) {
          // The opening mark goes with the space after it; a closing one goes alone.
          const [first, ...rest] = ref.node.getChildren('HeaderMark');
          if (first) {
            let end = first.to;
            while (end < ref.to && /[ \t]/.test(doc.sliceString(end, end + 1))) end++;
            hide(first.from, end);
          }
          for (const closing of rest) hide(closing.from, closing.to);
          return;
        }
        switch (name) {
          // A block of code keeps what it holds as typed, and its fences' lines their band.
          case 'FencedCode': {
            const marks = ref.node.getChildren('CodeMark');
            const info = ref.node.getChild('CodeInfo');
            if (marks[0]) hide(marks[0].from, (info ?? marks[0]).to);
            if (marks.length > 1) hide(marks[marks.length - 1].from, marks[marks.length - 1].to);
            return false;
          }
          case 'CodeBlock':
          case 'Table':
          case 'HTMLBlock':
          case 'CommentBlock':
          case 'LinkReference':
          case 'Bracketed':
            return false;
          case 'EmphasisMark':
          case 'StrikethroughMark':
            return hide(ref.from, ref.to);
          case 'CodeMark':
            if (ref.node.parent?.name === 'InlineCode') hide(ref.from, ref.to);
            return;
          case 'Escape':
            return hide(ref.from, ref.from + 1);
          // A link the page follows reads as its words, which keep what is marked in them; any other
          // is shown as typed, as it is read.
          case 'Link':
          case 'Image': {
            const node = ref.node;
            const [a, b] = between(node);
            const url = node.getChildren('URL').find((u) => u.from > b);
            if (!url || !follows(doc.sliceString(url.from, url.to)) || b <= a) return;
            hide(ref.from, a);
            hide(b, ref.to);
            return;
          }
          case 'Autolink': {
            const url = ref.node.getChild('URL');
            const address = url ? doc.sliceString(url.from, url.to) : '';
            if (!follows(address)) return false;
            hide(ref.from, url.from + SCHEME.exec(address)[0].length);
            hide(url.to, ref.to);
            return false;
          }
          // An address standing alone reads without its scheme, as it does read.
          case 'URL': {
            const up = ref.node.parent?.name;
            if (up === 'Link' || up === 'Image') return;
            const address = doc.sliceString(ref.from, ref.to);
            if (follows(address)) hide(ref.from, ref.from + SCHEME.exec(address)[0].length);
            return;
          }
          // A quote is ruled down its lines, once however deep, and its marks go.
          case 'Blockquote': {
            if (ref.node.parent?.name === 'Blockquote') return;
            for (let line = doc.lineAt(ref.from); ; line = doc.line(line.number + 1)) {
              mark(line.from, quoteLine);
              if (line.to >= ref.to || line.number === doc.lines) break;
            }
            return;
          }
          case 'QuoteMark': {
            const end = doc.sliceString(ref.to, ref.to + 1) === ' ' ? ref.to + 1 : ref.to;
            return hide(ref.from, end);
          }
          // A task's item reads as its box, and a bullet as a dot; a number reads as typed.
          case 'ListMark': {
            const item = ref.node.parent;
            const task = item?.getChild('Task');
            if (task) return hide(ref.from, Math.min(task.from, ref.to + 1));
            if (item?.parent?.name === 'BulletList') hide(ref.from, ref.to, bullet);
            return;
          }
          case 'TaskMarker': {
            const done = /x/i.test(doc.sliceString(ref.from, ref.to));
            if (done) mark(ref.from, doneLine);
            return hide(ref.from, ref.to, done ? ticked : open);
          }
          case 'HorizontalRule':
            mark(ref.from, ruleLine);
            return hide(ref.from, ref.to);
        }
      },
    });
  }
  // Hidden marks that touch are made one, so that no caret can stop between them, as it could
  // inside `***`, and write into the middle of the run.
  out.sort((a, b) => a.from - b.from || a.value.startSide - b.value.startSide);
  const merged = [];
  for (const range of out) {
    const last = merged[merged.length - 1];
    if (last && range.value === hidden && last.value === hidden && last.to === range.from) {
      merged[merged.length - 1] = hidden.range(last.from, range.to);
    } else merged.push(range);
  }
  return Decoration.set(merged, true);
}

const read = ViewPlugin.define(
  (view) => ({
    decorations: reading(view),
    update(u) {
      const parsed = syntaxTree(u.startState) !== syntaxTree(u.state);
      if (u.docChanged || u.viewportChanged || writtenChanged(u) || parsed) this.decorations = reading(u.view);
    },
  }),
  { decorations: (plugin) => plugin.decorations },
);

// Everything the body needs to read where it is not written.
export const readWhereNotWritten = [written, marked, listening, letGo, read];
