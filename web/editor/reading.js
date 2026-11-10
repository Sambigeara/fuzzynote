// A note read wherever it is not being written, in the head of the row under the cursor and in the
// open body alike: a mark shows only in the span the selection touches.
//
// Every line reads as the page reads it elsewhere, its marks taken out, a quote ruled, a bullet
// and a task's box drawn, a rule drawn and a fence's backticks gone from its band, and a span
// shows its marks as typed while the selection touches it, at either edge, by any of its ranges:
// emphasis, strong text, strikethrough, code, a link with its address, an address, an escape. A
// line's lead, from its start to the space after the last mark that opens it, a quote's, a list
// item's and a task's box, a heading's, is one span, so that a caret at the start of a line's
// words shows how the line is made and where a letter will go; a rule shows while the caret is on
// its line, and a fence while the caret is anywhere in its block. Nothing else decides it: a span
// is shown however the caret came to it and read again once it has left, as SilverBullet and
// Obsidian show theirs, but that a text just arrived at reads whole until the caret moves or
// something is written (see read). Since a hidden mark always lies inside its span, a caret at
// either side of one touches the span and shows it, so no caret ever stops beside a mark it cannot
// see.
//
// The first line is the head, in a row and in the open body alike, and its box, `[ ]` or `[x]` at
// its start, is the task's: with the space after it, and a heading's mark after that, it is the
// head's lead, drawn as a box while the selection does not touch it. In a row the box is drawn as
// the list draws it and ticked by the page when it is pressed; in the open body it is drawn as a
// body's box is, which a press does not tick. A row's head is one line, read as a head is read in
// the list (see render.js). In a place, the place's labels that end the head read hidden, in a row
// and in the open body alike, as the row reads off the cursor, until the caret goes into them; a
// caret before them, where the caret lands, leaves them hidden and writes before them.
//
// Nothing here changes the text. Decorations hide marks and draw in their place, so the note, its
// undo, a copy and a paste are the source. Only marks are hidden, and a place's labels, and each
// only within its own line, since a decoration given by a plugin may not cover a line break; a
// table in a body, drawn in place of its lines, is the one exception, and is made apart (see
// `tables`). A line read is as high as written but for what its marks take once wrapped, a
// heading's size being its line's and a fence's line its band. What a block of code holds stays as
// it is styled, being read best as typed, and raw HTML, a link's definition and `[[Boiler]]` stay
// as typed, as they are read. A box drawn in a body is not pressed: a press on it puts the caret
// beside it, as a press anywhere does.
//
// While a composition is under way the marks hold still, moved only with the text, so that no mark
// opens or closes beside what an input method is writing; they are read again once it has ended.
// So they do while a press is held, and the span it lands in opens once it is let go.

import { Annotation, EditorSelection, EditorState, Facet, RangeSet, StateEffect, StateField } from '@codemirror/state';
import { Decoration, EditorView, ViewPlugin, WidgetType } from '@codemirror/view';
import { syntaxTree } from '@codemirror/language';
import { between, body, follows, imageId, picture } from './render.js';

// The text is a row's head, one line, and not a whole note.
export const asHead = Facet.define({ combine: (given) => given.length > 0 });

// The place's labels that end the head, which its row reads without, as the page has them from
// the engine; none outside a place.
export const placeLabels = Facet.define({ combine: (given) => given[0] ?? '' });

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

// A task's box: a head's, which the page ticks when it is pressed and so the editor leaves alone,
// or a body's, which a press puts the caret beside.
class Box extends WidgetType {
  constructor(done, head) {
    super();
    this.done = done;
    this.head = head;
  }
  eq(other) {
    return other.done === this.done && other.head === this.head;
  }
  toDOM() {
    const box = document.createElement('span');
    box.className = `${this.head ? 'tick' : 'md-check'}${this.done ? ' done' : ''}`;
    box.setAttribute('aria-label', this.done ? '[x]' : '[ ]');
    return box;
  }
  ignoreEvent() {
    return this.head;
  }
}

// An image of the space's own with no alt text, in a head, reads as the faint word `image`.
class ImageWord extends WidgetType {
  eq() {
    return true;
  }
  toDOM() {
    const word = document.createElement('span');
    word.className = 'md-image-word';
    word.textContent = 'image';
    return word;
  }
  ignoreEvent() {
    return false;
  }
}

const hidden = Decoration.replace({});
const imageWord = Decoration.replace({ widget: new ImageWord() });
const bullet = Decoration.replace({ widget: new Bullet() });
const open = Decoration.replace({ widget: new Box(false, false) });
const ticked = Decoration.replace({ widget: new Box(true, false) });
const tick = Decoration.replace({ widget: new Box(false, true) });
const untick = Decoration.replace({ widget: new Box(true, true) });
const quoteLine = Decoration.line({ class: 'md-quote' });
const ruleLine = Decoration.line({ class: 'md-rule' });
const doneLine = Decoration.line({ class: 'md-done' });
const headingLines = [1, 2, 3, 4, 5, 6].map((n) => Decoration.line({ class: `md-h${n}` }));
const SCHEME = /^(?:https?:\/\/|mailto:)/i;
// A head's box, as the engine reads one, with the space after it and a heading's mark after that,
// which a row reads without as it reads a heading's.
const BOX = /^\[[ x]\] ?(?:#{1,6}[ \t]+(?=\S))?/;
// The marks a span of text holds, by the kind of span.
const MARK = {
  Emphasis: 'EmphasisMark',
  StrongEmphasis: 'EmphasisMark',
  Strikethrough: 'StrikethroughMark',
  InlineCode: 'CodeMark',
};

// A change the page makes, which is not the person's and not one to tell the page about: a head or
// a body put back, changed from elsewhere or its words written out, or the caret the engine put.
export const fromPage = Annotation.define();

// What reads, as far as the view reaches: every mark of a span the selection does not touch,
// hidden or drawn. A text just arrived at touches nothing.
function reading(view, arrived) {
  const { state } = view;
  const { doc, selection } = state;
  const tree = syntaxTree(state);
  const head = state.facet(asHead);
  const touched = (from, to) => !arrived && selection.ranges.some((r) => r.from <= to && r.to >= from);
  const touchedLine = (pos) => {
    const line = doc.lineAt(pos);
    return touched(line.from, line.to);
  };
  const out = [];
  const hide = (from, to, how = hidden) => {
    if (to <= from || doc.lineAt(from).number !== doc.lineAt(to).number) return;
    out.push(how.range(from, to));
  };
  const mark = (pos, how) => out.push(how.range(doc.lineAt(pos).from));
  // Past the space after a mark, or every space and tab after it.
  const past = (to, all = false) => {
    while (/[ \t]/.test(doc.sliceString(to, to + 1))) {
      to++;
      if (!all) break;
    }
    return to;
  };
  // Where each line's lead ends, found once a line as it is asked for: past the last of the marks
  // that open it. A line with none has none.
  const leads = new Map();
  const leadOf = (line) => {
    if (leads.has(line.number)) return leads.get(line.number);
    let end = -1;
    if (line.number === 1) {
      const box = BOX.exec(line.text);
      if (box) end = line.from + box[0].length;
    }
    tree.iterate({
      from: line.from,
      to: line.to,
      enter(ref) {
        if (ref.from > line.to) return false;
        if (ref.from < line.from) return;
        switch (ref.name) {
          case 'QuoteMark':
            end = Math.max(end, past(ref.to));
            return;
          case 'ListMark': {
            const marker = ref.node.parent?.getChild('Task')?.getChild('TaskMarker');
            end = Math.max(end, past((marker ?? ref.node).to));
            return;
          }
          case 'HeaderMark':
            if (/^ATXHeading/.test(ref.node.parent?.name) && ref.from === ref.node.parent.from) {
              end = Math.max(end, past(ref.to, true));
            }
            return;
        }
      },
    });
    leads.set(line.number, end);
    return end;
  };
  const leadTouched = (pos) => {
    const line = doc.lineAt(pos);
    const end = leadOf(line);
    return end >= 0 && touched(line.from, end);
  };
  // A head's box is drawn in its place, the space after it and a heading's mark after that gone:
  // a row's to be ticked, the open body's to be read.
  const first = doc.line(1);
  const box = BOX.exec(first.text);
  if (box && !leadTouched(first.from)) {
    const done = box[0][1] === 'x';
    hide(first.from, first.from + box[0].length, head ? (done ? untick : tick) : done ? ticked : open);
  }
  for (const { from, to } of view.visibleRanges) {
    tree.iterate({
      from,
      to,
      enter(ref) {
        const name = ref.name;
        const node = ref.node;
        // A heading's mark is its line's lead, and goes with the spaces after it, and a closing one
        // is a span of its own; what is marked in its words is a span of its own too. Its size is
        // its line's, given whether or not the mark shows, so that showing the mark changes no
        // height.
        if (/^ATXHeading\d$/.test(name)) {
          mark(ref.from, headingLines[Number(name.slice(-1)) - 1]);
          const [first, ...rest] = node.getChildren('HeaderMark');
          if (first && !leadTouched(ref.from)) hide(first.from, past(first.to, true));
          for (const closing of rest) if (!touched(closing.from, closing.to)) hide(closing.from, closing.to);
          return;
        }
        if (name === 'SetextHeading1' || name === 'SetextHeading2') {
          mark(ref.from, headingLines[Number(name.slice(-1)) - 1]);
          return;
        }
        if (name in MARK) {
          if (touched(ref.from, ref.to)) return;
          for (let child = node.firstChild; child; child = child.nextSibling) {
            if (child.name === MARK[name]) hide(child.from, child.to);
          }
          return;
        }
        switch (name) {
          // A block of code keeps what it holds as typed, and its fences' lines their band, their
          // backticks shown while the caret is anywhere in the block.
          case 'FencedCode': {
            if (touched(ref.from, ref.to)) return false;
            const marks = node.getChildren('CodeMark');
            const info = node.getChild('CodeInfo');
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
          case 'Escape':
            if (!touched(ref.from, ref.to)) hide(ref.from, ref.from + 1);
            return;
          // A link the page follows reads as its words, which keep what is marked in them, and
          // shows its brackets and address while the selection touches any of it; any other is
          // shown as typed, as it is read.
          case 'Link':
          case 'Image': {
            if (touched(ref.from, ref.to)) return;
            const [a, b] = between(node);
            const url = node.getChildren('URL').find((u) => u.from > b);
            // An image of the space's own reads as its alt text, as a link reads as its words, its
            // picture drawn under its line (see `pictures`); with none, a head reads the faint word
            // `image` and in a body it reads as nothing.
            if (name === 'Image' && url && imageId(doc.sliceString(url.from, url.to))) {
              if (b > a) {
                hide(ref.from, a);
                hide(b, ref.to);
              } else if (head) hide(ref.from, ref.to, imageWord);
              else {
                // Read as nothing at the start of what its line reads, it takes the space after it
                // too, so that the line's words start where they would without it.
                const line = doc.lineAt(ref.from);
                const first = !doc.sliceString(line.from, ref.from).trim() || leadOf(line) >= ref.from;
                hide(ref.from, first ? past(ref.to, true) : ref.to);
              }
              return;
            }
            if (!url || !follows(doc.sliceString(url.from, url.to)) || b <= a) return;
            hide(ref.from, a);
            hide(b, ref.to);
            return;
          }
          case 'Autolink': {
            const url = node.getChild('URL');
            const address = url ? doc.sliceString(url.from, url.to) : '';
            if (touched(ref.from, ref.to) || !follows(address)) return false;
            hide(ref.from, url.from + SCHEME.exec(address)[0].length);
            hide(url.to, ref.to);
            return false;
          }
          // An address standing alone reads without its scheme, as it does read.
          case 'URL': {
            const up = node.parent?.name;
            if (up === 'Link' || up === 'Image' || touched(ref.from, ref.to)) return;
            const address = doc.sliceString(ref.from, ref.to);
            if (follows(address)) hide(ref.from, ref.from + SCHEME.exec(address)[0].length);
            return;
          }
          // A quote is ruled down its lines, once however deep, but on a line whose lead the
          // caret touches, where its marks show instead.
          case 'Blockquote': {
            if (node.parent?.name === 'Blockquote') return;
            for (let line = doc.lineAt(ref.from); ; line = doc.line(line.number + 1)) {
              if (!leadTouched(line.from)) mark(line.from, quoteLine);
              if (line.to >= ref.to || line.number === doc.lines) break;
            }
            return;
          }
          case 'QuoteMark':
            if (!leadTouched(ref.from)) hide(ref.from, past(ref.to));
            return;
          // A bullet reads as a dot, a task as its box, ticked and faint when done, and a number
          // as typed.
          case 'ListMark': {
            const item = node.parent;
            const marker = item?.getChild('Task')?.getChild('TaskMarker');
            const done = !!marker && /x/i.test(doc.sliceString(marker.from, marker.to));
            if (done) mark(ref.from, doneLine);
            if (leadTouched(ref.from)) return;
            if (marker) {
              hide(ref.from, marker.from);
              hide(marker.from, marker.to, done ? ticked : open);
            } else if (item?.parent?.name === 'BulletList') hide(ref.from, ref.to, bullet);
            return;
          }
          case 'HorizontalRule':
            if (touchedLine(ref.from)) return;
            mark(ref.from, ruleLine);
            return hide(ref.from, ref.to);
        }
      },
    });
  }
  // A place's labels that end the head read hidden until the caret goes into them, past their
  // start, so that the caret landing before them, and what it types there, leave them so.
  const labels = labelsAt(state);
  const into = (r) => r.to > labels.from && r.from <= labels.to;
  if (labels && (arrived || !selection.ranges.some(into))) hide(labels.from, labels.to);
  return Decoration.set(out, true);
}

// Where the place's labels are while they end the head: from the space before them to the end of
// its line. A new note's head that is nothing but them reads as nothing, a title to write.
export function labelsAt(state) {
  const labels = state.facet(placeLabels);
  if (!labels) return null;
  const line = state.doc.line(1);
  const kept = line.text.trimEnd().length;
  const from = kept - labels.length;
  if (from < 0 || line.text.slice(from, kept) !== labels) return null;
  return { from: line.from + from, to: line.to };
}

// Where what a head reads ends: before the place's labels while they end it, or at its end. A
// caret the page puts past it goes there.
export const readEnd = (state) => labelsAt(state)?.from ?? state.doc.length;

// Whether a transaction put the whole text in place of the old, as a version from elsewhere does,
// which leaves nothing for a held reading to be moved through.
const replacesAll = (tr) => {
  let all = false;
  tr.changes.iterChangedRanges((from, to) => {
    if (from === 0 && to === tr.startState.doc.length) all = true;
  });
  return all;
};

// The reading, made again whenever the text, the selection, the view or the parse moves, but held
// while a composition is under way or a press is held, its marks moved only with the text, and
// made again at the first update once it has ended. A press is held still because a span opening
// under the pointer moves the text, and the editor would read the smallest tremor over moved text
// as a drag, selecting letters where a caret was meant. An input method that ends a composition
// without changing the text, and a press let go, are given an update to make it in. A whole text
// put in while it is held is read afresh, since there is nothing to move the marks through.
//
// A text arrived at, a head the cursor lands on or a body opening, reads whole, its caret touching
// nothing, until the person moves the caret or writes, so that arriving at the end of a head that
// ends in a mark opens nothing; a change the page makes is not one.
const read = ViewPlugin.fromClass(
  class {
    constructor(view) {
      this.arrived = true;
      this.decorations = reading(view, true);
      this.held = false;
      this.pressed = false;
      this.moved = false;
      this.view = view;
      this.release = () => {
        if (!this.pressed) return;
        this.pressed = false;
        this.view.dispatch({});
      };
      const doc = view.dom.ownerDocument;
      doc.addEventListener('mouseup', this.release);
      doc.addEventListener('pointercancel', this.release);
    }
    update(u) {
      const before = this.decorations;
      const moves = (tr) => (tr.selection || tr.docChanged) && !tr.annotation(fromPage);
      const left = this.arrived && u.transactions.some(moves);
      if (left) this.arrived = false;
      if ((u.view.composing || this.pressed) && !u.transactions.some(replacesAll)) {
        this.decorations = before.map(u.changes);
        this.held = true;
      } else if (
        this.held ||
        left ||
        u.docChanged ||
        u.selectionSet ||
        u.viewportChanged ||
        syntaxTree(u.startState) !== syntaxTree(u.state)
      ) {
        this.decorations = reading(u.view, this.arrived);
        this.held = u.view.composing || this.pressed;
      }
      this.moved = !RangeSet.eq([before], [this.decorations]);
    }
    destroy() {
      const doc = this.view.dom.ownerDocument;
      doc.removeEventListener('mouseup', this.release);
      doc.removeEventListener('pointercancel', this.release);
    }
  },
  {
    decorations: (plugin) => plugin.decorations,
    eventHandlers: {
      mousedown() {
        this.pressed = true;
      },
      compositionend(event, view) {
        setTimeout(() => {
          if (this.held && !view.composing) view.dispatch({});
        });
      },
    },
  },
);

// Whether the span from `from` to `to` shows its marks, being written in: the selection touches it
// and the text has been moved in since it was arrived at, as the reading judges it.
export const writtenIn = (view, from, to) =>
  !view.plugin(read)?.arrived && view.state.selection.ranges.some((r) => r.from <= to && r.to >= from);

// Whether an update opened or closed a mark, which moves the caret's letters.
export const readingMoved = (update) => !!update.view.plugin(read)?.moved;

// A table in the open body is drawn as the body beside the list draws it, in the sheet's whole
// width, until the selection touches it, when it is shown as typed, as any span is; a text just
// arrived at touches nothing. It stands in for whole lines, which only a state field may replace,
// so it is apart from the reading above. A drawn table is as high as its source, its head's rule
// taking the room of the row of dashes, so that nothing moves as the caret goes in or out; a press
// on it puts the caret in the cell pressed (see `cellAt`), one on its scrollbar only scrolls it,
// and the arrows walk into it (see `walkIn`). One that does not start and end its lines, inside a
// quote or a list, is shown as typed. One wider than the room fades at each edge that hides
// columns, as a head being written does, which is read again as it scrolls and as it or the room
// changes width, a font arriving included, and the room its bar takes under the fade is measured,
// being none where the bar is laid over the table or there is none. Under a mouse its bar shows
// while the pointer is over it, by a class rather than `:hover`, since Chromium does not always
// paint a scrollbar again when only its box's `:hover` has changed, and left one shown, or hidden,
// until something else was painted.
class Drawn extends WidgetType {
  constructor(source) {
    super();
    this.source = source;
  }
  eq(other) {
    return other.source === this.source;
  }
  toDOM() {
    const box = document.createElement('div');
    box.className = 'md-table-read';
    box.style.setProperty('--lines', this.source.split('\n').length);
    box.append(body(this.source));
    const edges = () => {
      const over = box.scrollWidth - box.clientWidth;
      box.classList.toggle('before', box.scrollLeft > 1);
      box.classList.toggle('after', box.scrollLeft < over - 1);
      box.style.setProperty('--under', `${box.offsetHeight - box.clientHeight}px`);
    };
    box.addEventListener('scroll', edges, { passive: true });
    box.addEventListener('pointerenter', () => box.classList.add('pointed'));
    box.addEventListener('pointerleave', () => box.classList.remove('pointed'));
    const sized = new ResizeObserver(edges);
    sized.observe(box);
    if (box.firstElementChild) sized.observe(box.firstElementChild);
    measured.set(box, sized);
    return box;
  }
  destroy(box) {
    measured.get(box)?.disconnect();
  }
  // A press on its own scrollbar is the browser's to scroll it by, so the editor does not hear it:
  // the caret stays where it was and the table stays drawn. While the table is wider than the box,
  // its cells cover the box everywhere but the scrollbar, drawn beside them or over them, so a
  // press whose target is the box itself is a press on the scrollbar.
  ignoreEvent(event) {
    const pressed = event.target;
    return (
      event.type === 'mousedown' &&
      pressed.classList?.contains('md-table-read') &&
      pressed.scrollWidth > pressed.clientWidth
    );
  }
}

// What watches each drawn table's width, let go when the table is.
const measured = new WeakMap();

function drawn(state, arrived) {
  if (state.facet(asHead)) return Decoration.none;
  const { doc, selection } = state;
  const out = [];
  syntaxTree(state).iterate({
    enter(ref) {
      if (ref.name !== 'Table') return;
      const to = doc.lineAt(ref.to).to;
      const whole = doc.lineAt(ref.from).from === ref.from && doc.sliceString(ref.to, to).trim() === '';
      const touched = !arrived && selection.ranges.some((r) => r.from <= to && r.to >= ref.from);
      if (whole && !touched) {
        const widget = new Drawn(doc.sliceString(ref.from, to));
        out.push(Decoration.replace({ widget, block: true }).range(ref.from, to));
      }
      return false;
    },
  });
  return Decoration.set(out);
}

const tables = StateField.define({
  create: (state) => ({ arrived: true, tables: drawn(state, true) }),
  update(value, tr) {
    const arrived = value.arrived && !((tr.selection || tr.docChanged) && !tr.annotation(fromPage));
    const moved = tr.docChanged || tr.selection || arrived !== value.arrived;
    if (!moved && syntaxTree(tr.startState) === syntaxTree(tr.state)) return value;
    return { arrived, tables: drawn(tr.state, arrived) };
  },
  provide: (field) => EditorView.decorations.from(field, (value) => value.tables),
});

// Said to the editor when an image has come, or been opened, so that its picture is drawn.
export const picturesChanged = StateEffect.define();

// How wide the open body's text is, and how high its line, as last drawn, for a picture's height
// to be guessed before it is drawn: the picture's own and the room above and below it, a quarter
// of a line and a half, which is padding so that the editor counts it.
let measure = 0;
let lineHeight = 0;

// An image of the space's own is drawn under the line it is written on, in the open body, whole
// wherever the caret is, so that nothing moves as the caret goes in or out of its line, which
// shows its source as any span does (see "An image is drawn under its line"). The picture is a
// block after its line, which only a state field may give, as `tables` does. One in a table reads
// as its alt text, as a cell is one line, and a head draws none, being one line.
class Picture extends WidgetType {
  constructor(id, alt, got) {
    super();
    this.id = id;
    this.alt = alt;
    this.got = got;
  }
  eq(other) {
    return (
      other.id === this.id &&
      other.alt === this.alt &&
      (other.got?.url ?? null) === (this.got?.url ?? null) &&
      (other.got === 'missing') === (this.got === 'missing')
    );
  }
  get estimatedHeight() {
    const { got } = this;
    if (!got?.w || !got?.h || !measure) return -1;
    return Math.min(got.h, (got.h * measure) / got.w, 0.6 * window.innerHeight) + 0.75 * lineHeight;
  }
  toDOM(view) {
    measure = view.contentDOM.clientWidth || measure;
    lineHeight = view.defaultLineHeight || lineHeight;
    const box = document.createElement('div');
    box.className = 'md-picture';
    const { got } = this;
    if (got === 'missing') {
      box.classList.add('missing');
      box.textContent = 'not here yet';
    } else if (got) {
      const img = box.appendChild(document.createElement('img'));
      img.src = got.url;
      if (got.w && got.h) {
        img.width = got.w;
        img.height = got.h;
      }
      img.alt = this.alt;
      img.onload = () => view.requestMeasure();
    }
    return box;
  }
  ignoreEvent() {
    return false;
  }
}

function pictured(state) {
  if (state.facet(asHead)) return Decoration.none;
  const { doc } = state;
  const out = [];
  syntaxTree(state).iterate({
    enter(ref) {
      if (ref.name === 'Table') return false;
      if (ref.name !== 'Image') return;
      const node = ref.node;
      const [a, b] = between(node);
      const url = node.getChildren('URL').find((u) => u.from > b);
      const id = url && imageId(doc.sliceString(url.from, url.to));
      if (!id) return false;
      const widget = new Picture(id, doc.sliceString(a, b), picture(id));
      out.push(Decoration.widget({ widget, block: true, side: 1 }).range(doc.lineAt(node.to).to));
      return false;
    },
  });
  return Decoration.set(out, true);
}

const pictures = StateField.define({
  create: pictured,
  update(value, tr) {
    const changed = tr.docChanged || tr.effects.some((effect) => effect.is(picturesChanged));
    if (!changed && syntaxTree(tr.startState) === syntaxTree(tr.state)) return value;
    return pictured(tr.state);
  },
  provide: (field) => EditorView.decorations.from(field),
});

// An arrow, or vim's `j` or `k`, steps over what stands in for whole lines, and a drawn table would
// be walked past unread; so a step from the line just above a table to the line just below it, or
// back, lands in its first row, or its last, at the same column, which shows it as typed.
const walkIn = EditorState.transactionFilter.of((tr) => {
  const was = tr.startState.selection.main;
  if (!tr.selection || tr.docChanged || tr.annotation(fromPage) || tr.isUserEvent('select.pointer')) return tr;
  const now = tr.selection.main;
  if (!was.empty || !now.empty || tr.selection.ranges.length > 1) return tr;
  const { doc } = tr.startState;
  const [from, to] = [doc.lineAt(was.head), doc.lineAt(now.head)];
  let into = null;
  tr.startState.field(tables).tables.between(0, doc.length, (start, end) => {
    const [first, last] = [doc.lineAt(start).number, doc.lineAt(end).number];
    if (from.number === first - 1 && to.number === last + 1) into = doc.line(first);
    if (from.number === last + 1 && to.number === first - 1) into = doc.line(last);
  });
  if (!into) return tr;
  const column = Math.min(was.head - from.from, into.length);
  return [tr, { selection: EditorSelection.cursor(into.from + column), sequential: true }];
});

// Where a press on a drawn table puts the caret: at the start of the words of the cell pressed,
// in its row as typed, the row of dashes passed over.
export function cellAt(view, target) {
  const box = target.closest?.('.md-table-read');
  const cell = target.closest?.('th, td');
  if (!box) return null;
  const from = view.posAtDOM(box);
  if (!cell) return from;
  const row = cell.parentElement.rowIndex;
  const line = view.state.doc.lineAt(from).number + (row === 0 ? 0 : row + 1);
  if (line > view.state.doc.lines) return from;
  const { text, from: start } = view.state.doc.line(line);
  const pipes = [...text.matchAll(/(?<!\\)\|/g)].map((m) => m.index);
  const lead = /^\s*\|/.test(text) ? 1 : 0;
  const index = cell.cellIndex + lead - 1;
  let at = index < 0 ? 0 : (pipes[index] ?? text.length - 1) + 1;
  while (text[at] === ' ') at++;
  return start + Math.min(at, text.length);
}

// Everything a note needs to read where it is not written.
export const readWhereNotWritten = [read, tables, pictures, walkIn];

// Whether the open body has not been written in, nor its caret moved, since it opened.
export const arrivedAt = (view) => !!view.plugin(read)?.arrived;
