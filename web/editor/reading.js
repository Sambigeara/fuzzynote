// The open body read wherever it is not being written: a mark shows only in the span the selection
// touches.
//
// Every line reads as the body beside the list reads, its marks taken out, a quote ruled, a bullet
// and a task's box drawn, a rule drawn and a fence's backticks gone from its band, and a span
// shows its marks as typed while the selection touches it, at either edge, by any of its ranges:
// emphasis, strong text, strikethrough, code, a link with its address, an address, an escape. A
// heading's mark, a quote's on a line and a rule show while the caret is on their line; a list
// item's lead, its indent, its mark and a task's box, is one span, so that a caret at the start of
// an item shows where a letter will go; a fence shows its backticks while the caret is anywhere in
// its block. Nothing else decides it: a span is shown however the caret came to it and read again
// once it has left, as SilverBullet and Obsidian show theirs. Since a hidden mark always lies
// inside its span, a caret at either side of one touches the span and shows it, so no caret ever
// stops beside a mark it cannot see.
//
// Nothing here changes the text. Decorations hide marks and draw in their place, so the note, its
// undo, a copy and a paste are the source. Only marks are hidden, never words, and each only within
// its own line, since a decoration given by a plugin may not cover a line break. A line read is as
// high as written but for what its marks take once wrapped, a heading's size being its line's and a
// fence's line its band. Tables and what a block of code holds stay as they are styled, being read
// best as typed, and raw HTML, a link's definition and `[[Boiler]]` stay as typed, as they are
// read. A box drawn here is not pressed: a press on it puts the caret beside it, as a press
// anywhere does.
//
// While a composition is under way the marks hold still, moved only with the text, so that no mark
// opens or closes beside what an input method is writing; they are read again once it has ended.
// So they do while a press is held, and the span it lands in opens once it is let go.

import { RangeSet } from '@codemirror/state';
import { Decoration, ViewPlugin, WidgetType } from '@codemirror/view';
import { syntaxTree } from '@codemirror/language';
import { between, follows } from './render.js';

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
const headingLines = [1, 2, 3, 4, 5, 6].map((n) => Decoration.line({ class: `md-h${n}` }));
const SCHEME = /^(?:https?:\/\/|mailto:)/i;
// The marks a span of text holds, by the kind of span.
const MARK = {
  Emphasis: 'EmphasisMark',
  StrongEmphasis: 'EmphasisMark',
  Strikethrough: 'StrikethroughMark',
  InlineCode: 'CodeMark',
};

// What reads, as far as the view reaches: every mark of a span the selection does not touch,
// hidden or drawn.
function reading(view) {
  const { state } = view;
  const { doc, selection } = state;
  const touched = (from, to) => selection.ranges.some((r) => r.from <= to && r.to >= from);
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
  for (const { from, to } of view.visibleRanges) {
    syntaxTree(state).iterate({
      from,
      to,
      enter(ref) {
        const name = ref.name;
        const node = ref.node;
        // A heading's mark goes with the space after it, and a closing one alone, unless the caret
        // is on its line; what is marked in its words is a span of its own. Its size is its line's,
        // given whether or not the mark shows, so that showing the mark changes no height.
        if (/^ATXHeading\d$/.test(name)) {
          mark(ref.from, headingLines[Number(name.slice(-1)) - 1]);
          if (touchedLine(ref.from)) return;
          const [first, ...rest] = node.getChildren('HeaderMark');
          if (first) {
            let end = first.to;
            while (end < ref.to && /[ \t]/.test(doc.sliceString(end, end + 1))) end++;
            hide(first.from, end);
          }
          for (const closing of rest) hide(closing.from, closing.to);
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
          // A quote is ruled down its lines, once however deep, but on the caret's line, where its
          // marks show instead.
          case 'Blockquote': {
            if (node.parent?.name === 'Blockquote') return;
            for (let line = doc.lineAt(ref.from); ; line = doc.line(line.number + 1)) {
              if (!touched(line.from, line.to)) mark(line.from, quoteLine);
              if (line.to >= ref.to || line.number === doc.lines) break;
            }
            return;
          }
          case 'QuoteMark': {
            if (touchedLine(ref.from)) return;
            const end = doc.sliceString(ref.to, ref.to + 1) === ' ' ? ref.to + 1 : ref.to;
            return hide(ref.from, end);
          }
          // A list item's lead is one span, from the start of its line, its indent counting, to
          // the space after its mark or its task's box: a bullet reads as a dot, a task as its
          // box, ticked and faint when done, and a number as typed.
          case 'ListMark': {
            const item = node.parent;
            const marker = item?.getChild('Task')?.getChild('TaskMarker');
            const done = !!marker && /x/i.test(doc.sliceString(marker.from, marker.to));
            if (done) mark(ref.from, doneLine);
            let end = (marker ?? node).to;
            if (doc.sliceString(end, end + 1) === ' ') end++;
            if (touched(doc.lineAt(ref.from).from, end)) return;
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
  return Decoration.set(out, true);
}

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
const read = ViewPlugin.fromClass(
  class {
    constructor(view) {
      this.decorations = reading(view);
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
      if ((u.view.composing || this.pressed) && !u.transactions.some(replacesAll)) {
        this.decorations = before.map(u.changes);
        this.held = true;
      } else if (
        this.held ||
        u.docChanged ||
        u.selectionSet ||
        u.viewportChanged ||
        syntaxTree(u.startState) !== syntaxTree(u.state)
      ) {
        this.decorations = reading(u.view);
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

// Whether an update opened or closed a mark, which moves the caret's letters.
export const readingMoved = (update) => !!update.view.plugin(read)?.moved;

// Everything the body needs to read where it is not written.
export const readWhereNotWritten = [read];
