// The note's editor: CodeMirror, holding the head of the row under the cursor, or the whole note
// while its body is open, with vim's keys there when this browser has asked for them.
//
// It is a plain-text editor and nothing more. The page owns everything else: where the editor is,
// what the engine is told, which key asks the session for what, where the focus goes and the
// caret, which the page draws itself from the coordinates asked for here. So this is one view,
// made once and moved, as the cursor moves, into the row it lands on and into the page's sheet
// whenever a body opens, and a handful of plain functions the page calls. Its state is made afresh
// each time it lands or a body opens or closes. A head is one line, with no undo of its own, since
// the engine's undo puts a head back; a body has step undo for the one visit, and after that the
// engine's undo is the only one.
//
// The text is markdown, kept as typed. A head reads as its row reads off the cursor and a body as
// the body beside the list reads, and in either the span the selection touches shows its marks as
// typed, styled (see reading.js). The parsers, the highlighting and the languages of code are the
// page's reading of markdown, imported from it, so that a note written and a note read look alike.
//
// Vim's Ex commands are its own, global to the module, so they reach whichever page mounted the
// view last. In vim a write stays and a quit goes; here a version is written when the body
// closes, so `:w` asks the page to close and open again, and the quits ask it to close. Vim's
// prompt for a command is a panel, which the editor would stick to the foot of whatever scrolls
// it, over the text; it goes in the element the page gives for it instead, under the text.

import { Compartment, EditorSelection, EditorState, RangeSetBuilder, Transaction } from '@codemirror/state';
import { Decoration, EditorView, ViewPlugin, drawSelection, keymap, panels } from '@codemirror/view';
import { history, indentWithTab, insertNewline, redo, standardKeymap, undo } from '@codemirror/commands';
import {
  Language, ParseContext, StreamLanguage, defineLanguageFacet, syntaxHighlighting, syntaxTree,
} from '@codemirror/language';
import { parseCode } from '@lezer/markdown';
import { CodeMirror, Vim, getCM, vim } from '@replit/codemirror-vim';
import { between, follows, highlighter, language, line, markdown } from './render.js';
import { asHead, fromPage, readEnd, readWhereNotWritten, readingMoved, writtenIn } from './reading.js';

// A block of code is parsed in its language: by its grammar, or a stream mode made a parser here,
// and while the language is on its way it is passed over, to be parsed again when it arrives.
const streams = new WeakMap();
const codeParser = (info) => {
  const found = language(info);
  if (found instanceof Promise) return ParseContext.getSkippingParser(found);
  if (!found?.token) return found;
  if (!streams.has(found)) streams.set(found, StreamLanguage.define(found).parser);
  return streams.get(found);
};
const note = new Language(defineLanguageFacet(), markdown.configure(parseCode({ codeParser })), [], 'markdown');
// A head is read as a line alone, as its row reads it.
const headLine = new Language(defineLanguageFacet(), line, [], 'markdown');

// A head is one line: a newline pasted, dropped or dictated into it becomes a space rather than a
// body nobody sees, and the caret goes after what came in.
const oneLine = EditorState.transactionFilter.of((tr) => {
  if (!tr.docChanged || tr.newDoc.lines === 1) return tr;
  const changes = [];
  tr.changes.iterChanges((from, to, _from, _to, inserted) =>
    changes.push({ from, to, insert: inserted.toString().replace(/\n+/g, ' ') }),
  );
  const set = tr.startState.changes(changes);
  return {
    changes: set,
    selection: tr.startState.selection.map(set, 1),
    scrollIntoView: true,
    userEvent: tr.annotation(Transaction.userEvent),
  };
});

// A block of code is set in the monospace a line at a time, fences and all, as far as the view
// reaches. The highlighting cannot give it the face, since a language's own starts afresh inside
// the block.
const codeLine = Decoration.line({ class: 'hl-code' });
const codeLines = (view) => {
  const lines = new RangeSetBuilder();
  const { doc } = view.state;
  syntaxTree(view.state).iterate({
    ...view.viewport,
    enter(node) {
      if (node.name !== 'FencedCode' && node.name !== 'CodeBlock') return;
      for (let line = doc.lineAt(node.from); ; line = doc.line(line.number + 1)) {
        lines.add(line.from, line.from, codeLine);
        if (line.to >= node.to) return false;
      }
    },
  });
  return lines.finish();
};
const codeInMonospace = ViewPlugin.define(
  (view) => ({
    lines: codeLines(view),
    update(u) {
      if (u.docChanged || u.viewportChanged || syntaxTree(u.startState) !== syntaxTree(u.state)) {
        this.lines = codeLines(u.view);
      }
    },
  }),
  { decorations: (plugin) => plugin.lines },
);

// What the read makes a link is marked as one, as far as the view reaches: a link's or an image's
// words when the address after them is one the page follows, and an address so followed standing
// alone, bare, in angle brackets or in a link's definition. The highlighting cannot tell, since
// markdown takes any words in brackets for a link, and `[draft]` or `www.example.org` would be
// marked as links that lead nowhere. A link's brackets and its address stay faint, as marks.
const linkMark = Decoration.mark({ class: 'hl-link' });
const linkMarks = (view) => {
  const marks = new RangeSetBuilder();
  const { doc } = view.state;
  syntaxTree(view.state).iterate({
    ...view.viewport,
    enter(ref) {
      if (ref.name === 'Link' || ref.name === 'Image') {
        const node = ref.node;
        const [from, to] = between(node);
        const url = node.getChildren('URL').find((after) => after.from > to);
        const followed = url && follows(doc.sliceString(url.from, url.to));
        if (followed && to > from) marks.add(from, to, linkMark);
        return false;
      }
      if (ref.name !== 'URL' || !follows(doc.sliceString(ref.from, ref.to))) return;
      marks.add(ref.from, ref.to, linkMark);
    },
  });
  return marks.finish();
};
const linksMarked = ViewPlugin.define(
  (view) => ({
    marks: linkMarks(view),
    update(u) {
      if (u.docChanged || u.viewportChanged || syntaxTree(u.startState) !== syntaxTree(u.state)) {
        this.marks = linkMarks(u.view);
      }
    },
  }),
  { decorations: (plugin) => plugin.marks },
);

// What a press at a position in a body follows, if it lands on a link read rather than written
// in: an address the page follows, as `{ url }`, or a note's name in brackets, as `{ name }`. A link
// whose marks show is being written in, and a press there is the caret's.
function linkAt(view, pos) {
  const { state } = view;
  for (const side of [1, -1]) {
    for (let node = syntaxTree(state).resolveInner(pos, side); node; node = node.parent) {
      const text = (n) => state.sliceDoc(n.from, n.to);
      let found = null;
      if (node.name === 'Bracketed') found = { name: state.sliceDoc(node.from + 2, node.to - 2) };
      else if (node.name === 'Link' || node.name === 'Image') {
        const url = node.getChildren('URL').find((u) => u.from > between(node)[1]);
        if (url && follows(text(url))) found = { url: text(url) };
      } else if (node.name === 'URL' && follows(text(node))) found = { url: text(node) };
      if (found) return writtenIn(view, node.from, node.to) ? null : found;
    }
  }
  return null;
}

let current = null;

// `:q!` reaches `q` with the bang left over as its argument.
const bang = (params) => params?.argString?.trim() === '!';
Vim.defineEx('write', 'w', () => current?.onEx('write'));
Vim.defineEx('quit', 'q', (cm, params) => current?.onEx(bang(params) ? 'discard' : 'quit'));
Vim.defineEx('wq', 'wq', () => current?.onEx('quit'));
Vim.defineEx('xit', 'x', () => current?.onEx('quit'));
// ZZ and ZQ are actions of their own rather than keys mapped onto the Ex commands, so that they
// run at once and the prompt never shows for them.
Vim.defineAction('fznQuit', () => current?.onEx('quit'));
Vim.defineAction('fznDiscard', () => current?.onEx('discard'));
Vim.mapCommand('ZZ', 'action', 'fznQuit', {}, { context: 'normal' });
Vim.mapCommand('ZQ', 'action', 'fznDiscard', {}, { context: 'normal' });
// j and k go by the lines as shown, since a body wraps and a paragraph is one line to vim. They
// are a motion of the page's own rather than gj and gk, because a line-wise selection is made of
// vim's lines: there they step by those, keeping the column as vim's j does, and V3jd takes four
// whole lines. Otherwise they step as gj does, by the lines as laid out, held to the column on
// screen where the steps began, and stop at the first or last. The motion is mapped in normal and
// visual mode only, so that after an operator j and k keep their own line-wise motion and dj still
// takes two whole lines. A count typed before them is the motion's repeat.
const shown = (cm, head, args, state) => {
  const steady = state.lastMotion === shown;
  if (state.visualLine) {
    const ch = steady ? state.lastHPos : head.ch;
    state.lastHPos = ch;
    const line = head.line + (args.forward ? args.repeat : -args.repeat);
    const at = new CodeMirror.Pos(Math.max(cm.firstLine(), Math.min(cm.lastLine(), line)), ch);
    state.lastHSPos = cm.charCoords(at, 'div').left;
    return at;
  }
  if (!steady) state.lastHSPos = cm.charCoords(head, 'div').left;
  let at = head;
  for (let i = 0; i < args.repeat; i++) {
    const next = cm.findPosV(at, args.forward ? 1 : -1, 'line', state.lastHSPos);
    if (next.hitSide) break;
    at = next;
  }
  if (at !== head) state.lastHPos = at.ch;
  return at;
};
Vim.defineMotion('fznShown', shown);
for (const context of ['normal', 'visual']) {
  Vim.mapCommand('j', 'motion', 'fznShown', { forward: true }, { context });
  Vim.mapCommand('k', 'motion', 'fznShown', { forward: false }, { context });
}

export function mount(hooks, foot) {
  current = hooks;
  const vimSlot = new Compartment();
  let vimWanted = false;
  let whole = false;
  const vimKeys = () => (vimWanted ? [vim({ status: false })] : []);
  // What a head and a body share: the reading, the highlighting, the links marked, the grey
  // selection under the page's caret, and what the page is told.
  const shared = [
    syntaxHighlighting(highlighter),
    codeInMonospace,
    linksMarked,
    readWhereNotWritten,
    drawSelection(),
    EditorView.contentAttributes.of({
      autocapitalize: 'sentences',
      autocorrect: 'on',
      spellcheck: 'false',
      enterkeyhint: 'enter',
      'aria-label': 'The note under the cursor',
    }),
    EditorView.updateListener.of((u) => {
      const typed = u.docChanged && !u.transactions.some((tr) => tr.annotation(fromPage));
      if (typed) hooks.onChange(u.state.doc.toString());
      if (u.selectionSet || u.docChanged || u.focusChanged || readingMoved(u)) hooks.onSelection();
    }),
    EditorView.domEventHandlers({ blur: (event) => hooks.onBlur(event), focus: () => hooks.onFocus() }),
  ];
  // A body wraps, and its keys are an editor's: Enter is a newline and no more, with or without
  // Shift. Ctrl-O does nothing here, so that it never reaches the browser's file dialog. Tab
  // indents and Shift-Tab takes the indent back, as in any editor, rather than moving the focus off
  // the page's text: Escape, `:q` or the word in the foot is the way out. A press on a link read
  // follows it, and the caret stays where it was.
  const body = () => [
    EditorView.domEventHandlers({
      mousedown(event, view) {
        if (event.button !== 0) return false;
        const at = view.posAtCoords({ x: event.clientX, y: event.clientY });
        const link = at == null ? null : linkAt(view, at);
        if (!link) return false;
        event.preventDefault();
        hooks.onFollow(link);
        return true;
      },
    }),
    vimSlot.of(vimKeys()),
    keymap.of([
      { key: 'Enter', run: insertNewline, shift: insertNewline },
      { key: 'Ctrl-o', run: () => true },
      indentWithTab,
      { key: 'Mod-z', run: undo },
      { key: 'Mod-y', mac: 'Mod-Shift-z', run: redo },
      { key: 'Mod-Shift-z', run: redo },
      ...standardKeymap,
    ]),
    history(),
    note,
    panels({ bottomContainer: foot }),
    EditorView.lineWrapping,
    ...shared,
  ];
  // A head is one line, which scrolls sideways under the caret keeping five letters beside it. Its
  // keys are the page's but for those that write in it or move its caret, which are an editor's,
  // and it has no undo of its own, since the engine's undo puts a head back.
  const head = (hidden, margin) => [
    keymap.of(standardKeymap),
    headLine,
    oneLine,
    asHead.of({ hidden }),
    EditorView.cursorScrollMargin.of({ x: margin, y: 5 }),
    EditorView.contentAttributes.of({ 'aria-multiline': 'false' }),
    ...shared,
  ];
  const view = new EditorView();

  // The vim adapter is made again whenever its plugin is, on every new state, and the mode is
  // heard from whichever adapter there is now.
  const heard = new WeakSet();
  const listen = () => {
    const cm = getCM(view);
    if (!cm || heard.has(cm)) return;
    heard.add(cm);
    cm.on('vim-mode-change', () => hooks.onMode(mode()));
  };
  const mode = () => {
    const state = getCM(view)?.state.vim;
    if (!state) return null;
    return state.insertMode ? 'insert' : state.visualMode ? 'visual' : 'normal';
  };

  return {
    dom: view.dom,
    contentDOM: view.contentDOM,
    scrollDOM: view.scrollDOM,
    // A head, given the place's labels its row reads without, with the caret at `at` or where what
    // it reads ends, whichever is sooner. It is opened where it is to be shown, whose letters say
    // how wide five of them are.
    openHead(text, at, hidden = '') {
      whole = false;
      const margin = 2.5 * (parseFloat(getComputedStyle(view.contentDOM).fontSize) || 16);
      const state = EditorState.create({ doc: text, extensions: head(hidden, margin) });
      view.setState(state.update({ selection: EditorSelection.cursor(Math.min(at, readEnd(state))) }).state);
    },
    // The whole note, with the caret at `at`.
    openBody(text, at) {
      whole = true;
      view.setState(EditorState.create({ doc: text, selection: EditorSelection.cursor(at), extensions: body() }));
      listen();
    },
    get whole() {
      return whole;
    },
    // What the page puts in is not a step of the person's to undo. The caret stays where it was,
    // as far as the text reaches, or goes where the page puts it, in a head no further than what
    // the head reads.
    setText(text, at = null) {
      const changes = view.state.doc.toString() === text ? [] : { from: 0, to: view.state.doc.length, insert: text };
      const end = at == null ? text.length : readEnd(view.state.update({ changes }).state);
      view.dispatch({
        changes,
        selection: EditorSelection.cursor(Math.min(at ?? view.state.selection.main.head, end)),
        annotations: [fromPage.of(true), Transaction.addToHistory.of(false)],
      });
    },
    // Puts back a selection the page kept, which is the person's, so that the span it touches
    // shows its marks as it did before the page took it away.
    select({ anchor, head }) {
      const end = view.state.doc.length;
      view.dispatch({ selection: EditorSelection.single(Math.min(anchor, end), Math.min(head, end)) });
    },
    // Puts the caret where a press landed, as a press in the editor would.
    placeAt(x, y) {
      const at = view.posAtCoords({ x, y });
      if (at != null) view.dispatch({ selection: EditorSelection.cursor(at), userEvent: 'select.pointer' });
    },
    text: () => view.state.doc.toString(),
    // A head as its row reads it, without the place's labels while they end it.
    reads: () => view.state.doc.sliceString(0, readEnd(view.state)),
    selection: () => ({ anchor: view.state.selection.main.anchor, head: view.state.selection.main.head }),
    focus: () => view.focus(),
    get hasFocus() {
      return view.hasFocus;
    },
    get composing() {
      return view.composing;
    },
    get mode() {
      return mode();
    },
    // Where the caret is, for the page to draw: the position's own rectangle, on the side of the
    // soft wrap the selection keeps to, and the right edge of the letter after it when that
    // letter is on the same line.
    caret() {
      const { empty, head, assoc } = view.state.selection.main;
      const rect = view.coordsAtPos(head, assoc || 1);
      let right = null;
      if (rect && head < view.state.doc.length && view.state.sliceDoc(head, head + 1) !== '\n') {
        const after = view.coordsAtPos(head + 1, -1);
        if (after && after.top === rect.top) right = after;
      }
      return { empty, rect, right, mode: mode() };
    },
    // Where a position is, for what is offered under a word.
    coordsAt: (pos) => view.coordsAtPos(pos, 1),
    setVim(on) {
      vimWanted = on;
      if (whole) view.dispatch({ effects: vimSlot.reconfigure(vimKeys()) });
      listen();
    },
    get vimOn() {
      return !!getCM(view);
    },
  };
}
