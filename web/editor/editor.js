// The body's editor: CodeMirror, with vim's keys when this browser has asked for them.
//
// It is a plain-text editor and nothing more. The page owns everything else: when a body opens
// and closes, what the engine is told, where the focus goes and the caret, which the page draws
// itself from the coordinates asked for here. So this is one view, made once and put in the
// page's sheet whenever a body opens, and a handful of plain functions the page calls. Its state
// is made afresh each time a body opens, so the step undo belongs to one visit; after that the
// engine's undo is the only one.
//
// The text is markdown, written as source: every mark stays where it was typed and in sight,
// only styled, so that nothing moves as the caret walks and vim's motions go over what is shown.
// The parser, the highlighting and the languages of code are the page's reading of markdown,
// imported from it, so that a body written and a body read look alike.
//
// Vim's Ex commands are its own, global to the module, so they reach whichever page mounted the
// view last. In vim a write stays and a quit goes; here a version is written when the body
// closes, so `:w` asks the page to close and open again, and the quits ask it to close. Vim's
// prompt for a command is a panel, which the editor would stick to the foot of whatever scrolls
// it, over the text; it goes in the element the page gives for it instead, under the text.

import { Annotation, Compartment, EditorSelection, EditorState, RangeSetBuilder, Transaction } from '@codemirror/state';
import { Decoration, EditorView, ViewPlugin, drawSelection, keymap, panels } from '@codemirror/view';
import { history, indentWithTab, insertNewline, redo, standardKeymap, undo } from '@codemirror/commands';
import {
  Language, ParseContext, StreamLanguage, defineLanguageFacet, syntaxHighlighting, syntaxTree,
} from '@codemirror/language';
import { parseCode } from '@lezer/markdown';
import { CodeMirror, Vim, getCM, vim } from '@replit/codemirror-vim';
import { between, follows, highlighter, language, markdown } from './render.js';

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

// A change the page makes is not one to tell the page about.
const fromPage = Annotation.define();

export function mount(hooks, foot) {
  current = hooks;
  const vimSlot = new Compartment();
  let vimWanted = false;
  const vimKeys = () => (vimWanted ? [vim({ status: false })] : []);
  const extensions = () => [
    vimSlot.of(vimKeys()),
    // Enter is a newline and no more, with or without Shift. Ctrl-O does nothing here, so that
    // it never reaches the browser's file dialog. Tab indents and Shift-Tab takes the indent back,
    // as in any editor, rather than moving the focus off the page's text: Escape, `:q` or the word
    // in the foot is the way out.
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
    syntaxHighlighting(highlighter),
    codeInMonospace,
    linksMarked,
    panels({ bottomContainer: foot }),
    drawSelection(),
    EditorView.lineWrapping,
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
      if (u.selectionSet || u.docChanged || u.focusChanged) hooks.onSelection();
    }),
    EditorView.domEventHandlers({ blur: (event) => hooks.onBlur(event), focus: () => hooks.onFocus() }),
  ];
  const view = new EditorView({ state: EditorState.create({ extensions: extensions() }) });

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
    open(text, at) {
      view.setState(
        EditorState.create({ doc: text, selection: EditorSelection.cursor(at), extensions: extensions() }),
      );
      listen();
    },
    // What the page puts in is not a step of the person's to undo.
    setText(text) {
      const head = Math.min(view.state.selection.main.head, text.length);
      view.dispatch({
        changes: { from: 0, to: view.state.doc.length, insert: text },
        selection: EditorSelection.cursor(head),
        annotations: [fromPage.of(true), Transaction.addToHistory.of(false)],
      });
    },
    text: () => view.state.doc.toString(),
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
    setVim(on) {
      vimWanted = on;
      view.dispatch({ effects: vimSlot.reconfigure(vimKeys()) });
      listen();
    },
    get vimOn() {
      return !!getCM(view);
    },
  };
}
