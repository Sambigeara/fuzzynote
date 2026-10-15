// The body's editor: CodeMirror, with vim's keys when this browser has asked for them.
//
// It is a plain-text editor and nothing more. The page owns everything else: when a body opens
// and closes, what the engine is told, where the focus goes and the caret, which the page draws
// itself from the coordinates asked for here. So this is one view, made once and put in the
// page's sheet whenever a body opens, and a handful of plain functions the page calls. Its state
// is made afresh each time a body opens, so the step undo belongs to one visit; after that the
// engine's undo is the only one.
//
// Vim's Ex commands are its own, global to the module, so they reach whichever page mounted the
// view last. In vim a write stays and a quit goes; here a version is written when the body
// closes, so `:w` asks the page to close and open again, and the quits ask it to close. Vim's
// prompt for a command is a panel, which the editor would stick to the foot of whatever scrolls
// it, over the text; it goes in the element the page gives for it instead, under the text.

import { Annotation, Compartment, EditorSelection, EditorState, Transaction } from '@codemirror/state';
import { EditorView, drawSelection, keymap, panels } from '@codemirror/view';
import { history, insertNewline, redo, standardKeymap, undo } from '@codemirror/commands';
import { Vim, getCM, vim } from '@replit/codemirror-vim';

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
    // it never reaches the browser's file dialog.
    keymap.of([
      { key: 'Enter', run: insertNewline, shift: insertNewline },
      { key: 'Ctrl-o', run: () => true },
      { key: 'Mod-z', run: undo },
      { key: 'Mod-y', mac: 'Mod-Shift-z', run: redo },
      { key: 'Mod-Shift-z', run: redo },
      ...standardKeymap,
    ]),
    history(),
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
