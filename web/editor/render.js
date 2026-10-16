// The page's reading of markdown: a body rendered where it is only read, a row's head read as a
// line of the same markdown, and the highlighting that the body's editor shares.
//
// The note stays plain text; this is how the page reads it. It is fetched as the page starts and
// never waited for, so it is kept small: Lezer's markdown parser with GitHub's tables, tasks,
// strikethrough and bare addresses, and the highlighter, which the editor imports from here so
// that what is written and what is read look alike. A body is built as nodes, never as markup,
// so nothing a note says becomes part of the page, and nothing in it reaches outside the page by
// being read: a link goes only to the web or to mail and opens apart from the page, and an image
// is a link reading its alt text, never loaded. What has no address the page will follow, raw
// HTML and `[[Boiler]]` among it, is shown as it was typed.
//
// Code is highlighted in its language, and each language is a file of its own under code/,
// fetched the first time a block in it is read or written. Until it arrives the block is plain,
// and the page is told when it does, to paint again.

import { GFM, parser } from '@lezer/markdown';
import { highlightTree, tagHighlighter, tags as t } from '@lezer/highlight';
import { StringStream } from '@codemirror/streamparser';

// `[[Boiler]]` is how a link to a note is typed, which markdown would read as a link with no
// address inside a pair of brackets. It is read whole, its brackets its marks.
const Bracketed = {
  defineNodes: [
    { name: 'Bracketed', style: t.link },
    { name: 'BracketMark', style: t.processingInstruction },
  ],
  parseInline: [
    {
      name: 'Bracketed',
      before: 'Link',
      parse(cx, next, pos) {
        const found = next === 91 && /^\[\[[^\]\n]+\]\]/.exec(cx.slice(pos, cx.end));
        if (!found) return -1;
        const end = pos + found[0].length;
        const marks = [cx.elt('BracketMark', pos, pos + 2), cx.elt('BracketMark', end - 2, end)];
        return cx.addElement(cx.elt('Bracketed', pos, end, marks));
      },
    },
  ],
};

export const markdown = parser.configure([GFM, Bracketed]);

// How each kind of text looks, in the page's greys and never a colour. Marks are faint, and so
// are code's punctuation and its comments, which slant too; what a string, a number or a constant
// says is soft; headings, strong text, keywords and what a line defines are heavier.
export const highlighter = tagHighlighter([
  {
    tag: [t.processingInstruction, t.contentSeparator, t.labelName, t.url, t.punctuation, t.meta, t.deleted],
    class: 'hl-faint',
  },
  { tag: t.comment, class: 'hl-faint hl-slant' },
  { tag: [t.literal, t.atom, t.null, t.unit], class: 'hl-soft' },
  { tag: [t.heading, t.strong, t.keyword, t.definition(t.name)], class: 'hl-heavy' },
  { tag: t.emphasis, class: 'hl-slant' },
  { tag: t.monospace, class: 'hl-mono' },
  { tag: t.strikethrough, class: 'hl-struck' },
]);

// The languages code is highlighted in, by the names a fence calls them. Each file under code/
// gives a Lezer parser or a stream mode of CodeMirror 5's kind.
const CODE = [
  [() => import('./code/shell.js'), 'shell', 'sh', 'bash', 'zsh', 'console'],
  [() => import('./code/javascript.js'), 'javascript', 'js', 'jsx', 'mjs', 'cjs'],
  [() => import('./code/typescript.js'), 'typescript', 'ts', 'tsx'],
  [() => import('./code/json.js'), 'json', 'jsonc'],
  [() => import('./code/python.js'), 'python', 'py'],
  [() => import('./code/rust.js'), 'rust', 'rs'],
  [() => import('./code/go.js'), 'go', 'golang'],
  [() => import('./code/sql.js'), 'sql'],
  [() => import('./code/yaml.js'), 'yaml', 'yml'],
  [() => import('./code/toml.js'), 'toml'],
  [() => import('./code/html.js'), 'html', 'htm'],
  [() => import('./code/css.js'), 'css'],
  [() => import('./code/diff.js'), 'diff', 'patch'],
  [() => import('./code/dockerfile.js'), 'dockerfile', 'docker'],
  [() => import('./code/cpp.js'), 'cpp', 'c++', 'c', 'cc', 'cxx', 'h', 'hpp'],
  [() => import('./code/java.js'), 'java'],
];
const loads = new Map(CODE.flatMap(([load, ...names]) => names.map((name) => [name, load])));
const languages = new Map();
let arrived = () => {};

// What the page does when a language arrives.
export function onLanguage(then) {
  arrived = then;
}

// The language a fence names by its first word, in any case: the parser or the stream mode once
// it is here, and until then the promise of it, its fetch begun. A language that could not be
// fetched is none.
export function language(info) {
  const load = loads.get(/^[^\s,{]*/.exec(info)[0].toLowerCase());
  if (!load) return null;
  if (!languages.has(load)) {
    const coming = load()
      .then((module) => module.default, () => null)
      .then((found) => {
        languages.set(load, found);
        if (found) arrived();
        return found;
      });
    languages.set(load, coming);
  }
  return languages.get(load);
}

// A row's head reads as one line of markdown, as a body's paragraph does, so that the same line
// reads one way beside the list and in it: emphasis, strong text, code, strikethrough, links and
// bare addresses rendered, a leading heading mark dropped, and whatever a body shows as typed shown
// as typed here too. A line that would be a block of another kind, a list item, a quote, a fence,
// stays as typed, since a head is not a block. A head with no character that can begin a mark is
// not parsed: most heads are plain, and a list paints hundreds of them when the line is cleared.
const MARKED = /[*_`~\\[<]|:\/\/|mailto:/i;
const HEADING = /^#{1,6}[ \t]+(?=\S)/;
export function head(text) {
  if (!MARKED.test(text)) return text.replace(HEADING, '');
  const block = markdown.parse(text).topNode.firstChild;
  if (!block) return text;
  const heading = /^ATXHeading\d$/.test(block.name);
  if (block.name !== 'Paragraph' && !heading) return text;
  const from = heading ? (block.getChild('HeaderMark')?.to ?? block.from) : block.from;
  const out = inline(block, from, block.to, text, document.createDocumentFragment());
  // The space after a heading's mark is text to the parser, and the row does not want it.
  if (heading && out.firstChild?.nodeType === Node.TEXT_NODE) out.firstChild.data = out.firstChild.data.replace(/^[ \t]+/, '');
  return out;
}

// A body as a document, each block the element it is, with its marks taken out and everything
// else where it was. A long one can be read only as far as the line past a point, for its top.
// One nested deeper than the stack reaches, thousands of quotes or lists deep, is read as typed.
export function body(text, upTo = text.length) {
  const parse = markdown.startParse(text);
  if (upTo < text.length) parse.stopAt(upTo);
  let tree;
  while (!(tree = parse.advance()));
  try {
    return blocks(tree.topNode, text, document.createDocumentFragment());
  } catch {
    return text;
  }
}

const el = (tag, cls) => {
  const node = document.createElement(tag);
  if (cls) node.className = cls;
  return node;
};

// A link's brackets are never read, since only what is inside them is, so they are not marks
// here, and the colon of a link's definition, which is one too, reads as typed.
const MARKS = new Set([
  'HeaderMark', 'QuoteMark', 'ListMark', 'EmphasisMark', 'CodeMark', 'CodeInfo', 'StrikethroughMark',
  'TableDelimiter', 'TaskMarker',
]);
const HOLDS_BLOCKS = {
  Blockquote: 'blockquote', BulletList: 'ul', OrderedList: 'ol', ListItem: 'li', Table: 'table',
};
const HOLDS_TEXT = {
  Paragraph: 'p', HTMLBlock: 'p', CommentBlock: 'p', ProcessingInstructionBlock: 'p',
  LinkReference: 'p', Emphasis: 'em', StrongEmphasis: 'strong', Strikethrough: 'del',
  InlineCode: 'code',
};

// What lies between the blocks in a container is its marks and the space after them.
function blocks(node, text, into) {
  for (let child = node.firstChild; child; child = child.nextSibling) {
    const made = read(child, text);
    if (made) into.append(made);
  }
  return into;
}

// Text is read as typed between the nodes inside it, from one point to another.
function inline(node, from, to, text, into) {
  let at = from;
  for (let child = node.firstChild; child; child = child.nextSibling) {
    if (child.from < from || child.to > to) continue;
    if (child.from > at) into.append(text.slice(at, child.from));
    const made = read(child, text);
    if (made) into.append(made);
    at = child.to;
  }
  if (to > at) into.append(text.slice(at, to));
  return into;
}

// What a node reads as: an element, text, or for a mark nothing.
function read(node, text) {
  const name = node.name;
  if (MARKS.has(name)) return null;
  if (name in HOLDS_BLOCKS) {
    const made = blocks(node, text, el(HOLDS_BLOCKS[name]));
    if (name === 'OrderedList') made.start = parseInt(text.slice(node.from, node.to), 10);
    // A table's columns are aligned as the row of dashes under its head says.
    const rule = name === 'Table' && node.getChild('TableDelimiter');
    if (rule) {
      const dashes = text.slice(rule.from, rule.to).replace(/^\s*\||\|\s*$/g, '').split('|');
      const align = dashes.map((cell) => (/-:\s*$/.test(cell) ? (/^\s*:/.test(cell) ? 'center' : 'right') : ''));
      for (const row of made.rows) {
        for (const cell of row.cells) if (align[cell.cellIndex]) cell.style.textAlign = align[cell.cellIndex];
      }
    }
    return made;
  }
  if (name in HOLDS_TEXT) return inline(node, node.from, node.to, text, el(HOLDS_TEXT[name]));
  const heading = /Heading(\d)$/.exec(name);
  if (heading) return inline(node, node.from, node.to, text, el(`h${heading[1]}`));
  switch (name) {
    // A cell left empty has no node of its own, only a delimiter straight after another.
    case 'TableHeader':
    case 'TableRow': {
      const row = el('tr');
      const cell = name === 'TableHeader' ? 'th' : 'td';
      for (let child = node.firstChild; child; child = child.nextSibling) {
        if (child.name === 'TableCell') row.append(inline(child, child.from, child.to, text, el(cell)));
        else if (child.prevSibling?.name === 'TableDelimiter') row.append(el(cell));
      }
      return row;
    }
    // A task's box is drawn, ticked or not, and nothing toggles it.
    case 'Task': {
      const marker = node.firstChild;
      const task = el('p', /x/i.test(text.slice(marker.from, marker.to)) ? 'task done' : 'task');
      task.append(el('span', 'check'));
      return inline(node, marker.to, node.to, text, task);
    }
    case 'FencedCode':
    case 'CodeBlock': {
      const info = node.getChild('CodeInfo');
      const code = node.getChildren('CodeText').map((part) => text.slice(part.from, part.to)).join('');
      const found = info && language(text.slice(info.from, info.to));
      const pre = el('pre');
      highlight(code, found instanceof Promise ? null : found, pre.appendChild(el('code')));
      return pre;
    }
    case 'HorizontalRule':
      return el('hr');
    case 'HardBreak':
      return '\n';
    case 'Escape':
      return text.slice(node.from + 1, node.to);
    case 'Autolink':
      return read(node.getChild('URL'), text);
    case 'URL':
      return anchor(node, text) ?? text.slice(node.from, node.to);
    // A link holds no link: an address or an image in its text reads as its text.
    case 'Link':
    case 'Image': {
      const a = anchor(node, text);
      if (!a) return text.slice(node.from, node.to);
      const [from, to] = between(node);
      if (to > from) a.replaceChildren(inline(node, from, to, text, document.createDocumentFragment()));
      for (const inner of a.querySelectorAll('a')) inner.replaceWith(...inner.childNodes);
      return a;
    }
    default:
      return text.slice(node.from, node.to);
  }
}

// A link to the address a node gives, reading as the address, if it is one the page will follow:
// a whole address on the web, a host after `//`, or one to mail. `http:boiler` is not one, and
// a browser would take it as a path on the relay. A link's address is the one after its text, which may hold one too. An
// address read as itself is shown without its scheme, as an address bar shows one, since the
// scheme is the part nobody reads; the whole of it is there when the pointer rests on it.
function anchor(node, text) {
  let url = node;
  if (node.name !== 'URL') {
    const [, end] = between(node);
    url = node.getChildren('URL').find((after) => after.from > end);
  }
  const address = url ? text.slice(url.from, url.to) : '';
  if (!/^(?:https?:\/\/[^/?#\s]|mailto:\S)/i.test(address)) return null;
  const a = el('a');
  a.href = address;
  a.target = '_blank';
  a.rel = 'noopener noreferrer';
  a.tabIndex = -1;
  a.title = address;
  a.textContent = address.replace(/^(https?:\/\/|mailto:)/i, '');
  return a;
}

// Where a link's text, or an image's alt text, lies: between its brackets.
const between = (node) => {
  const [open, close] = node.getChildren('LinkMark');
  return [open.to, close.from];
};

// Code in a language that is here has its kinds of text in the highlighter's classes; in one that
// is not, or none, it is as typed.
function highlight(code, found, into) {
  let at = 0;
  const put = (from, to, classes) => {
    const span = el('span', classes);
    span.textContent = code.slice(from, to);
    if (from > at) into.append(code.slice(at, from));
    into.append(span);
    at = to;
  };
  if (found?.token) stream(found, code, put);
  else if (found) highlightTree(found.parse(code), highlighter, put);
  if (code.length > at) into.append(code.slice(at));
}

// A stream mode is run as CodeMirror runs one, a line at a time and a token at a time, and the
// names it gives its tokens are read as the tags CodeMirror reads them as.
function stream(mode, code, put) {
  const state = mode.startState ? mode.startState(2) : true;
  let at = 0;
  for (const line of code.split('\n')) {
    const tokens = new StringStream(line, 4, 2);
    if (!line) mode.blankLine?.(state, 2);
    while (!tokens.eol()) {
      tokens.start = tokens.pos;
      let style = null;
      for (let tries = 0; tries < 10 && tokens.pos === tokens.start; tries++) style = mode.token(tokens, state);
      if (tokens.pos === tokens.start) break;
      const classes = style && classesOf(style);
      if (classes) put(at + tokens.start, at + tokens.pos, classes);
    }
    at += line.length + 1;
  }
}

const LEGACY = {
  variable: 'variableName', 'variable-2': 'variableName.special', 'string-2': 'string.special',
  def: 'variableName.definition', tag: 'tagName', attribute: 'attributeName', type: 'typeName',
  builtin: 'variableName.standard', qualifier: 'modifier', error: 'invalid', header: 'heading',
  property: 'propertyName',
};
const styled = new Map();
function classesOf(style) {
  if (!styled.has(style)) {
    const found = style.split(' ').flatMap((name) => {
      const [base, ...modifiers] = (LEGACY[name] ?? name).split('.');
      const tag = t[base];
      return tag && typeof tag !== 'function' ? [modifiers.reduce((tag, modifier) => t[modifier](tag), tag)] : [];
    });
    styled.set(style, highlighter.style(found));
  }
  return styled.get(style);
}
