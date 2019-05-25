import curses
import os
from enum import Enum
from xml.etree import ElementTree as ET

os.environ.setdefault('ESCDELAY', '25')

FILE_LOCATION = 'sample.opml'


class InputMode(Enum):
    NORMAL = "normal"
    INSERT = "insert"


INPUT_MODE = InputMode.NORMAL


# def cust_addstr(y_pos, x_pos, stdscr):
    # def func(string):
        # stdscr.addstr(y_pos, x_pos, string)
    # return func


# class Node:
    # """Node representing a single text object in the document"""

    # __slots__ = ("is_visible", "tag", "text", "parent",)

    # def __init__(self, is_visible, tag, text, parent=None):
        # self.is_visible = is_visible
        # self.tag = tag
        # self.text = text
        # self.parent = parent

    # def __repr__(self):
        # return self.text

    # def __iter__(self):
        # for i in self.children:


def debug(stdscr):
    curses.nocbreak()
    stdscr.keypad(0)
    curses.echo()
    curses.endwin()
    import pdb; pdb.set_trace()


class View:
    """Represents the data that should visually populate the window at any one time"""

    def __init__(self, stdscr, top_offset=1, top_ptr=0):
        self.window = stdscr
        self.top_offset = top_offset

        self.height = None
        self.width = None

        # top_ptr represents the current highest tree element displayed in the top row
        self.top_ptr = top_ptr

        parsed = ET.parse(FILE_LOCATION)
        self.root = parsed.getroot()[1]
        # self.root = Node(is_visible=True, tag=root_xml.tag, text=root_xml.text)
        self.document = self.generate_document(self.root)

    def generate_item(self, node, arr, n_indent=0):
        for child in node:
            # if child.is_visible:
            if not child.attrib.get("_complete"):
                arr.append((n_indent, child.attrib['text']))
                self.generate_item(child, arr, n_indent+1)

    def generate_document(self, root):
        """Generates overall text blob representing entire document in list of list form"""
        arr = []
        self.generate_item(root, arr)
        return arr

    def paint(self):
        """Paint the required items based on current position and window size"""
        # stdscr.addstr(0, 0, "Current mode: Typing mode", curses.A_REVERSE)
        window_line = self.top_offset
        while window_line < self.height:
            n_indent, text = self.document[self.top_ptr + window_line - 1]
            margin_spacer = 2
            bullet = "- "
            left_margin = len(margin_spacer*n_indent*" ")
            line_length = self.width - left_margin
            text = f"{bullet}{text}"
            remaining_text = text
            # Need to account for long lines, so iterate over text and increment window_line
            # TODO fix nested while conditional duplication
            while len(remaining_text) and window_line < self.height:
                self.window.addstr(window_line, left_margin, remaining_text[:line_length], curses.color_pair(2))
                remaining_text = remaining_text[line_length:-2]
                if remaining_text:
                    remaining_text = bullet.replace("-", " ") + remaining_text
                window_line += 1

    def scroll_up(self):
        self.top_ptr -= 1
        self.top_ptr = max(0, self.top_ptr)

    def scroll_down(self):
        self.top_ptr += 1
        self.top_ptr = min(len(self.document) - self.height + 1, self.top_ptr)


def main(stdscr):
    global INPUT_MODE
    k = 0
    cursor_x = 0
    cursor_y = 1

    stdscr.clear()
    stdscr.refresh()

    # Start colors in curses
    curses.init_pair(1, curses.COLOR_CYAN, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_WHITE, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_BLACK, curses.COLOR_WHITE)

    top_offset = 1
    view = View(stdscr, top_offset=top_offset)

    # Loop where k is the last character pressed
    while (k != ord('q')):

        # Initialization
        stdscr.erase()

        height, width = stdscr.getmaxyx()
        view.height = height - top_offset
        view.width = width

        if INPUT_MODE == InputMode.INSERT:
            if k == 27:
                stdscr.timeout(10)
                next_k = stdscr.getch()
                if next_k == curses.ERR:
                    INPUT_MODE = InputMode.NORMAL
                stdscr.timeout(1000)
        elif INPUT_MODE == InputMode.NORMAL and k == ord('i'):
            INPUT_MODE = InputMode.INSERT

        if INPUT_MODE == InputMode.NORMAL:
            if k == ord("j"):
                cursor_y = cursor_y + 1
            elif k == ord("k"):
                cursor_y = cursor_y - 1
            elif k == ord("l"):
                cursor_x = cursor_x + 1
            elif k == ord("h"):
                cursor_x = cursor_x - 1

        if k == curses.KEY_DOWN:
            cursor_y = cursor_y + 1
        elif k == curses.KEY_UP:
            cursor_y = cursor_y - 1
        elif k == curses.KEY_RIGHT:
            cursor_x = cursor_x + 1
        elif k == curses.KEY_LEFT:
            cursor_x = cursor_x - 1

        cursor_x = max(0, cursor_x)
        cursor_x = min(view.width-1, cursor_x)

        if cursor_y < top_offset:  # < 1 to deal with top margin
            view.scroll_up()
        if cursor_y == view.height:
            view.scroll_down()

        cursor_y = max(top_offset, cursor_y)
        cursor_y = min(view.height-1, cursor_y)

        # Rendering some text
        # stdscr.addstr(0, 0, f"Current mode: {INPUT_MODE.value} mode", curses.A_REVERSE)
        # whstr = "Width: {}, Height: {}".format(view.width, view.height)  # TODO remove
        # stdscr.addstr(1, 0, whstr, curses.color_pair(1))  # TODO remove

        stdscr.addstr(0, 0, f"Current mode: {INPUT_MODE.value} mode", curses.A_REVERSE)

        view.paint()

        # Move cursor
        stdscr.move(cursor_y, cursor_x)

        # Refresh the screen
        stdscr.refresh()

        # Wait for next input
        k = stdscr.getch()


if __name__ == '__main__':
    os.environ.setdefault('ESCDELAY', '25')
    curses.wrapper(main)
