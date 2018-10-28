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


class View:
    """Represents the data that should visually populate the window at any one time"""

    def __init__(self, stdscr, top_offset=1, bottom_offset=1, top_ptr=0):
        self.window = stdscr
        self.top_offset = top_offset
        self.bottom_offset = bottom_offset

        self.height = None
        self.width = None

        # top_ptr represents the current highest tree element displayed in the top row
        self.top_ptr = top_ptr

        parsed = ET.parse(FILE_LOCATION)
        self.root = parsed.getroot()[1]
        # self.root = Node(is_visible=True, tag=root_xml.tag, text=root_xml.text)
        self.document = self.generate_document(self.root)

    def generate_item(self, node, arr, sublevel=0):
        spacer = sublevel * "  "
        sublevel += 1
        for child in node:
            # if child.is_visible:
            if not child.attrib.get("_complete"):
                arr.append(f"{spacer}- {child.attrib['text']}")
                self.generate_item(child, arr, sublevel)

    def generate_document(self, root):
        """Generates overall text blob representing entire document in list of list form"""
        arr = []
        self.generate_item(root, arr)
        return arr

    def paint(self):
        """Paint the required items based on current position and window size"""
        # stdscr.addstr(0, 0, "Current mode: Typing mode", curses.A_REVERSE)
        i = self.top_offset
        while i < self.height - self.bottom_offset:
            self.window.addstr(i, 0, self.document[i-1], curses.color_pair(2))
            i += 1

    def refresh_window_size(self):
        raise NotImplementedError


def main(stdscr):
    global INPUT_MODE
    k = 0
    cursor_x = 0
    cursor_y = 2

    stdscr.clear()
    stdscr.refresh()

    # Start colors in curses
    curses.init_pair(1, curses.COLOR_CYAN, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_WHITE, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_BLACK, curses.COLOR_WHITE)

    view = View(stdscr)

    # Loop where k is the last character pressed
    while (k != ord('q')):

        # Initialization
        stdscr.erase()
        view.height, view.width = stdscr.getmaxyx()

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
        elif INPUT_MODE == InputMode.INSERT:
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

        cursor_y = max(0, cursor_y)
        cursor_y = min(view.height-1, cursor_y)

        # Rendering some text
        stdscr.addstr(0, 0, f"Current mode: {INPUT_MODE.value} mode", curses.A_REVERSE)
        # whstr = "Width: {}, Height: {}".format(view.width, view.height)  # TODO remove
        # stdscr.addstr(1, 0, whstr, curses.color_pair(1))  # TODO remove

        view.paint()

        # Move cursor
        stdscr.move(cursor_y, cursor_x)

        # stdscr.addstr(0, 0, "Current mode: Typing mode", curses.A_REVERSE)

        # Refresh the screen
        stdscr.refresh()

        # Wait for next input
        k = stdscr.getch()


if __name__ == '__main__':
    os.environ.setdefault('ESCDELAY', '25')
    curses.wrapper(main)
