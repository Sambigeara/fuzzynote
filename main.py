import curses
import os
from enum import Enum
from xml.etree import ElementTree as ET

from fuzzy_note.parser import parse_tree

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


class View:
    """Represents the data that should visually populate the window at any one time"""
    def __init__(self, stdscr):
        self.tree = self.parse_tree(FILE_LOCATION, stdscr.addstr, 2)

    def crawler(self, node, paint_func, height, width=0):
        width += 4
        for child in node:
            if not child.attrib.get("_complete"):
                height += 1
                paint_func(height, width, f"- {child.attrib['text']}"[:50])
            self.crawler(child, paint_func, height, width)

    def parse_tree(self, file_name, paint_func, start_height=0):
        tree = ET.parse(file_name)
        root = tree.getroot()
        body = root[1]
        arr = []
        self.crawler(body, paint_func, start_height)
        return arr


def main(stdscr):
    global INPUT_MODE
    k = 0
    cursor_x = 0
    cursor_y = 2

    stdscr.clear()
    stdscr.refresh()

    # Start colors in curses
    curses.init_pair(1, curses.COLOR_CYAN, curses.COLOR_BLACK)
    curses.init_pair(2, curses.COLOR_RED, curses.COLOR_BLACK)
    curses.init_pair(3, curses.COLOR_BLACK, curses.COLOR_WHITE)

    # Loop where k is the last character pressed
    while (k != ord('q')):

        # Initialization
        stdscr.erase()
        height, width = stdscr.getmaxyx()

        if INPUT_MODE == InputMode.NORMAL:
            if k == 27:
                stdscr.timeout(10)
                next_k = stdscr.getch()
                if next_k == curses.ERR:
                    INPUT_MODE = InputMode.INSERT
                stdscr.timeout(1000)
        elif INPUT_MODE == InputMode.INSERT and k == ord('i'):
            INPUT_MODE = InputMode.NORMAL

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
        cursor_x = min(width-1, cursor_x)

        cursor_y = max(0, cursor_y)
        cursor_y = min(height-1, cursor_y)

        # Rendering some text
        stdscr.addstr(0, 0, f"Current mode: {INPUT_MODE.value} mode", curses.A_REVERSE)
        whstr = "Width: {}, Height: {}".format(width, height)
        stdscr.addstr(1, 0, whstr, curses.color_pair(1))

        parse_tree(FILE_LOCATION, stdscr.addstr, 2)

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
