import difflib
import re

from .color import colored

__all__ = [
    'diff_files',
]


def diff_files(l_path, r_path, l_lines=None, r_lines=None, collapse_lines=None, collapse_format='... ({num} lines)', stream=False, num_context_lines=0, indent=''):
    if l_lines is None:
        with open(l_path, 'r') as l_file:
            l_lines = l_file.readlines()
    if r_lines is None:
        with open(r_path, 'r') as r_file:
            r_lines = r_file.readlines()
    blk = []
    def _show_collapsed():
        if blk:
            text = collapse_format.format(num=len(blk)).rstrip('\n')
            print(colored(text, 'magenta'), file=stream)
            blk.clear()

    for line in difflib.unified_diff(l_lines, r_lines, str(l_path), str(r_path), n=num_context_lines):
        if collapse_lines is not None and collapse_lines(line):
            blk.append(line)
        else:
            _show_collapsed()
            print(indent + format_diff_line(line), file=stream, end='')
    _show_collapsed()


def format_diff_lines(lines):
    return [format_diff_line(line) for line in lines]


def format_diff_line(line):
    if line.startswith('---') or line.startswith('+++'):
        line = colored(line, styles=['bold'])
    elif line.startswith('@@'):
        line = colored(line, 'bright-cyan')
    elif line.startswith('-'):
        line = colored(line, 'red')
    elif line.startswith('+'):
        line = colored(line, 'green')
    return line
