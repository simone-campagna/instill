import functools
import itertools
import re
import sys


__all__ = [
    'set_colored',
    'get_colored',
    'AnsiColor',
    'C',
    'colored',
    'Console',
]


COLORED = None


def set_colored(value=None):
    global COLORED
    if value is None:
        value = sys.stdout.isatty()
    COLORED = bool(value)


def get_colored():
    global COLORED
    if COLORED is None:
        set_colored()
    return COLORED


class AnsiColor:
    __colors__ = {
        'black': 30,  # 'grey' in termcolor
        'red': 31,
        'green': 32,
        'yellow': 33,
        'blue': 34,
        'magenta': 35,
        'cyan': 36,
        'white': 37,
        'bright-black': 90,
        'bright-red': 91,
        'bright-green': 92,
        'bright-yellow': 93,
        'bright-blue': 94,
        'bright-magenta': 95,
        'bright-cyan': 96,
        'bright-white': 97,
    }
    __background_offset__ = 10
    __styles__ = {
        'bold': 1,
        'light': 2,  # 'dark' in termcolor
        'italic': 3,
        'underline': 4,
        'slow-blink': 5,
        'blink': 5,
        'fast-blink': 6,
        'reverse': 7,
        'concealed': 8,
        'strike': 9,
    }
    __ansi_format__ = '\033[{code}m'
    __reset__ = __ansi_format__.format(code=0)
    __regex_uncolor__ = __ansi_format__.format(code=r'\d+')


    def _rendered(self, code):
        return self.__ansi_format__.format(code=code)

    def rendered_fg_color(self, name):
        return self._rendered(self.__colors__[name])

    def rendered_bg_color(self, name):
        return self._rendered(self.__colors__[name] + self.__background_offset__)

    def rendered_styles(self, styles):
        rendered_styles = []
        for style in styles:
            rendered_styles.append(self._rendered(self.__styles__[style]))
        return ''.join(rendered_styles)

    def color(self, text, fg_color=None, bg_color=None, styles=None):
        text = str(text)
        if not get_colored():
            return text
        tokens = []
        if fg_color:
            tokens.append(self.rendered_fg_color(fg_color))
        if bg_color:
            tokens.append(self.rendered_bg_color(bg_color))
        if styles:
            tokens.append(self.rendered_styles(styles))
        if tokens:
            tokens.append(text)
            tokens.append(self.__reset__)
            return ''.join(tokens)
        else:
            return text

    def uncolor(self, text):
        return self.__re_uncolor__.sub('', text)

    def __call__(self, text, fg_color=None, bg_color=None, styles=None):
        return self.color(text, fg_color=fg_color, bg_color=bg_color, styles=styles)


ANSI_COLOR = AnsiColor()


class Colors:
    def __init__(self):
        colors = {
            'x': None,
            'k': 'black',
            'r': 'red',
            'g': 'green',
            'y': 'yellow',
            'b': 'blue',
            'm': 'magenta',
            'c': 'cyan',
            'w': 'white',
            'K': 'bright-black',
            'R': 'bright-red',
            'G': 'bright-green',
            'Y': 'bright-yellow',
            'B': 'bright-blue',
            'M': 'bright-magenta',
            'C': 'bright-cyan',
            'W': 'bright-white',
        }
        styles = {
            'x': [],
            'b': ['bold'],
            'l': ['light'],
            'i': ['italic'],
            'u': ['underline'],
        }
        for fg_color, bg_color, style in itertools.product(colors, colors, styles):
            name = ''.join([fg_color, bg_color, style])
            function = functools.partial(colored, fg_color=colors[fg_color], bg_color=colors[bg_color], styles=styles[style])
            setattr(self, name, function)


colored = ANSI_COLOR.color

C = Colors()


class Console:
    DEBUG = 0
    INFO = 1
    WARNING = 0
    ERROR = 2
    def __init__(self, stream=sys.stdout, info_level=0):
        self.stream = stream
        self.info_level = info_level
        fmt = '{:7s}'
        self._hdr = {
            None: fmt.format(''),
            self.DEBUG: colored(fmt.format('debug'), 'blue'),
            self.INFO: colored(fmt.format('info'), 'green'),
            self.WARNING: colored(fmt.format('warning'), 'yellow'),
            self.ERROR: colored(fmt.format('error'), 'red'),
        }

    def print(self, text, *args, file=None, flush=None, **kwargs):
        if file is None:
            file = self.stream
        if flush is None:
            flush = True
        print(text, *args, file=self.stream, flush=flush, **kwargs)

    def debug(self, text, **kwargs):
        self.log(self.DEBUG, text, **kwargs)

    def info(self, text, **kwargs):
        self.log(self.INFO, text, **kwargs)

    def warning(self, text, **kwargs):
        self.log(self.WARNING, text, **kwargs)

    def error(self, text, **kwargs):
        self.log(self.ERROR, text, **kwargs)

    def log(self, level, text, *, continuation=False):
        if level >= self.info_level:
            if continuation:
                lv = None
            else:
                lv = level
            print(f'{self._hdr[lv]} {text}', file=self.stream)
