# drop: start instill_api
# drop: - formula="api"
# drop: - type="text"
import abc
import base64
import datetime
import fnmatch
import inspect
import io
import json
import re
import sys
import tarfile

from collections.abc import Mapping
from pathlib import Path
from operator import attrgetter


__all__ = [
    'get_container', 'is_defined', 'is_set', 'get', 'get_flask',
    'get_max_line_length', 'set_max_line_length',
    'Pattern', 'DropFilter', 'Drop', 'TextDrop', 'BytesDrop',
    'Flask', 'Container',
]

INSTILL_API_VERSION = '0.1.1'

MAX_LINE_LENGTH = 120

def get_max_line_length():
    return MAX_LINE_LENGTH


def set_max_line_length(value):
    global MAX_LINE_LENGTH
    MAX_LINE_LENGTH = int(value)


class Timestamp(datetime.datetime):
    def __str__(self):
        return self.strftime('%Y%m%d-%H%M%S')


class DropError(RuntimeError):
    pass


class UndefinedDropError(DropError):
    "drop not defined"


class AbstractDropError(DropError):
    "drop not set"


class DropMeta(abc.ABCMeta):
    def __new__(mcls, class_name, class_bases, class_dict):
        cls = super().__new__(mcls, class_name, class_bases, class_dict)
        if not inspect.isabstract(cls):
            cls.__registry__[cls.class_drop_type()] = cls
        return cls


UNDEF = object()


class Drop(metaclass=DropMeta):
    __registry__ = {}

    def __init__(self, name, init, conf=None, path=None):
        if isinstance(init, (str, bytes)):
            self.content = init
            self.lines = self.encode(self.content)
        else:
            self.lines = list(init)
            self.content = self.decode(self.lines)
        self.name = name
        self.conf = conf or {}
        if path:
            path = Path(path)
        self.path = path

    @property
    def formula(self):
        return self.conf.get('formula', None)

    @classmethod
    def drop_class(cls, drop_type, default=UNDEF):
        if default is UNDEF:
            return cls.__registry__[drop_type]
        else:
            return cls.__registry__.get(drop_type, default)

    @classmethod
    @abc.abstractmethod
    def class_drop_type(cls):
        raise NotImplementedError()

    @property
    def drop_type(self):
        return self.class_drop_type()

    @classmethod
    @abc.abstractmethod
    def encode(cls, content):
        raise NotImplementedError()

    @classmethod
    @abc.abstractmethod
    def decode(cls, lines):
        raise NotImplementedError()

    def get_lines(self):
        return self.lines

    def get_text(self):
        return ''.join(self.get_lines())

    def get_content(self):
        return self.content

    def _file_mode(self):
        return 'w'

    def _build_path(self, path):
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        return path

    def write_file(self, path):
        path = self._build_path(path)
        content = self.get_content()
        with open(path, self._file_mode()) as file:
            file.write(content)

    def __str__(self):
        return self.name

    def __repr__(self):
        return f'{type(self).__name__}({self.container!r}, {self.name!r}, {self.start!r}, {self.end!r})'


class TextDrop(Drop):
    @classmethod
    def encode(cls, content):
        return [line + '\n' for line in content.split('\n')]

    @classmethod
    def decode(cls, lines):
        return ''.join(lines)

    @classmethod
    def class_drop_type(cls):
        return 'text'


class BytesDrop(Drop):
    __data_prefix__ = '#|'

    @classmethod
    def encode(cls, content):
        lines = []
        data = str(base64.b85encode(content), 'utf-8')
        data_prefix = cls.__data_prefix__
        dlen = max(get_max_line_length() - len(data_prefix), len(data_prefix) + 1)
        for index in range(0, len(data), dlen):
            lines.append(f'{data_prefix}{data[index:index+dlen]}\n')
        return lines

    @classmethod
    def decode(cls, lines):
        data_prefix = cls.__data_prefix__
        data = ''.join(line[len(data_prefix):].strip() for line in lines if line.startswith(data_prefix))
        return base64.b85decode(data)

    @classmethod
    def class_drop_type(cls):
        return 'bytes'

    def _file_mode(self):
        return 'wb'

    def untar(self, path, mode='r|*'):
        path = self._build_path(path)
        b_file = io.BytesIO(self.get_content())
        with tarfile.open(fileobj=b_file, mode=mode) as t_file:
            t_file.extractall(path)


class Flask:
    def __init__(self, container, name, start, end, conf=None):
        self.container = container
        self.name = name
        self.start = start
        self.end = end
        self._drop = None
        self.conf = dict(conf or {})
        self.num_params = len(self.conf)

    def index_range(self, headers=False):
        if headers:
            return self.start, self.end
        else:
            return self.start + self.num_params + 1, self.end - 1

    @property
    def drop_type(self):
        return self.conf.get('type', None)

    @property
    def drop_class(self):
        drop_type = self.drop_type
        drop_class = Drop.drop_class(drop_type, None)
        if drop_class is None:
            raise DropError(f"{self.container.filename}@{self.start + 1}: unknown drop type {drop_type!r}")
        return drop_class

    def is_set(self):
        return bool(self.get_lines())

    @property
    def drop(self):
        if self._drop is None:
            init = self.get_lines()
            if not init:
                raise AbstractDropError(f"{self.container.filename}@{self.start + 1}: drop not set")
            self._drop = self.drop_class(
                name=self.name,
                init=init, conf=self.conf,
                path=self.container.path)
        return self._drop

    @property
    def formula(self):
        return self.conf.get('formula', None)

    def merge_conf(self, line_index, key, value):
        if line_index != (self.start + self.num_params + 1):
            raise DropError(f"{self.container.filename}@{line_index + 1}: unexpected drop conf")
        self.conf[key] = value
        self.num_params += 1

    def get_lines(self, headers=False):
        if headers:
            s_offset, e_offset = 0, 0
        else:
            s_offset, e_offset = self.num_params + 1, 1
        return self.container.lines[self.start+s_offset:self.end-e_offset]

    def get_text(self, headers=True):
        return '\n'.join(self.get_lines(headers=headers))

    def __str__(self):
        return self.name

    def __repr__(self):
        return f'{type(self).__name__}({self.container!r}, {self.name!r}, {self.start!r}, {self.end!r})'


def get_file():
    try:
        return __file__
    except NameError:
        # if __file__ is not available:
        return inspect.getfile(sys.modules[__name__])


class Pattern:
    def __init__(self, pattern, reverse=False):
        self.pattern = pattern
        self.reverse = reverse

    @classmethod
    def build(cls, value):
        if value.startswith('~'):
            reverse, pattern = True, value[1:]
        else:
            reverse, pattern = False, value
        return cls(pattern, reverse)

    def __call__(self, value):
        return self.reverse != bool(fnmatch.fnmatch(value, self.pattern))

    def __str__(self):
        if self.reverse:
            return f'~{self.pattern}'
        return self.pattern

    def __repr__(self):
        return f'{type(self).__name__}({self.pattern!r}, {self.reverse!r})'


class DropFilter:
    __regex__ = re.compile(r'(?P<op>[\^\:\/])?(?P<pattern>[^\^\:]+)\s*')
    __key_dict__ = {'': 'name', ':': 'drop_type', '^': 'formula', '/': 'path'}

    def __init__(self, name=None, drop_type=None, formula=None, path=None):
        self.patterns = []
        if name:
            self.patterns.append(('name', Pattern.build(name), attrgetter('name')))
        if drop_type:
            self.patterns.append(('drop_type', Pattern.build(drop_type), attrgetter('drop_type')))
        if formula:
            self.patterns.append(('formula', Pattern.build(formula), attrgetter('formula')))
        if path:
            if not path.startswith('/'):
                path = '*/' + path
            self.patterns.append(('path', Pattern.build(path), attrgetter('path')))

    @classmethod
    def build(cls, value):
        kwargs = {}
        for token in value.split():
            for op, pattern in cls.__regex__.findall(token):
                kwargs[cls.__key_dict__[op]] = pattern
        return cls(**kwargs)

    def __call__(self, drop):
        return all(pattern(getter(drop)) for _,  pattern, getter in self.patterns)

    def __repr__(self):
        args = ', '.join(f'{key}={pattern!r}' for key, pattern, _ in self.patterns)
        return f'{type(self).__name__}({args})'

    def __str__(self):
        key_rev = {value: key for key, value in self.__key_dict__.items()}
        return ' '.join(key_rev[key] + str(pattern) for key, pattern, _ in self.patterns)


class Container(Mapping):
    __re_drop__ = r'\# drop:\s+(?P<action>start|end)\s+(?P<name>[^\s\/\:]+)'
    __re_conf__ = r'\# drop:\s+-\s+(?P<key>\w+)\s*=\s*(?P<value>.*)\s*$'

    def __init__(self, file=None, lines=None):
        if file is None:
            file = get_file()
            path = None
        if isinstance(file, (str, Path)):
            path = Path(file)
            if lines is None:
                if isinstance(file, (str, Path)):
                    with open(file, 'r') as fh:
                        lines = fh.readlines()
        else:
            path = getattr(file, 'name', None)
            if path:
                path = Path(path)
            if lines is None:
                lines = file.readlines()
        self.file = file
        self.path = path
        self.filename = str(self.path) if self.path is not None else '<stdin>'
        self.lines = lines
        self.flasks = {}
        self._parse_lines()

    def filter(self, drop_filters):
        if drop_filters is None:
            drop_filters = []
        drops = list(self.values())
        for drop_filter in drop_filters:
            new_drops = []
            for drop in drops:
                if drop_filter(drop):
                    new_drops.append(drop)
            drops = new_drops
            if not drops:
                break
        selected_names = {drop.name for drop in drops}
        return [name for name in self if name in selected_names]

    def _parse_lines(self):
        filename = self.filename
        lines = self.lines
        re_drop = re.compile(self.__re_drop__)
        re_conf = re.compile(self.__re_conf__)
        flasks = self.flasks

        flask = None
        def _store_flask(line_index=None):
            nonlocal flasks, flask
            if flask:
                if line_index is None:
                    line_index = flask.start + flask.num_params
                flask.end = line_index + 1
                flasks[flask.name] = flask
                flask = None

        for cur_index, line in enumerate(lines):
            m_drop = re_drop.match(line)
            if m_drop:
                cur_action, cur_name = (
                    m_drop['action'], m_drop['name'])
                if cur_action == 'end':
                    if flask and cur_name == flask.name:
                        _store_flask(cur_index)
                        continue
                    else:
                        raise DropError(f'{filename}@{cur_index + 1}: unexpected directive "{cur_action} {cur_name}"')
                elif cur_action == 'start':
                    if flask:
                        # empty drop
                        _store_flask(None)
                    flask = Flask(self, name=cur_name, start=cur_index, end=None)
                    if flask.name in self.flasks:
                        raise DropError(f"{filename}@{flask.start + 1}: duplicated drop {flask}")
                    continue
                continue

            m_conf = re_conf.match(line)
            if m_conf:
                if flask is None:
                    raise DropError(f"{filename}@{cur_index + 1}: unexpected parameter {m_conf['key']}={m_conf['value']}")
                key = m_conf['key']
                serialized_value = m_conf['value']
                try:
                    value = json.loads(serialized_value)
                except Exception as err:
                    raise DropError(f"{filename}@{cur_index + 1}: conf key {key}={serialized_value!r}: {type(err).__name__}: {err}")
                flask.merge_conf(cur_index, key, value)

        if flask:
            _store_flask(None)

    def __len__(self):
        return len(self.flasks)

    def __iter__(self):
        yield from self.flasks

    def __getitem__(self, name):
        if name not in self.flasks:
            raise UndefinedDropError(f"{self.filename}: undefined drop {name!r}")
        return self.flasks[name]

    def is_defined(self, name):
        return name in self.flasks

    def is_set(self, name):
        return self.is_defined(name) and self.flasks[name].is_set()

    def get(self, name):
        return self.flasks[name].drop

    def __repr__(self):
        return f'{type(self).__name__}({self.filename!r})'


CONTAINER_CACHE = {}
def get_container(file=None):
    """get Container instance (cached)"""
    if file is None:
        file = __file__
    file = Path(file).resolve()
    container = CONTAINER_CACHE.get(file, None)
    if container is None:
        container = Container(file)
        CONTAINER_CACHE[file] = container
    return container


def is_defined(name, file=None):
    """return True if drop *name* is defined, eventually not set"""
    container = get_container(file)
    return container.is_defined(name)


def is_set(name, file=None):
    """return True if drop *name* is defined and set"""
    container = get_container(file)
    return container.is_set(name)


def get(name, file=None):
    """get the drop named *name*"""
    container = get_container(file)
    if name not in container:
        raise DropError(f'drop {name} not found')
    return container.get(name)


def get_flask(name, file=None):
    """get the drop named *name*"""
    container = get_container(file)
    if name not in container:
        raise DropError(f'drop {name} not found')
    return container[name]


# drop: end instill_api
