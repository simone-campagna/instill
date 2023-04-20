import abc
import functools
import itertools
import json
import os
import shutil
import sys

from collections.abc import Mapping, MutableMapping
from contextlib import contextmanager
from pathlib import Path

import yaml

from .color import colored, Console, C
from .formula import Formula, FormulaParseError
from .log import LOG
from .drop import DropError, Container, Drop, Flask, DropFilter
from .util import diff_files

__all__ = [
    'MutableContainer',
    'Recipient',
]


def h_name(text):
    return text
    # return C.xxi(text)


class Position(abc.ABC):
    @abc.abstractmethod
    def __call__(self, drop_file):
        raise NotImplemented()


class Begin(Position):
    def __call__(self, drop_file):
        for l_index, line in enumerate(drop_file.lines):
            if not line.startswith('#!'):
                return l_index
        return 0


class End(Position):
    def __call__(self, drop_file):
        return len(drop_file.lines)


class _Relative(Position):
    def __init__(self, filters):
        self.filters = filters

    @classmethod
    def build(cls, value):
        return cls([DropFilter.build(value)])

    def filtered_flasks(self, drop_file):
        flasks = [drop_file[name] for name in drop_file.filter(self.filters)]
        if not flasks:
            raise DropError(f'filters {self.filters}: 0 drops selected')
        return flasks

class Before(_Relative):
    def __call__(self, drop_file):
        flasks = self.filtered_flasks(drop_file)
        return min(flask.start for flask in self.filtered_flasks(drop_file))


class After(_Relative):
    def __call__(self, drop_file):
        flasks = self.filtered_flasks(drop_file)
        return max(flask.end for flask in self.filtered_flasks(drop_file))


class AtLine(Position):
    def __init__(self, line_index):
        self.line_index = line_index

    def __call__(self, drop_file):
        return self.line_index


class MutableContainer(MutableMapping, Container):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.content_version = 0

    def _update_lines(self, l_start, l_diff):
        for flask in self.flasks.values():
            if flask.start >= l_start:
                flask.start += l_diff
                flask.end += l_diff

    def __delitem__(self, name):
        self.del_drop(name, content_only=False)

    def del_drop(self, name, content_only=False):
        if content_only:
            flask = self.flasks[name]
            start, end = flask.index_range(headers=False)
        else:
            flask = self.flasks.pop(name)
            start, end = flask.index_range(headers=True)
        del self.lines[start:end]
        self._update_lines(flask.start, -(end - start))
        self.content_version += 1

    def __setitem__(self, name, drop):
        self.set_drop(name, drop)

    def set_drop(self, name, drop, empty=False, position=None):
        if isinstance(drop, Formula):
            drop = formula()
        if not isinstance(drop, Drop):
            raise TypeError(drop)

        name = drop.name
        drop_type = drop.drop_type
        if empty:
            content_lines = None
        else:
            content_lines = drop.encode(drop.get_content())

        self.content_version += 1
        deleted_flask = self.flasks.get(name, None)
        if deleted_flask:
            # replace existing block
            del self[name]
            start = deleted_flask.start
            if position is None:
                position = AtLine(start)
        else:
            if position is None:
                if drop_type == 'text':
                    position = Begin()
                else:
                    position = End()
        start = position(self)
        drop_lines = self.get_drop_lines(name=name, conf=drop.conf, content_lines=content_lines)
        # drop_lines = [f'# drop: start {name}\n']
        # for key, value in drop.conf.items():
        #     serialized_value = json.dumps(value)
        #     drop_lines.append(f'# drop: - {key}={serialized_value}\n')
        # if content_lines is not None:
        #     drop_lines.extend(content_lines)
        # drop_lines.append(f'# drop: end {drop.name}\n')
        self.lines[start:start] = drop_lines
        l_diff = len(drop_lines)
        self._update_lines(start, l_diff)
        flask = Flask(self, name=name, start=start, end=start + len(drop_lines), conf=drop.conf)
        self.flasks[flask.name] = flask

    def get_drop_lines(self, name, conf, content_lines):
        drop_lines = [f'# drop: start {name}\n']
        for key, value in conf.items():
            serialized_value = json.dumps(value)
            drop_lines.append(f'# drop: - {key}={serialized_value}\n')
        if content_lines is not None:
            drop_lines.extend(content_lines)
        drop_lines.append(f'# drop: end {name}\n')
        return drop_lines


class Recipient(MutableContainer):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.path is None:
            self.base_dir = Path.cwd()
        else:
            self.base_dir = Path(os.path.normpath(str(self.path.parent.absolute())))
        self.__cached_formulae = None
        # self.__parse_formulae()

    def relocate(self, output_path):
        lines = self.lines[:]
        out_base_dir = output_path.parent
        flask_items = sorted(self.items(), key=lambda x: x[1].start)
        offset = 0
        for name, flask in flask_items:
            content_lines = flask.get_lines(headers=False)
            # reparse flask conf:
            formula = self.get_formula(name)
            conf = formula.parse_conf(self.base_dir, output_path, flask.conf)
            # relocate parsed conf:
            conf = formula.relocate_conf(out_base_dir, output_path, conf)
            # create new formula's conf:
            r_formula = type(formula)(name=name, **conf)
            conf = r_formula.conf()
            # rewrite drop lines:
            drop_lines = self.get_drop_lines(name=name, conf=conf, content_lines=content_lines)
            lines[offset + flask.start:offset + flask.end] = drop_lines
            offset += len(drop_lines) - (flask.end - flask.start)
        return Recipient(output_path, lines=lines)

    @contextmanager
    def refactor(self, output_path=None, force_rewrite=False, mode=None):
        if self.path is None:
            raise DropError('{self}: path is not set')
        content_version = self.content_version
        yield
        if output_path is None:
            output_path = self.path
        else:
            output_path = Path(output_path)
        if output_path.resolve() != self.path.resolve():
            # out ot place, container must be relocated!
            container = self.relocate(output_path)
            if mode is None:
                if self.path.is_file():
                    mode = self.path.stat().st_mode
                else:
                    mode = None
            with container.refactor(force_rewrite=True, mode=mode):
                pass
            return

        write = True
        if (not force_rewrite) and content_version == self.content_version:
            write = False
        if write:
            if mode is None and self.path.is_file():
                mode = self.path.stat().st_mode
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, 'w') as fh:
                fh.writelines(self.lines)
            if mode is not None:
                output_path.chmod(mode)

    def __parse_formulae(self):
        if self.__cached_formulae is None:
            console = Console()
            errors = 0
            self.__cached_formulae = {}
            for name, flask in self.items():
                formula_name = flask.formula
                if formula_name is None:
                    console.error(f'drop {C.xxb(name)}: formula not set')
                    errors += 1
                    continue
                try:
                    formula_class = Formula.formula_class(flask.formula)
                    parsed_conf = formula_class.parse_conf(self.base_dir, self.path, flask.conf)
                    formula = formula_class(name=flask.name, **parsed_conf)
                    self.__cached_formulae[name] = formula
                except Exception as err:
                    console.error(f'drop {C.xxb(name)}: cannot create formula {C.xxb(flask.formula)}: {C.rxb(type(err).__name__)}: {C.rxx(str(err))}')
                    errors += 1
                    continue
                if formula is not None:
                    formula.fix_conf(flask.conf)

    def get_formula(self, name):
        self.__parse_formulae()
        if name not in self.__cached_formulae:
            raise DropError(f'drop {name}: formula not available')
        return self.__cached_formulae[name]

    def abs_path(self, path):
        path = Path(path)
        if not path.is_absolute():
            path = self.base_dir / path
        return path

    def rel_path(self, path):
        path = Path(path).absolute()
        if self.base_dir in path.parents:
            return path.relative_to(self.base_dir)
        return path

    def __paths(self, output_file=None):
        source_path = self.path
        source_rel_path = self.rel_path(source_path)
        if output_file is None:
            target_rel_path, target_path = source_rel_path, source_path
        else:
            target_path = Path(output_file).absolute()
            target_rel_path = self.rel_path(target_path)
        return source_rel_path, source_path, target_rel_path, target_path

    def set_formula(self, formula, output_file=None, replace=False, position=None, empty=False):
        source_rel_path, source_path, target_rel_path, target_path = self.__paths(output_file)

        LOG.info(f'{source_rel_path} -> {target_rel_path}')
        if formula.name in self and not replace:
            raise DropError(f'cannot overwrite drop {formula.name}')
        with self.refactor(target_path):
            self.set_drop(formula.name, formula(), position=position, empty=empty)

    def del_drops(self, output_file=None, filters=None, content_only=False):
        source_rel_path, source_path, target_rel_path, target_path = self.__paths(output_file)

        LOG.info(f'{source_rel_path} -> {target_rel_path}')
        with self.refactor(target_path):
            names = self.filter(filters)
            for name in names:
                self.del_drop(name, content_only=content_only)

    def update(self, output_file=None, filters=None, stream=sys.stdout, info_level=0):
        console = Console(stream=stream, info_level=info_level)
        source_rel_path, source_path, target_rel_path, target_path = self.__paths(output_file)

        LOG.info(f'{source_rel_path} -> {target_rel_path}')
        with self.refactor(target_path):
            if filters:
                included_names = self.filter(filters)
                excluded_names = set(self).difference(included_names)
                # print(filters, included_names, excluded_names)
            else:
                excluded_names = set()
            for name, flask in list(self.items()):
                formula = self.get_formula(name)
                formula.fix_conf(flask.conf)
                if name not in self or name not in excluded_names:
                    console.print(h_name(name), end=' ')
                    try:
                        e_drop = formula()
                        if name in self:
                            if flask.is_set():
                                f_drop = flask.drop
                                if f_drop.get_lines() == e_drop.get_lines():
                                    console.print(C.Cxb('skipped'))
                                    continue
                        self.set_drop(name, e_drop, empty=False)
                        console.print(C.Gxb('added'))
                    except:
                        console.print(C.Rxb('add failed!'))
                        raise
            for discarded_name in set(self).difference(self):  # FIXME - useless
                console.print(h_name(name), end=' ')
                try:
                    del self[discarded_name]
                    console.print(C.gxx('removed'))
                except:
                    console.print(C.rxx('remove failed!'))
                    raise

    def status(self, stream=sys.stdout, info_level=0, filters=None):
        console = Console(stream=stream, info_level=info_level)
        if not self.path.is_file():
            console.error(f'file {self.path} is missing')
            return
        names = self.filter(filters)
        for name in names:
            formula = self.get_formula(name)
            flask = self[name]
            formula.fix_conf(flask.conf)
            console.print(h_name(name), end=' ')
            if not flask.is_set():
                console.print(f'{C.Yxb("not-set")}')
                continue
            try:
                f_drop = flask.drop
                f_lines = f_drop.get_lines()
            except Exception as err:
                f_lines = []
            try:
                e_drop = formula()
                e_lines = e_drop.get_lines()
            except Exception as err:
                console.print(f'{C.Rxb("drop load error")}: {type(err).__name__}: {err}')
                continue
            if f_lines != e_lines:
                console.print(f'{C.Rxb("out-of-date")}')
            else:
                console.print(f'{C.Gxb("up-to-date")}')

    def diff(self, stream=sys.stdout, info_level=0, filters=None, binary=False):
        console = Console(stream=stream, info_level=info_level)
        if not self.path.is_file():
            console.error(f'file {rel_path} is missing')
            return
        names = self.filter(filters)
        for name in names:
            formula = self.get_formula(name)
            flask = self[name]
            formula.fix_conf(flask.conf)
            start, end = flask.index_range(headers=False)
            console.print(h_name(name), end=' ')
            if flask.is_set():
                try:
                    f_drop = flask.drop
                    f_lines = f_drop.get_lines()
                except Exception as err:
                    f_lines = []
            else:
                f_lines = []
            try:
                e_drop = formula()
                e_lines = e_drop.get_lines()
            except Exception as err:
                console.print(f'{C.Rxb("drop load error")}: {type(err).__name__}: {err}')
                continue
            if f_lines != e_lines:
                console.print(f'{C.Rxb("out-of-date")}')
                if (not binary) and flask.drop_type == 'bytes':
                    console.print(C.Rxb('(binary diff)'))
                else:
                    all_f_lines = list(self.lines)
                    all_e_lines = list(self.lines)
                    all_f_lines[start:] = f_lines
                    all_e_lines[start:] = e_lines
                    diff_files(f'found', f'expected', all_f_lines, all_e_lines,
                               stream=stream,
                               num_context_lines=3,
                               # indent='    ',
                    )
            else:
                console.print(f'{C.Gxb("up-to-date")}')

    def show_drop_conf(self, stream=sys.stdout, filters=None):
        console = Console(stream=stream)
        names = self.filter(filters)
        for name in names:
            console.print(f'{h_name(name)}')
            flask = self[name]
            for var_name, var_value in flask.conf.items():
                console.print(C.xxi(f'    {var_name}={json.dumps(var_value)}'))

    def show_drop_lines(self, stream=sys.stdout, filters=None):
        console = Console(stream=stream)
        names = self.filter(filters)
        for name in names:
            console.print(f'{h_name(name)} x')
            flask = self[name]
            drop = flask.drop
            offset = flask.start
            for index, line in enumerate(drop.get_lines()):
                console.print(f'    {index + offset:6d}| {C.xxi(line)}', end='')

    def list_drops(self, stream=sys.stdout, show_header=True, filters=None):
        console = Console(stream=stream)
        table = []
        names = self.filter(filters)
        for name in names:
            flask = self[name]
            formula = flask.formula or ''
            num_chars = len(flask.get_text())
            table.append([flask.name, flask.drop_type or '', formula or '',
                          f'{flask.start+1}:{flask.end+1}', str(num_chars)])
        if table:
            if show_header:
                names.insert(0, None)
                table.insert(0, ['name', 'type', 'formula', 'lines', 'size'])
            mlen = [max(len(row[c]) for row in table) for c in range(len(table[0]))]
            if show_header:
                names.insert(1, None)
                table.insert(1, ['─' * ml for ml in mlen])

            fmt = ' '.join(f'{{:{ml}s}}' for ml in mlen)
            for name, row in zip(names, table):
                console.print(fmt.format(*row))
