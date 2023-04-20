#!/usr/bin/env python3

import argparse
import fnmatch
import inspect
import sys

from pathlib import Path

import yaml

from .log import (
    configure_logging,
    set_trace,
    trace_errors,
)
from .drop import (
    get_max_line_length,
    set_max_line_length,
    DropFilter,
    Container,
)
from .formula import (
    ApiFormula,
    FileFormula,
    SourceFormula,
    DirFormula,
    UrlFormula,
)
from .version import get_version
from .recipient import (
    Before, After, Begin, End,
    Recipient,
)
from . import api


def add_input_argument(parser):
    parser.add_argument(
        'input_file',
        metavar='input',
        help='input python file')


def add_output_argument(parser, optional=True):
    kwargs = {}
    if optional:
        kwargs['nargs'] = '?'
    parser.add_argument(
        'output_file',
        metavar='output',
        help='do not change input file in-place, write output file instead',
        **kwargs)


def add_filters_argument(parser, required=False):
    parser.add_argument(
        '-f', '--filter',
        dest='filters',
        metavar='FLT',
        type=DropFilter.build,
        action='append',
        default=[],
        required=required,
        help="""\
add drop filters; the format can contain 'name', ':type' and '^formula',
where all components are optional. The name, type and formula values are patterns,
eventually preceded by ~ to reverse selection. For instance: '^api', '^url *wg*'""")


def add_name_argument(parser, required=False):
    parser.add_argument(
        '-n', '--name',
        required=required,
        help='drop name')


class FormulaType:
    class FormulaBuilder:
        def __init__(self, formula_class, value):
            self.formula_class = formula_class
            self.value = value

        def __call__(self, name, drop_type=None):
            obj = self.formula_class(self.value, name=name, drop_type=drop_type)
            return obj

        def __str__(self):
            return self.value

        def __repr__(self):
            return self.value
            #return f'{type(self).__name__}({self.formula_class.__name__}, {self.value})'

    __registry__ = {}

    def __init__(self, formula_class):
        self.formula_class = formula_class

    def __call__(self, value):
        key = (self.formula_class, value)
        if key not in self.__registry__:
            self.__registry__[key] = self.__class__.FormulaBuilder(*key)
        return self.__registry__[key]


def fn_drop_update(input_file, output_file, max_line_length, filters=None):
    if max_line_length is not None:
        set_max_line_length(max_line_length)

    recipient = Recipient(input_file)
    recipient.update(output_file=output_file, filters=filters)


def fn_drop_extract(input_file, output_file, name):
    container = Container(input_file)
    flask = container[name]
    drop = flask.drop
    drop.write_file(output_file)


def fn_drop_set(input_file, output_file, formula_builder, name, drop_type, max_line_length, replace, position, empty=False):
    if max_line_length is not None:
        set_max_line_length(max_line_length)
    formula = formula_builder(
        name=name,
        drop_type=drop_type)
    recipient = Recipient(input_file)
    recipient.set_formula(formula, output_file=output_file,
                   replace=replace, position=position, empty=empty)


def fn_drop_del(input_file, output_file, filters, content_only=False):
    recipient = Recipient(input_file)
    recipient.del_drops(output_file=output_file, filters=filters, content_only=content_only)


def fn_drop_status(input_file, filters=None, info_level=0):
    recipient = Recipient(input_file)
    recipient.status(filters=filters, info_level=info_level)


def fn_drop_diff(input_file, filters=None, binary=True, info_level=0):
    recipient = Recipient(input_file)
    recipient.diff(filters=filters, info_level=info_level, binary=binary)


def fn_drop_list(input_file, show_header, filters):
    recipient = Recipient(input_file)
    recipient.list_drops(
        show_header=show_header,
        filters=filters)


def fn_drop_show(input_file, show_target, filters):
    recipient = Recipient(input_file)
    if show_target == 'lines':
        function = recipient.show_drop_lines
    elif show_target == 'conf':
        function = recipient.show_drop_conf
    else:
        return
    function(filters=filters)


def add_common_arguments(parser):
    parser.add_argument(
        '--trace',
        action='store_true',
        default=False,
        help=argparse.SUPPRESS)
    v_mgrp = parser.add_mutually_exclusive_group()
    v_kwargs = {'dest': 'verbose_level', 'default': 1}
    v_mgrp.add_argument(
        '-v', '--verbose',
        action='count',
        help='increase verbose level',
        **v_kwargs)
    parser.add_argument(
        '-q', '--quiet',
        action='store_const',
        const=0,
        help='suppress warnings',
        **v_kwargs)


def build_parser(name, *, subparsers=None, function=None, **kwargs):
    if subparsers:
        parser = subparsers.add_parser(name, **kwargs)
    else:
        parser = argparse.ArgumentParser(name, **kwargs)
        add_common_arguments(parser)
    if function is None:
        function = parser.print_help
    parser.set_defaults(function=function)
    return parser


class InputFile:
    def __init__(self, file_type, file_name):
        self.file_type = file_type
        self.file_name = Path(file_name)


class InputFileType:
    def __init__(self, file_type):
        self.file_type = file_type

    def __call__(self, file_name):
        return InputFile(file_type=self.file_type, file_name=file_name)


def build_instill_parser(subparsers=None):
    parser = build_parser(
        name='instill',  subparsers=subparsers,
        description=f'''\
instill {get_version()} - add drops of data to source files
'''
    )
    subparsers = parser.add_subparsers()

    ### list
    list_parser = build_parser(
        'list', subparsers=subparsers,
        function=fn_drop_list,
        description='list drops in source file')
    add_input_argument(list_parser)
    add_filters_argument(list_parser)

    list_parser.add_argument(
        '-H', '--no-header',
        dest='show_header',
        action='store_false',
        default=True,
        help='do not show table header lines')

    ### show
    show_parser = build_parser(
        'show', subparsers=subparsers,
        function=fn_drop_show,
        description='show drops in source file')
    add_input_argument(show_parser)
    add_filters_argument(show_parser)

    target_mgrp = show_parser.add_mutually_exclusive_group()
    target_kwargs = {'dest': 'show_target', 'default': 'conf'}
    target_mgrp.add_argument(
        '-c', '--conf',
        action='store_const', const='conf',
        help='show drop conf',
        **target_kwargs)
    target_mgrp.add_argument(
        '-l', '--lines',
        action='store_const', const='lines',
        help='show drop lines',
        **target_kwargs)

    ### update
    update_parser = build_parser(
        'update', subparsers=subparsers,
        function=fn_drop_update,
        description='update drops in source file')
    add_input_argument(update_parser)
    add_output_argument(update_parser)
    add_filters_argument(update_parser)
    update_parser.add_argument(
        '-m', '--max-line-length',
        default=None,
        help='set max data line length')

    ### extract
    extract_parser = build_parser(
        'extract', subparsers=subparsers,
        function=fn_drop_extract,
        description='extract a drop object from source file')
    add_input_argument(extract_parser)
    add_output_argument(extract_parser, optional=False)
    add_name_argument(extract_parser)

    ### add
    set_parser = build_parser(
        'set', subparsers=subparsers,
        function=fn_drop_set,
        description='set drops in source file')
    add_input_argument(set_parser)
    add_output_argument(set_parser)

    set_parser.add_argument(
        '-n', '--name',
        default=None,
        help='drop name')

    set_parser.add_argument(
        '-m', '--max-line-length',
        metavar='LEN',
        default=None,
        help=f'set max data line length (default: {get_max_line_length()})')

    set_parser.add_argument(
        '-t', '--type',
        dest='drop_type',
        choices=['text', 'bytes'],
        default=None,
        help="drop type (default: 'text' for source drops, else 'bytes')")

    pos_group = set_parser.add_argument_group('position')
    pos_kwargs = {'dest': 'position', 'default': None}
    pos_group.add_argument(
        '-B', '--before',
        type=Before.build,
        help='add before drops',
        **pos_kwargs)
    pos_group.add_argument(
        '-A', '--after',
        type=After.build,
        help='add after drops',
        **pos_kwargs)
    pos_group.add_argument(
        '-b', '--begin',
        action='store_const', const=Begin(),
        help='add at the beginning of the file',
        **pos_kwargs)
    pos_group.add_argument(
        '-e', '--end',
        action='store_const', const=End(),
        help='add at the end of the file',
        **pos_kwargs)

    set_parser.add_argument(
        '-E', '--empty',
        action='store_true', default=False,
        help='add empty drop (no contents)')

    c_group = set_parser.add_argument_group('drop')
    c_mgrp = set_parser.add_mutually_exclusive_group(required=True)
    c_kwargs = {'dest': 'formula_builder'}
    api_formula = FormulaType(ApiFormula)
    c_mgrp.add_argument(
        '-a', '--api',
        choices=[api_formula(impl) for impl in api.get_api_implementations()],
        type=FormulaType(ApiFormula),
        nargs='?', const=FormulaType(ApiFormula)('memory'),
        help='add instill api (default implementation: "memory")',
        **c_kwargs)
    c_mgrp.add_argument(
        '-p', '--py-source',
        metavar='PY_SOURCE',
        type=FormulaType(SourceFormula),
        help='add python source file',
        **c_kwargs)
    c_mgrp.add_argument(
        '-f', '--file',
        metavar='FILE',
        type=FormulaType(FileFormula),
        help='add file',
        **c_kwargs)
    c_mgrp.add_argument(
        '-d', '--dir',
        metavar='DIR',
        type=FormulaType(DirFormula),
        help='add directory',
        **c_kwargs)
    c_mgrp.add_argument(
        '-u', '--url',
        metavar='URL',
        type=FormulaType(UrlFormula),
        help='add url',
        **c_kwargs)
    set_parser.add_argument(
        '-r', '--replace',
        action='store_true', default=False,
        help='replace existing drop with the same name')

    ### del
    del_parser = build_parser(
        'del', subparsers=subparsers,
        function=fn_drop_del,
        description='remove drops from source file')
    add_input_argument(del_parser)
    add_output_argument(del_parser)
    add_filters_argument(del_parser, required=True)
    del_parser.add_argument(
        '-c', '--content-only',
        action='store_true', default=False,
        help='remove only drop content')

    ### status:
    status_parser = build_parser(
        'status', subparsers=subparsers,
        function=fn_drop_status,
        description='show the source file status')
    add_input_argument(status_parser)
    add_filters_argument(status_parser)

    ### diff:
    diff_parser = build_parser(
        'diff', subparsers=subparsers,
        function=fn_drop_diff,
        description='show diffs')
    add_input_argument(diff_parser)
    add_filters_argument(diff_parser)
    diff_parser.add_argument(
        '-b', '--binary',
        action='store_true', default=False,
        help='show diff in encoded binary drops')

    return parser


def runner(parser):
    ### parsing:
    ns = parser.parse_args()
    set_trace(ns.trace)
    configure_logging(ns.verbose_level)

    ns_vars = vars(ns)
    function = ns.function
    f_args = {}
    for p_name, p_obj in inspect.signature(function).parameters.items():
        if p_name in ns_vars:
            f_args[p_name] = ns_vars[p_name]
        elif p_obj.default is p_obj.empty:
            raise RuntimeError(f'internal error: {function.__name__}: missing argument {p_name}')
    with trace_errors(function.__name__, on_error='exit'):
        result = function(**f_args)
    if not result:
        sys.exit(0)
    sys.exit(1)


def main():
    parser = build_instill_parser()
    runner(parser)
