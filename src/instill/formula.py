import abc
import inspect
import os

from pathlib import Path
from urllib.parse import urlparse

from .drop import DropError, Drop
from . import api


__all__ = [
    'FormulaError',
    'FormulaParseError',
    'FormulaMeta',
    'Formula',
    'ApiFormula',
    'SourceFormula',
    'FileFormula',
    'DirFormula',
    'UrlFormula',
]


class FormulaError(DropError):
    pass


class FormulaParseError(FormulaError):
    pass


class FormulaMeta(abc.ABCMeta):
    def __new__(mcls, class_name, class_bases, class_dict):
        cls = super().__new__(mcls, class_name, class_bases, class_dict)
        if not inspect.isabstract(cls):
            formula = cls.formula()
            cls.__registry__[formula] = cls
        return cls


class Formula(metaclass=FormulaMeta):
    __registry__ = {}
    def __init__(self, name=None, drop_type=None):
        self.name = name
        self.drop_type = drop_type

        self._check_name()
        self._check_drop_type()

    def _default_name(self):
        return None

    @classmethod
    def fix_conf(cls, conf):
        if not conf.get('type', None):
            conf['type'] = cls.default_drop_type()

    @classmethod
    @abc.abstractmethod
    def formula(self):
        raise NotImplemented()

    @classmethod
    def formula_class(cls, formula):
        if formula not in cls.__registry__:
            raise DropError(f'unknown formula {formula}')
        return cls.__registry__[formula]

    @classmethod
    def parse_conf(cls, base_dir, filename, data):
        result = {}
        if 'type' in data:
            result['drop_type'] = data['type']
        return result

    @classmethod
    def relocate_conf(cls, base_dir, filename, conf):
        return conf.copy()

    def conf(self):
        return {
            'formula': self.formula(),
            'type': self.drop_type,
        }

    @classmethod
    def _parse_key(cls, data, key, types):
        value = data.get(key, None)
        if value is None:
            raise FormulaParseError(f'{key} key not set')
        if not isinstance(value, types):
            raise FormulaParseError(f'{key} {value!r}: invalid type')
        return value

    def drop_class(self):
        return Drop.drop_class(self.drop_type)

    def __call__(self):
        return self.drop_class()(
            name=self.name,
            init=self.content(),
            conf=self.conf(),
        )

    @classmethod
    def default_drop_type(cls):
        return 'bytes'

    def _check_name(self):
        if self.name is None:
            self.name = self._default_name()
        if self.name is None:
            raise FormulaError(f'{type(self).__name__}: drop name not set')

    def _check_drop_type(self):
        if self.drop_type is None:
            self.drop_type = self.default_drop_type()
        if self.drop_type is None:
            self.drop_type = 'bytes'
        if self.drop_type not in {'text', 'bytes'}:
            raise FormulaError(f'{type(self).__name__}: unknown drop type {self.drop_type!r}')

    @abc.abstractmethod
    def content(self):
        raise NotImplemented()

    def __repr__(self):
        return f'{type(self).__name__}({self.name!r}, {self.drop_type!r})'


class PathFormula(Formula):
    def __init__(self, base_dir, path, name=None, drop_type=None):
        self._orig_path = path
        self.base_dir = Path(base_dir)
        path = Path(path)
        if not path.is_absolute():
            path = base_dir / path
        self.path = Path(os.path.normpath(str(path)))
        self._check_path()
        super().__init__(name=name, drop_type=drop_type)

    def _default_name(self):
        return self.path.name

    @classmethod
    def parse_conf(cls, base_dir, filename, data):
        result = super().parse_conf(base_dir, filename, data)
        result['base_dir'] = base_dir
        path = cls._parse_key(data, 'path', (str, Path))
        result['path'] = path
        return result

    @classmethod
    def relocate_conf(cls, base_dir, filename, conf):
        conf = conf.copy()
        path = conf['path']
        if path:
            path = Path(path)
            if not path.is_absolute():
                path = conf['base_dir'] / path
                rel_path = os.path.relpath(path, base_dir)
                conf['path'] = rel_path
        conf['base_dir'] = base_dir
        return conf

    def conf(self):
        result = super().conf()
        result['path'] = str(self._orig_path)
        return result

    def _check_path(self):
        if self.path is None:
            raise FormulaError(f'{type(self).__name__}: path not set')

    def __repr__(self):
        return f'{type(self).__name__}({self.path!r}, {self.name!r}, {self.drop_type!r})'


class FileFormula(PathFormula):
    def _check_path(self):
        path = self.path
        if not path.is_file():
            raise FormulaError(f'{type(self).__name__}: {path} is not a file')
        super()._check_path()

    @classmethod
    def formula(cls):
        return 'file'

    def content(self):
        mode = 'r'
        if self.drop_type == 'bytes':
            mode += 'b'
        with open(self.path, mode) as fh:
            return fh.read()


class SourceFormula(FileFormula):
    @classmethod
    def formula(cls):
        return 'source'

    @classmethod
    def default_drop_type(cls):
        return 'text'


class DirFormula(PathFormula):
    def __init__(self, base_dir, path, arcname=None, name=None, drop_type=None):
        super().__init__(base_dir, path, name=name, drop_type=drop_type)
        self.arcname = arcname

    @classmethod
    def formula(cls):
        return 'dir'

    def _check_path(self):
        path = self.path
        if not path.is_dir():
            raise FormulaError(f'{type(self).__name__}: {path} is not a directory')
        super()._check_path()

    def content(self):
        import gzip
        import io
        import tarfile
        bf = io.BytesIO()
        with tarfile.open(fileobj=bf, mode='w') as tf:
            tf.add(self.path, arcname=self.arcname)
        return gzip.compress(bf.getvalue(), mtime=0.0)  # make compressed data reproducible!

    @classmethod
    def parse_conf(cls, base_dir, filename, data):
        result = super().parse_conf(base_dir, filename, data)
        arcname = data.get('arcname', None)
        result['arcname'] = arcname
        return result
        
    def conf(self):
        result = super().conf()
        result['arcname'] = self.arcname
        return result


class UrlFormula(Formula):
    def __init__(self, url, name=None, drop_type=None):
        self.url = url
        super().__init__(name=name, drop_type=drop_type)
        self._check_url()

    def _default_name(self):
        return Path(urlparse(self.url).path).name

    @classmethod
    def formula(cls):
        return 'url'

    @classmethod
    def parse_conf(cls, base_dir, filename, data):
        result = super().parse_conf(base_dir, filename, data)
        url = cls._parse_key(data, 'url', (str,))
        result['url'] = url
        return result

    def conf(self):
        result = super().conf()
        result['url'] = str(self.url)
        return result

    def _check_url(self):
        if self.url is None:
            raise rError(f'{type(self).__name__}: url not set')

    def content(self):
        import urllib.request
        with urllib.request.urlopen(self.url) as response:
            return response.read()

    def __repr__(self):
        return f'{type(self).__name__}({self.url!r}, {self.name!r}, {self.drop_type!r})'


class ApiFormula(Formula):
    def __init__(self, implementation, name=None, drop_type=None):
        self.implementation = implementation
        super().__init__(name=name, drop_type=drop_type)
        self._check_implementation()

    def _default_name(self):
        return 'drop'

    @classmethod
    def default_drop_type(cls):
        return 'text'

    @classmethod
    def parse_conf(cls, base_dir, filename, data):
        result = super().parse_conf(base_dir, filename, data)
        implementation = data.get('implementation', api.default_api_implementation())
        if implementation not in api.get_api_implementations():
            raise FormulaParseError(f'unknown api implementation {implementation!r}')
        result['implementation'] = implementation
        return result

    def conf(self):
        result = super().conf()
        result['implementation'] = str(self.implementation)
        return result

    @classmethod
    def formula(cls):
        return 'api'

    def _check_implementation(self):
        if self.implementation is None:
            self.implementation = api.default_api_implementation()
        elif self.implementation not in api.get_api_implementations():
            raise FormulaError(f'{type(self).__name__}: api implementation {self.implementation} is not a directory')

    def content(self):
        return api.get_api(self.name, self.implementation)

    def __repr__(self):
        return f'{type(self).__name__}({self.implementation!r}, {self.name!r}, {self.drop_type!r})'
