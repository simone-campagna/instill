import base64
import gzip
import re

from . import drop


__all__ = [
    'get_inline_api',
    'get_simple_api',
    'get_tmpfile_api',
    'get_memory_api',
    'get_api',
    'default_api_implementation',
    'get_api_implementations',
]


def get_inline_api(name):
    source = drop.get('instill_api').get_content()
    import ast
    orig_module = ast.parse(source, mode='exec')
    new_module = ast.parse(f'''\
def _build_instill_namespace(name, file):
    def __build_bs(loc):  # better unparsing
        class InstillNamespace:
            pass
        
        instill_namespace = InstillNamespace
        all_names = set(loc.get('__all__', loc))
        for l_var, l_obj in loc.items():
            if l_var in all_names:
                setattr(instill_namespace, l_var, l_obj)
        return instill_namespace

    return __build_bs(locals())


{name} = _build_instill_namespace({name!r}, __file__)
''')
    # set function's body to drop module's body:
    build_ns_function = new_module.body[0]
    build_ns_function.body = orig_module.body + build_ns_function.body
    return '''\
# === instill api implementation: inline ===
''' + ast.unparse(new_module)


def get_simple_api(name):
    source = drop.get('instill_api').get_content()
    source += '''

### create locals
loc = locals()

class InstillNamespace:
    pass

instill_namespace = InstillNamespace
for l_var, l_obj in loc.items():
    setattr(instill_namespace, l_var, l_obj)

return instill_namespace
'''
    indent = '    '
    indented_lines = []
    for line in source.split('\n'):
        indented_lines.append(indent + line)
    indented_source = '\n'.join(indented_lines)
    return f'''\
# === drop api implementation: simple ===
def _build_instill_namespace(name, file):
{indented_source}

{name} = _build_instill_namespace({name!r}, __file__)
'''


def _compress_source(source):
    b_source = bytes(source, 'utf-8')
    ## set mtime to 0 to make gzip output reproducible
    data = str(base64.b85encode(gzip.compress(b_source, mtime=0)), 'utf-8')
    data_lines = ['"""']
    slen = drop.get_max_line_length()
    for idx in range(0, len(data), slen):
        data_lines.append(data[idx:idx+slen])
    data_lines[-1] += '"""'
    content = '\n'.join(data_lines)
    return f'''\
    def _get_source():
        import base64, re, gzip
        data = bytes(re.sub(r'\s', '', {content}), 'utf-8')
        return str(gzip.decompress(base64.b85decode(data)), 'utf-8')
'''


def get_tmpfile_api(name):
    source = drop.get('instill_api').get_content()
    uncompress_code = _compress_source(source)
    return f'''\
# === instill api implementation: tmpfile ===
def _load_module_from_tmpfile(name, file):
    import tempfile, atexit, shutil, sys, importlib.util
    from pathlib import Path
    tmp_path = Path(tempfile.mkdtemp())
    atexit.register(shutil.rmtree, tmp_path)
    tmp_file = tmp_path / (name + '.py')
{uncompress_code}
    source = _get_source()
    with open(tmp_file, 'w') as tmp_f:
        tmp_f.write(source)
    spec = importlib.util.spec_from_file_location(name, str(tmp_file))
    module = importlib.util.module_from_spec(spec)
    module.__file__ = file
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module

{name} = _load_module_from_tmpfile("{name}", __file__)
'''


def get_memory_api(name):
    source = drop.get('instill_api').get_content()
    uncompress_code = _compress_source(source)
    return f'''\
# === instill api implementation: memory ===
def _load_module_from_memory(name, file):
    import sys, importlib.abc, importlib.util

    class StringLoader(importlib.abc.SourceLoader):
        def __init__(self, data):
            self.data = data

        def get_source(self, fullname):
            return self.data

        def get_data(self, path):
            return self.data

        def get_filename(self, fullname):
            return f'/tmp/fake/{{fullname}}.py'

{uncompress_code}
    source = _get_source()
    spec = importlib.util.spec_from_loader(name, loader=StringLoader(source), origin='built-in')
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    module.__file__ = file
    return module

{name} = _load_module_from_memory("{name}", __file__)
'''

API_IMPLEMENTATION = {
    'simple': get_simple_api,
    'inline': get_inline_api,
    'tmpfile': get_tmpfile_api,
    'memory': get_memory_api,
}


DEFAULT_API_IMPLEMENTATION = 'memory'

def get_api(name=None, implementation=None):
    if name is None:
        name = 'drop'
    if not is_valid_modulename(name):
        raise ValueError(name)
    if implementation is None:
        implementation = DEFAULT_API_IMPLEMENTATION
    # print('-->', name, implementation)
    return API_IMPLEMENTATION[implementation](name)


def default_api_implementation():
    return DEFAULT_API_IMPLEMENTATION


def get_api_implementations():
    return list(API_IMPLEMENTATION)


def is_valid_modulename(name):
    regex = re.compile(r'[a-zA-Z0-9_]\w*')
    return bool(regex.match(name))
