import contextlib
import logging
import logging.config
import sys

__all__ = [
    'LOG',
    'get_log',
    'configure_logging',
    'set_trace',
    'get_trace',
    'trace_errors',
]


LOG = logging.getLogger(__name__)
TRACE = False


def get_log(self):
    return LOG


def configure_logging(verbose):
    if verbose >= 3:
        log_level = 'DEBUG'
    elif verbose >= 2:
        log_level = 'INFO'
    elif verbose >= 1:
        log_level = 'WARNING'
    else:
        log_level = 'ERROR'
    logging.config.dictConfig({
        'version': 1,
        'formatters': {
            'standard': {
                'format': '# %(levelname)-10s %(message)s',
                'datefmt': '%Y%m%d %H:%M:%S',
            }
        },
        'handlers': {
            'console': {
                'class': 'logging.StreamHandler',
                'formatter': 'standard',
            }
        },
        'loggers': {
            __name__: {
                'level': log_level,
                'handlers': ['console'],
            },
        },
        'root': {
            'level': log_level,
            'handlers': [], #'console'],
        },
    })


def get_trace(trace):
    return TRACE


def set_trace(trace):
    global TRACE
    TRACE = trace


@contextlib.contextmanager
def trace_errors(message, on_error='raise'):
    try:
        yield
    except Exception as err:
        if TRACE:
            fn = LOG.exception
        else:
            fn = LOG.error
        fn(f'{message}: {type(err).__name__}: {err}')
        if on_error == 'raise':
            raise
        elif on_error == 'exit':
            sys.exit(1)
