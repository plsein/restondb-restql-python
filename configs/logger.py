# import sys
from logger_tt import setup_logging
# import logging
from logging import getLogger
from logging.config import dictConfig

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "default": {
         'format': '%(asctime)s — %(name)s — %(levelname)s - %(processName)s - %(threadName)s in %(pathname)s at %(lineno)d: %(message)s',
         "datefmt": "%Y-%m-%d %H:%M:%S"
        }
    },

    "handlers": {
        "console": {
         "class": "logging.StreamHandler",
         "level": "DEBUG",
         "formatter": "default",
         "stream": "ext://sys.stdout"
        },
        "file": {
            "class": "logging.handlers.TimedRotatingFileHandler",
            "level": "DEBUG",
            "formatter": "default",
            "filename": "logs/app.log",
            "backupCount": 10,
            "encoding": "utf8",
            "when": "midnight",
            # "when": "d",
            # 'interval': 1,
            'delay': True
        },
    },

    "loggers": {
        "urllib3": {
            "level": "ERROR",
            "handlers": ["file"],
            "propagate": False
        },
        'requests': {
            'level': 'WARNING',
            'handlers': ['file'],
            'propagate': False
        },
        'aiohttp': {
            'level': 'WARNING',
            'handlers': ['file'],
            'propagate': False
        },
        'httpx': {
            'level': 'WARNING',
            'handlers': ['file'],
            'propagate': False
        },
        'gql': {
            'level': 'WARNING',
            'handlers': ['file'],
            'propagate': False
        },
        'asyncio': {
            'level': 'WARNING',
            'handlers': ['file'],
            'propagate': False
        }
     },

    "root": {
        "level": "DEBUG",
        "handlers": ["console", "file"]
    },

    "logger_tt": {
        "suppress": ["exchangelib", "urllib3", 'aiohttp', 'httpx'],
        "suppress_level_below": "WARNING",
        "capture_print": False,
        "strict": False,
        "guess_level": False,
        "full_context": 0,
        "use_multiprocessing": 'fork',
        # "limit_line_length": 1000,
        "analyze_raise_statement": False,
        "default_logger_formats": {
          "normal": ["%(name)s", "%(filename)s"],
          "thread": ["%(message)s", "%(threadName)s %(message)s"],
          "multiprocess": ["%(message)s", "%(processName)s %(message)s"],
          "both": ["%(message)s", "%(processName)s %(threadName)s %(message)s"]
        }
    }
}

dictConfig(LOGGING)

# setup_logging(
#     port=0,
#     capture_print=False,
#     strict=False,
#     guess_level=False,
#     full_context=False,
#     use_multiprocessing='fork',
#     # log_path={ 'file': 'logs/app.log' },
#     analyze_raise_statement=False
# )

log = getLogger('applog')
# log.setLevel(logging.WARNING)
# log_format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
# ch = logging.StreamHandler(sys.stdout)
# ch.setFormatter(log_format)
# log.addHandler(ch)
