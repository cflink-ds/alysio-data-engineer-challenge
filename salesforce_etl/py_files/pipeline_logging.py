'''
pipeline_logging.py contains the setup for info and error logging for the pipeline.
'''

import logging.config
import os
import sys
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.absolute()
LOGS_DIR = Path(__file__).parent.parent.absolute() / 'runtime_logs'
DB_PATH = Path(__file__).parent.parent.absolute() / 'database' / 'SalesforceData.db'
PCKG_NAME = os.path.basename(ROOT_DIR)

logging_config = {

    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "minimal": {"format": f'%(asctime)s, "{PCKG_NAME}", "%(message)s"',
                    "datefmt": "%Y-%m-%d %H:%M:%S"},
        "detailed": {"format": "%(levelname)s %(asctime)s [%(name)s:$(filename)s:%(funcName)s:%(lineno)d]\n%(message)s\n",
                     "datefmt": "%Y-%m-%d %H:%M:%S"},
        },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": logging.DEBUG,
            "formatter": "minimal",
            "stream": sys.stdout,
        },
        "info": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": Path(LOGS_DIR, "info.log"),
            "maxBytes": 10485760, # 1 MB size limit
            "backupCount": 5, # Purge after 5 backups
            "formatter": "detailed",
            "level": logging.INFO,
        },
        "error": {
            "class": "logging.handlers.RotatingFileHandler",
            "filename": Path(LOGS_DIR, "error.log"),
            "maxBytes": 10485760, # 1 MB size limit
            "backupCount": 5, # Purge after 5 backups
            "formatter": "minimal",
            "level": logging.ERROR,
        },
    },
    "root": {
        "handlers": ["console", "info", "error"],
        "level": logging.INFO,
        "propagate": True,
    },
}

logging.config.dictConfig(logging_config)
logger = logging.getLogger()
