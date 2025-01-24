"""
The config_json.py houses the ConfigJSON class and load_config function that loads data from the config file.
"""

import json
import os
from pipeline_logging import logger

class ConfigJSON:
    def __init__(self):
        self.config = self.load_config()

    def load_config(self, config_path):
        config_file = f'{config_path}/salesforce_config.json'
        try:
            with open(config_file, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            err_msg = f'Configuration File {config_file} not found.'
            logger.error(err_msg)
            raise FileNotFoundError(err_msg)
        except json.JSONDecodeError:
            err_msg = f'Failed to Parse JSON File: {config_file}'
            logger.error(err_msg)
            raise json.JSONDecodeError(err_msg)
        
    def get(self, key: str, default=None):
        return self.config.get(key, default)