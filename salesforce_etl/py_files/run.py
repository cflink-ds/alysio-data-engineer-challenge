"""
Purpose: This script is used to run the ETL process for Salesforce data.

Revisions: 01/23/2025 C Flink - Initial version
"""
from pipeline_logging import logger

## Try all import and log failures
try:
    import sys
    import pandas as pd
    import sqlite3
    import os
    import numpy as np
    import json
    from sql_schema_creation import create_schema
    from pathlib import Path
    logger.info('All imports successful.')
except ImportError as e:
    logger.error(f'Import error: {e}')
    sys.exit(1)

""" ## Uncomment the following code block and run the script to create the tables in the SalesforceData.db database
try:
    # Obtain db file path
    db_path = Path(__file__).parent.parent.parent.absolute() / 'SalesforceData.db'

    # Obtain schema file path
    schema_path = Path(__file__).parent.parent.parent.absolute() / 'schema' / 'init.sql'

    # Get init.sql schema
    with open(schema_path, 'r') as file:
        schema = file.read()
    logger.info('Schema file read successfully.')
except Exception as e:
    logger.error(f'Error reading schema file: {e}')
    sys.exit(1)

try:
    create_schema(db_path=db_path, schema=schema)
    logger.info('Schema created successfully.')
except Exception as e:
    logger.error(f'Error creating schema: {e}')
    sys.exit(1) """

def main() -> int:
    config_path = Path(__file__).parent.parent.absolute() / 'config'

##TODO: Add code to read the config file, get data from sources into DF's, do next part of requirements

if __name__ == "__main__":
    status, log_type, message = main()
    if status == 1 and log_type == 'error':
        logger.error(message)
    logger.info('ETL process completed.')
    sys.exit(status)

