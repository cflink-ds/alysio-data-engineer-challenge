"""
Purpose: This script is used to run the ETL process for Salesforce data.

Revisions: 01/23/2025 C Flink - Initial version
"""
from pipeline_logging import logger, DB_PATH

## Try all import and log failures
try:
    import pandas as pd
    import sqlite3
    import sys
    from config_json import ConfigJSON
    from data import DataLoader
    from pathlib import Path  
    logger.info('All imports successful.')
except ImportError as e:
    logger.error(f'Import error: {e}')
    sys.exit(1)

""" ## Uncomment the following code block and run the script to create the tables in the SalesforceData.db database
try:
    # Import create_schema function
    from sql_schema_creation import create_schema

    # Obtain schema file path
    schema_path = Path(__file__).parent.parent.absolute() / 'config' / 'schema' / 'init.sql'

    # Get init.sql schema
    with open(schema_path, 'r') as file:
        schema = file.read()
    logger.info('Schema file read successfully.')
except Exception as e:
    logger.error(f'Error reading schema file: {e}')
    sys.exit(1)

try:
    create_schema(db_path=DB_PATH, schema=schema)
    logger.info('Schema created successfully.')
except Exception as e:
    logger.error(f'Error creating schema: {e}')
    sys.exit(1) """

def main() -> int:
    error_message : str = ''
    try:
        config_path = Path(__file__).parent.parent.absolute() / 'config'
        config_file = ConfigJSON(config_path)
        cfg_data_path : str = config_file.get('data_source_path')
        cfg_data_files : dict = config_file.get('data_source_files')
        cfg_act_txt_cols : list = config_file.get('activities_textCols')
        cfg_act_dt_cols : list = config_file.get('activities_dateCols')
        cfg_comp_txt_cols : list = config_file.get('companies_textCols')
        cfg_comp_dt_cols : list = config_file.get('companies_dateCols')
        cfg_cont_txt_cols : list = config_file.get('contacts_textCols')
        cfg_cont_dt_cols : list = config_file.get('contacts_dateCols')
        cfg_opps_txt_cols : list = config_file.get('opportunities_textCols')
        cfg_opps_dt_cols : list = config_file.get('opportunities_dateCols')
        cfg_tbl_names : list = config_file.get('table_names')
        logger.info('ConfigJSON read config_file without error.')
    except Exception as e:
        error_message = f'Cannot initialize JSON configuration: {e}'
        return 1, 'error', error_message
    
    try:
        data_loader = DataLoader(conn = sqlite3.connect(DB_PATH))
        logger.info('SQLite connection successful.')
    except Exception as e:
        error_message = f'Cannot connect to SQLite database: {e}'
        return 1, 'error', error_message
    
    try:
        listFileName = []
        listFile = []
        listFileExtensions = []
        file_listMapping = cfg_data_files

        for key, value in file_listMapping.items():
            listFileName.append(key)
            listFile.append(value)
            listFileExtensions.append(value.split('.')[-1])        

    except Exception as e:
        error_message = f'Cannot get list of files: {e}'
        return 1, 'error', error_message
        
    try:
        dfs = {}
        for i in range(len(listFileName)):
            file_name = listFileName[i]
            file_path = cfg_data_path + listFile[i]
            file_extension = listFileExtensions[i]
            df = data_loader.source_to_df(file_path, file_extension)
            df_name = f'df_{file_name}'
            dfs[df_name] = df
            logger.info(f'DataFrame created for {listFileName[i]}.')
        df_activities = dfs['df_activities']
        df_companies = dfs['df_companies']
        df_contacts = dfs['df_contacts']
        df_opportunities = dfs['df_opportunities']
    except Exception as e:
        error_message = f'Cannot create DataFrames: {e}'
        return 1, 'error', error_message

    tbl_data = {'activities': df_activities,
                'companies': df_companies,
                'contacts': df_contacts,
                'opportunities': df_opportunities}

    try:
        for key, val in tbl_data.items():
            df = val
            columns_to_keep = data_loader.validate_source_data_mapping(found_colnames=df.columns.tolist(), 
                                                                       expected_mapping_values=config_file.get(f'{key}_cols'))
            if not columns_to_keep:
                error_message = f'The script found that the column names in the source file: {key} are not a subset of the column names in the config file.'
                return 1, 'error', error_message
            else:
                retained_file_data : pd.DataFrame = df.iloc[:, columns_to_keep]
                df = retained_file_data
    except Exception as e:
        error_message = f'Unable to validate source {e}'
        return 1, 'error', error_message

    for table in cfg_tbl_names:
        if not data_loader.validate_sql_mapping(import_table=table,mapping_keys=config_file.get(f'{table}_cols')):
            error_message = f'The script found that the column names in the config mapping for {table} are not a subset of the column names in the SQLite server.'
            return 1, 'error', error_message
    logger.info('SQLite tables validated against the column mapping in the Config file.')

    try:
        df_activities = data_loader.standardize_text_cols(df=df_activities, text_cols=cfg_act_txt_cols)
        df_activities = data_loader.standardize_date_cols(df=df_activities, date_cols=cfg_act_dt_cols)
        logger.info('Activities DF standardized.')
    except Exception as e:
        error_message = f'Cannot standardize activities DataFrame: {e}'
        return 1, 'error', error_message

    try:
        df_companies = data_loader.standardize_text_cols(df=df_companies, text_cols=cfg_comp_txt_cols)
        df_companies = data_loader.standardize_date_cols(df=df_companies, date_cols=cfg_comp_dt_cols)
        logger.info('Companies DF standardized.')
    except Exception as e:
        error_message = f'Cannot standardize activities DataFrame: {e}'
        return 1, 'error', error_message
    
    try:
        df_contacts = data_loader.standardize_text_cols(df=df_contacts, text_cols=cfg_cont_txt_cols)
        df_contacts = data_loader.standardize_date_cols(df=df_contacts, date_cols=cfg_cont_dt_cols)
        logger.info('Contacts DF standardized.')
    except Exception as e:
        error_message = f'Cannot standardize activities DataFrame: {e}'
        return 1, 'error', error_message
    
    try:
        df_opportunities = data_loader.standardize_text_cols(df=df_opportunities, text_cols=cfg_opps_txt_cols)
        df_opportunities = data_loader.standardize_date_cols(df=df_opportunities, date_cols=cfg_opps_dt_cols)
        logger.info('Opportunities DF standardized.')
    except Exception as e:
        error_message = f'Cannot standardize activities DataFrame: {e}'
        return 1, 'error', error_message

    try:
        df_contacts['phone'] = data_loader.normalize_phone_numbers(df_contacts['phone'])
        logger.info('Phone numbers normalized.')
    except Exception as e:
        error_message = f'Cannot normalize phone numbers: {e}'
        return 1, 'error', error_message
    
    try:
        df_contacts['email'] = data_loader.normalize_email_addresses(df_contacts['email'])
        logger.info('Email addresses normalized.')
    except Exception as e:
        error_message = f'Cannot normalize email addresses: {e}'
        return 1, 'error', error_message

    try:
        df_contacts, df_activities, df_opportunities = data_loader.remove_duplicate_contacts(contacts_df=df_contacts,
                                                                                             activities_df=df_activities,
                                                                                             opportunities_df=df_opportunities)        
    except Exception as e:
        error_message = f'Cannot remove duplicate contacts: {e}'
        return 1, 'error', error_message

    try:
        df_activities = data_loader.find_oppID_for_act_null(df_activities, df_opportunities)
        logger.info('Opportunity IDs added to df_activities.')
    except Exception as e:
        error_message = f'Cannot add Opportunity IDs to Activities: {e}'
        return 1, 'error', error_message
    
    try:
        invalid_date_ranges = data_loader.validate_opportunity_dates(df_opportunities)
        if len(invalid_date_ranges) == 0:
            logger.info('Opportunity dates validated.')
        else:
            error_message = 'Opportunity dates are Invalid'
            return 1, 'error', error_message
    except Exception as e:
        error_message = f'Cannot validate opportunity dates: {e}'
        return 1, 'error', error_message
    
    

    try:
        new_dfs = {}
        for key, val in tbl_data.items():
            table_name = key
            new_data = val
            if table_name == 'contacts':
                last_mod = 'last_modified'
            else:
                last_mod = None
            df = data_loader.incremental_update(new_data = new_data, table_name = table_name, key = 'id', last_modified_field = last_mod)
            df_name = f'df_{table_name}'
            new_dfs[df_name] = df
        df_activities = dfs['df_activities']
        df_companies = dfs['df_companies']
        df_contacts = dfs['df_contacts']
        df_opportunities = dfs['df_opportunities']
        logger.info('DataFrames created for incremental update.')
    except Exception as e:
        error_message = f'Cannot create DataFrames: {e}'
        return 1, 'error', error_message

    try:
        data_loader.truncate_sqlite_tables(cfg_tbl_names)
    except Exception as e:
        error_message = f'Cannot truncate tables: {e}'
        return 1, 'error', error_message

    try:
        data_loader.load_df_to_sqlite(tbl_data)
    except Exception as e:
        error_message = f'Cannot load DataFrames to SQLite: {e}'
        return 1, 'error', error_message
    
    data_loader.close_conn()
    return 0, 'info', 'ETL process completed successfully.'

if __name__ == "__main__":
    status, log_type, message = main()
    if status == 1 and log_type == 'error':
        logger.error(message)
    logger.info('ETL process completed.')
    sys.exit(status)

