"""
The data.py contains the DataLoader class and it's functions to manipulate and load data.
"""

import pandas as pd
import re
import sqlite3
from pipeline_logging import logger
from typing import List

class DataLoader:
    def __init__(self, conn: sqlite3.connect):
        try:
            self.conn = conn
            logger.info('SQLite connection successful.')
        except Exception as e:
            logger.error(f'Error connecting to SQLite DB: {e}')
            raise e

    def validate_sql_mapping(self, import_table: str, mapping_keys: List[str]) -> bool:
        """
        Validates the mapping of column names in a SQLite database.

        Args:
            import_table (str): The name of the table in the database.
            mapping_keys (List[str]): The list of column names as expected in the database.

        Returns:
            bool: True if mapping_keys is a subset of import_table column names, False otherwise.
        """
        try:
            import_table_column_names = pd.read_sql(sql=f'SELECT * FROM {import_table} WHERE 1=0', con=self.conn)
            import_table_column_names = set(import_table_column_names.columns)
        except Exception as e:
            logger.error(f'Cannot get SQLite column names with error: {e}')
            return False
        else:
            if not set(mapping_keys).issubset(import_table_column_names):
                logger.error('Invalid SQLite mapping. Double check column names.')
                return False
            else:
                logger.info('SQLite mapping is valid.')
                return True

    def validate_source_data_mapping(self, found_colnames: List[str], expected_mapping_values: List[str]) -> List[int] | bool:
        """
        Validates the mapping of column names in a file.

        Args:
            found_colnames (list[str]): The list of column names found in the source.
            expected_mapping_values (list[str]): The list of column names as expected in the source.

        Returns:
            list[int] | bool: If the mapping is valid, returns a list of indices of the columns.
            If the mapping is invalid, returns False.
        """
        try:
            if not set(expected_mapping_values).issubset(set(found_colnames)):
                logger.error('Column names do not match expected values.')
                return False

            # Get the column_intersection of colnames
            column_intersection = [colname for colname in found_colnames if colname in expected_mapping_values]
            # Get the order of the columns in column_intersecion (used to reorder columns on import to match cfg_column_mapping)
            csv_column_order = [found_colnames.index(colname) for colname in column_intersection]
            return csv_column_order
        except Exception as e:
            logger.error(f'Error validating csv data mapping: {e}')
            raise e

    def source_to_df(self, file_path: str, file_extension: str, **kwargs) -> pd.DataFrame:
        """
        Extracts data from a file and returns a DataFrame.

        Args:
            file_path (str): The path to the source file.
            file_extension (str): The extension of the source file.
            **kwargs: Additional keyword arguments to pass to the read function.

        Returns:
            pd.DataFrame: The DataFrame containing the data from the file.

        Raises:
            Exception: If there is an error reading the file.
        """
        if file_extension not in ['csv', 'json']:
            raise Exception(f'File extension {file_extension} not supported.')
        try:
            if file_extension == 'csv':
                file_data : pd.DataFrame = pd.read_csv(file_path, **kwargs)
            elif file_extension == 'json':
                file_data : pd.DataFrame = pd.read_json(file_path, **kwargs)
            logger.info(f'Data extracted from {file_path}.')
            return file_data
        except Exception as e:
            logger.error(f'Error reading {file_extension} file: {file_path} with error: {e}')
            raise e
        
    def standardize_text_cols(self, df: pd.DataFrame, text_cols: List[str]) -> pd.DataFrame:
        """
        Standardizes text columns in a DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to standardize.
            text_cols (List[str]): The list of text columns to standardize.

        Returns:
            pd.DataFrame: The DataFrame with standardized text columns.
        """
        try:
            for col in text_cols:
                df[col] = df[col].str.strip().str.lower().str.title()
            return df
        except Exception as e:
            logger.error(f'Error standardizing text columns of {df}: {e}')
            raise e
        
    def standardize_date_cols(self, df: pd.DataFrame, date_cols: List[str]) -> pd.DataFrame:
        """
        Standardizes date columns in a DataFrame.

        Args:
            df (pd.DataFrame): The DataFrame to standardize.
            date_cols (List[str]): The list of date columns to standardize.

        Returns:
            pd.DataFrame: The DataFrame with standardized date columns.
        """
        try:
            for col in date_cols:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            return df
        except Exception as e:
            logger.error(f'Error standardizing date columns of {df}: {e}')
            raise e
        
    def normalize_phone_numbers(self, phone_column):
        """
        Normalizes phone numbers to the format: +1-555-123-4567.

        Args:
            phone_column (pd.Series): The column containing phone numbers.

        Returns:
            pd.Series: Normalized phone numbers.
        """
        try:
            def format_phone(phone):
                if pd.isnull(phone):
                    return phone
                # Remove all non-numeric characters except "+"
                digits = re.sub(r"[^\d+]", "", phone)
                # Standardize to +1-XXX-XXX-XXXX if it starts with a country code or add default country code
                if digits.startswith("+"):
                    return re.sub(r"(\+\d{1})(\d{3})(\d{3})(\d{4})", r"\1-\2-\3-\4", digits)
                elif len(digits) == 10: # Assume default country code +1 for 10-digit numbers
                    return "+1-" + re.sub(r"(\d{3})(\d{3})(\d{4})", r"\1-\2-\3", digits)
                return phone # Return as is if formatting fails
            
            return phone_column.apply(format_phone)
        except Exception as e:
            logger.error(f'Error normalizing phone numbers: {e}')
            raise e

    def normalize_email_addresses(self, email_column):
        """
        Normalizes email addresses to lowercase and removes leading/trailing spaces.

        Args:
            email_column (pd.Series): The column containing email addresses.

        Returns:
            pd.Series: Normalized email addresses.
        """
        try:
            return email_column.str.strip().str.lower()
        except Exception as e:
            logger.error(f'Error normalizing email addresses: {e}')
            raise e
        
    def remove_duplicate_contacts(self, contacts_df, activities_df, opportunities_df):
        """
        Removes duplicate contacts while preserving relationship integrity. Also validates
        the referential integrity of the data before returning Tuple of DataFrames.

        Args:
            contacts_df (pd.DataFrame): The contacts DataFrame.
            activities_df (pd.DataFrame): The activities DataFrame.
            opportunities_df (pd.DataFrame): The opportunities DataFrame.

        Returns:
            tuple: Updated DataFrames (contacts_df, activities_df, opportunities_df).
        """        
        try:
            # Step 1: Identify duplicate contacts based on email
            # Sort by last_modified (keeping the most recent record)
            contacts_df = contacts_df.sort_values(by=['email', 'last_modified'], ascending=[True, False])
            
            # Remove duplicates, keeping the first (most recent) record for each email
            deduplicated_contacts = contacts_df.drop_duplicates(subset=['email'], keep='first')

            # Step 2: Map old contact IDs to retained contact IDs
            id_mapping = contacts_df.set_index('id')['email'].to_dict()
            email_to_new_id = deduplicated_contacts.set_index('email')['id'].to_dict()
            id_remap = {old_id: email_to_new_id[email] for old_id, email in id_mapping.items() if email in email_to_new_id}

            # Step 3: Update relationships in activities and opportunities
            activities_df['contact_id'] = activities_df['contact_id'].replace(id_remap)
            opportunities_df['contact_id'] = opportunities_df['contact_id'].replace(id_remap)

            # Step 4: Validate referential integrity
            orphaned_activities = activities_df[~activities_df['contact_id'].isin(deduplicated_contacts['id'])]
            orphaned_opportunities = opportunities_df[~opportunities_df['contact_id'].isin(deduplicated_contacts['id'])]

            if orphaned_activities.empty and orphaned_opportunities.empty:
                logger.info('Referential integrity validated.')
                return deduplicated_contacts, activities_df, opportunities_df
            else:
                raise Exception('Referential integrity violated.')
        except Exception as e:
            logger.error(f'Error removing duplicate contacts: {e}')
            raise e
    
    def find_oppID_for_act_null(self, df_activities: pd.DataFrame, df_opportunities: pd.DataFrame) -> pd.DataFrame:
        """
        Finds the opportunity ID for activities with null opportunity ID by matching contact_id between the
        activities df and opportunities df and ensuring the timestamp of the activity is between the created_date
        and closed_date of the opportunity.

        Args:
            df_activities (pd.DataFrame): The activities DataFrame.
            df_opportunities (pd.DataFrame): The opportunities DataFrame.

        Returns:
            pd.DataFrame: The activities DataFrame with opportunity IDs filled in.
        """
        try:
            # Ensure timestamp and date fields are in datetime format
            df_activities['timestamp'] = pd.to_datetime(df_activities['timestamp'])
            df_opportunities['created_date'] = pd.to_datetime(df_opportunities['created_date'])
            df_opportunities['close_date'] = pd.to_datetime(df_opportunities['close_date'])

            # Filter activities where `opportunity_id` is None
            null_opportunity_activities = df_activities[df_activities['opportunity_id'].isna()].copy()

            # Perform the matching
            def match_opportunity(row):
                matches = df_opportunities[
                    (df_opportunities['contact_id'] == row['contact_id']) &
                    (df_opportunities['created_date'] <= row['timestamp']) &
                    (df_opportunities['close_date'] >= row['timestamp'])
                ]
                return matches['id'].iloc[0] if not matches.empty else None

            # Apply the matching logic to the filtered activities
            null_opportunity_activities['opportunity_id'] = null_opportunity_activities.apply(match_opportunity, axis=1)

            # Update the original activities DataFrame
            df_activities.update(null_opportunity_activities)
            return df_activities
        except Exception as e:
            logger.error(f'Error finding opportunity IDs for activities with null opportunity ID: {e}')
            raise e
        
    def validate_opportunity_dates(self, opportunities_df):
        """
        Validates that no `created_date` in the opportunities DataFrame is after `close_date`.

        Args:
            opportunities_df (pd.DataFrame): The opportunities DataFrame.

        Returns:
            pd.DataFrame: A DataFrame containing invalid rows where `created_date` > `close_date`.
        """
        try:
            # Ensure the dates are in datetime format
            opportunities_df['created_date'] = pd.to_datetime(opportunities_df['created_date'])
            opportunities_df['close_date'] = pd.to_datetime(opportunities_df['close_date'])

            # Filter rows where created_date comes after close_date
            invalid_rows = opportunities_df[opportunities_df['created_date'] > opportunities_df['close_date']]

            return invalid_rows
        except Exception as e:
            logger.error(f'Error validating opportunity dates: {e}')
            raise e
    
    def truncate_sqlite_tables(self, table_names: List[str]):
        """
        Truncates tables in an SQLite database.

        Args:
            table_names (List[str]): A list of table names to truncate.
        """
        conn = self.conn
        cursor = conn.cursor()
        try:
            for table_name in table_names:
                cursor.execute(f'DELETE FROM {table_name};')
                conn.commit()
                cursor.execute(f'VACUUM;')
                logger.info(f'Table {table_name} truncated successfully.')
        except sqlite3.Error as e:
            logger.error(f'Error truncating tables: {e}')
            raise e

    def load_df_to_sqlite(self, table_data):
        """
        Loads multiple pandas DataFrames into an SQLite database.

        Args:
            conn (sqlite3.connect): The connection to the SQLite database.
            table_data (dict): A dictionary where keys are table names and values are DataFrames.
        """
        conn = self.conn
        try:
            for table_name, df in table_data.items():
                if isinstance(df, pd.DataFrame): # Check if value is a DataFrame
                    # Load DataFrame into the specified table
                    df.to_sql(table_name, conn, if_exists='append', index=False)
                    logger.info(f"Table '{table_name}' loaded successfully.")
                else:
                    logger.error(f"Error: The value for table '{table_name}' is not a DataFrame.")
        except Exception as e:
            logger.error(f"An error occurred: {e}")
            raise e
    
    def incremental_update(self, new_data : pd.DataFrame, table_name : str, key : str, last_modified_field : str = None):
        """
        Perform an incremental update on existing data with new data.

        Args:
            new_data (pd.DataFrame): Incoming DataFrame with new or updated records.
            table_name (str): Name of the table in the database that houses the existing data.
            key (str): Column name to use as the unique identifier for matching.
            last_modified_field (str, optional): Field name to determine the most recent changes.
        
        Returns:
            pd.DataFrame: Updated DataFrame with new and modified records.
        """
        try:
            # Get existing data from the database
            existing_data = pd.read_sql(f'SELECT * FROM {table_name}', self.conn)
            existing_data = existing_data.drop('etl_timestamp', axis=1)
            # Merge new data with existing data based on the key
            merged_data = pd.merge(existing_data, new_data, on=key, how='outer', suffixes=('_existing', '_new'))
            # Determine which records to keep from new data
            if last_modified_field:
                merged_data[last_modified_field + '_new'] = pd.to_datetime(merged_data[last_modified_field + '_new'])
                merged_data[last_modified_field + '_existing'] = pd.to_datetime(merged_data[last_modified_field + '_existing'])

                # Use new data if it has a later last_modified timestamp
                merged_data['use_new'] = (
                    merged_data[last_modified_field + '_new'] > merged_data[last_modified_field + '_existing']
                ) | merged_data[last_modified_field + '_existing'].isna()
            else:
                # If no last_modified field, prefer new data for non-matching rows
                merged_data['use_new'] = merged_data.iloc[:,1].isna()                
            # Apply updates: combine existing and new records
            merged_data_new = merged_data.loc[merged_data['use_new']==True]
            updated_data = merged_data_new.filter(regex='^id|_new$').rename(columns=lambda x: x.replace('_new', ''))
            merged_data_old = merged_data.loc[merged_data['use_new']==False]
            unchanged_data = merged_data_old.filter(regex='^id|_existing$').rename(columns=lambda x: x.replace('_existing', ''))
            return pd.concat([updated_data, unchanged_data], ignore_index=True)
        except Exception as e:
            logger.error(f'Error performing incremental update: {e}')
            raise e
        
    def close_conn(self):
        """
        Closes the connection to the SQLite database.
        """
        self.conn.close()
        logger.info('SQLite connection closed.')