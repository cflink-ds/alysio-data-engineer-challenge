import sqlite3

def create_schema(db_path, schema):
    """
    Creates the schema for the database in SQLite.

    Args:
        db_path (str): The path to the SQLite database.
    """
    # Connect to the database
    conn = sqlite3.connect(db_path)
    # Create a cursor object
    cursor = conn.cursor()
    # Execute the schema
    try:
        cursor.executescript(schema)
        conn.commit()
        print('Schema created successfully.')
    except sqlite3.Error as e:
        print(f'Error creating schema: {e}')
    finally:
        conn.close()

