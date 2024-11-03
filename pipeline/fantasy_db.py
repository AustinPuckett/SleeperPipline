import sqlite3
from time import time

def get_table_column_names(conn, table_name):
    c = conn.cursor()

    # Get the column names from the table
    query = c.execute(f'''SELECT * from {table_name}''')
    column_names = query.description
    
    return [i[0] for i in column_names]

# Low Level Database Functions
def table_exists(conn, table_name):
    try:
        c = conn.cursor()
        c.execute(f'''SELECT * FROM {table_name}''')
        return True
    except:
        return False

def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """

    conn = None
    try:
        conn = sqlite3.connect(db_file)
    except:
        print("There was an error creating this connection")

    return conn

def create_table(conn, table_name, table_fields):
    '''This function creates a table in the database '''
    c = conn.cursor()
    c.execute(f'''CREATE TABLE {table_name}
                (entryId int)''')
    conn.commit()

    for field in table_fields:
        c.execute(f'''ALTER TABLE {table_name}
                        ADD COLUMN {field}''')
        conn.commit()

def delete_table(conn, table_name):
    # table_name = input('Enter the name of the table you wish to delete: ')
    c = conn.cursor()
    c.execute(f'''DROP TABLE IF EXISTS {table_name}''')
    conn.commit()

def create_table_row(conn, table_name, row_vals):
    '''This function creates a row in the given table'''
    c = conn.cursor()

    column_names = get_table_column_names(conn, table_name)
    
    # Automatically assign a primary key that is one greater than the latest entry in the table.
    primary_key = column_names[0]
    c.execute(f'''SELECT * FROM {table_name}
                    ORDER BY {primary_key} DESC 
                    LIMIT 1''')
    primary_key_query = c.fetchall()
    if primary_key_query == []:
        primary_key_val = 1
    else:
        primary_key_val = int(primary_key_query[0][0]) + 1

    row_vals = [primary_key_val] + row_vals
    
    # Create "(?, ?, ?)" like string for the row insert statement
    val_insert = ''
    for i in range (len(row_vals)):
        if i == 0:
            if len(row_vals) == 1:
                val_insert = val_insert + '(?)'
            else:
                val_insert = val_insert + '(?, '
        elif i == (len(row_vals) - 1):
            val_insert = val_insert + '?)'
        else:
            val_insert = val_insert + '?, '

    # Add row values to the table
    c.execute(f'''INSERT INTO {table_name}
                    VALUES {val_insert}''', tuple(row_vals))
    
    conn.commit()


#High Level Database Functions
def refresh_table(conn, table_name, table_data):
    '''This function replaces all existing data with the table_data matrix'''
    c = conn.cursor()
    c.execute(f'''DELETE FROM {table_name}''')
    conn.commit()
    
    for row in table_data:
        create_table_row(conn, table_name, row)
        
def get_table_data(conn, table_name):
    c = conn.cursor()

    # Get the column names from the table
    query = c.execute(f'''SELECT * from {table_name}''')
    table_data = query.fetchall()
    
    return table_data


def create_db(db_file_name):
    conn = sqlite3.connect(db_file_name)
    conn.close()

def connect_to_db(db_file_name):
    return sqlite3.connect(db_file_name)


if __name__ ==  '__main__':
    # Refresh Tables
    db_file = 'fantasy.db'
    conn = create_connection(db_file)
    table_names = []
    tables = []
    for i, j in zip(table_names, tables):
        refresh_table(conn, table_name=i, table_data=j)
    conn.close()

    # Create tables
    db_file = 'fantasy.db'
    conn = create_connection(db_file)
    table_names = []
    headers = []
    for i, j in zip(table_names, headers):
        try:
            create_table(conn, table_name=i, table_fields=j)
        except Warning:
            print(f'{i} could not be created. It may already exist in the database.')
    conn.close()
