from datetime import datetime
import pandas as pd
import os
import sqlite3

def new_table_statement(df, table_name):
    column_names = df.columns.str.strip()
    datatypes = df.dtypes.values

    statement = 'CREATE TABLE IF NOT EXISTS ' + table_name + ' (\n'

    for idx, column_name in enumerate(column_names):
        dtype = datatypes[idx]
        statement += column_name.lower()
        if dtype == 'float64':
            statement += ' DECIMAL'
        elif dtype =='int64':
            statement += ' INTEGER'
        elif dtype == 'O':
            statement += ' VARCHAR'
        #denote primary key
        #if column_name == 'VAERS_ID':
            #statement += ' PRIMARY KEY'
        if idx != len(column_names) - 1:
            statement += ", \n"
    statement += ');'
    print(statement)
    return statement

def find_csvs(year):
    file_dir = os.path.abspath('.')
    data_folder = 'VAERS-data'
    data_year_folder = str(year) + 'VAERSData' #usual format
    path = os.path.join(file_dir, data_folder, data_year_folder)
    print(path)
    csv_list = list(filter(lambda x: '.csv' in x, os.listdir(path)))
    return csv_list, path

def make_delete_statement(table_name, year):
    statement = 'DELETE FROM ' + table_name + ' WHERE year = ' + str(year) + ';'
    return statement

def make_tables(db_name, year):
    this_year = datetime.now().year
    csv_list, path = find_csvs(year)
    conn = sqlite3.connect(db_name)
    c = conn.cursor()
    for csv_file in csv_list:
        csv_path = os.path.join(path, csv_file)
        df = pd.read_csv(csv_path, encoding_errors = 'ignore', low_memory=False)
        #making table name based on csv name
        string = csv_file.split('/')[-1].split('.')[0]
        table_name = string[9:].lower()
        year = int(string[:4])
        df['YEAR'] = year


        c.execute(new_table_statement(df, table_name))
        conn.commit()

        if this_year == year:
            #remove the rows with that year and replace with this updated csv data
            delete_statement = make_delete_statement(table_name, year)
            c.execute(delete_statement)
            conn.commit()
            print('Deleted any current data from given year!')
            #then replace
            df.to_sql(table_name, conn, if_exists = 'append', index=False)
        else:
            df.to_sql(table_name, conn, if_exists = 'append', index=False)
        conn.commit()

        print('Done! Inserted ' + table_name)
    c.close()
    conn.close()
