import os
import pandas as pd
import pyodbc as odbc
import time
import tkinter as tk
from tkinter import filedialog
import pyodbc as odbc
import re

data_sources = odbc.dataSources()
impala_data_source = [i for i, v in data_sources.items() if
                      re.search("Cloudera ODBC Driver for Impala", v) and not re.search('Sample Cloudera Impala DSN',
                                                                                        i)][0]
# next((ds for ds, driver in data_sources.items() if re.search("Cloudera ODBC Driver for Impala", driver) and not re.search('Sample Cloudera Impala DSN', ds)), None)
impala_data_source
if impala_data_source:
    print(f"Data source for Cloudera Impala: {impala_data_source}")
else:
    print("Cloudera Impala data source not found in the ODBC data sources.")

if impala_data_source:
    connection = odbc.connect("DSN=" + impala_data_source, autocommit=True)
else:
    connecta = None
    print("ODBC connection error happened..")



def select_excel_file(conn=connection):
    root = tk.Tk()
    root.withdraw()

    file_path = filedialog.askopenfilename(title="Select the Excel file", filetypes=[("Excel files", "*.xlsx")])

    if not file_path:
        print("No file selected..")
        return

    process_excel_file(file_path, conn)
    root.destroy()


def process_excel_file(file_path, conn=connection):
    excel_file_name = os.path.basename(file_path)

    tablename = os.path.splitext(excel_file_name)[0]

    excel_file = pd.read_excel(file_path)
    columns = excel_file.columns
    data_tuple = [tuple([excel_file_name] + list(t)) for t in excel_file.to_numpy()]

    c = conn.cursor()

    create_table_sql = f"CREATE TABLE IF NOT EXISTS db.{tablename} (file_name STRING, "
    for column in columns:
        data_type = excel_file[column].dtype
        if data_type == 'int64':
            column_type = 'INT'
        elif data_type == 'float64':
            column_type = 'DOUBLE'
        else:
            column_type = 'STRING'
        create_table_sql += f"{column} {column_type}, "
    create_table_sql = create_table_sql.rstrip(", ") + ")"

    c.execute(create_table_sql)

    query_insert = f"INSERT INTO db.{tablename} VALUES ({', '.join(['?'] * (len(columns) + 1))})"

    print("Processing started...")
    start_time = time.time()

    for value in data_tuple:
        c.execute(query_insert, value)

    end_time = time.time()

    print("Query took {} seconds".format(end_time - start_time))

    conn.commit()
    conn.close()


if __name__ == "__main__":
    select_excel_file()
