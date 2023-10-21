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

if impala_data_source:
    print(f"this is your data source name: {impala_data_source}")
    try:
        connection = odbc.connect("DSN=" + impala_data_source, autocommit=True)
    except odbc.Error as e:
        connection = None
        print("ODBC Connection error", e)
else:
    connection = None
    print("Cloudera data source not found in ODBC data sources")

def select_excel_file(conn=connection):
    root = tk.Tk()
    root.withdraw()

    file_path = filedialog.askopenfilename(title="Select the Excel file", filetypes=[("Excel files", "*.xlsx")])

    if not file_path:
        print("No file selected..")
        return

    process_excel_file(file_path, conn)
    root.destroy()

def select_excel_file_from_path(conn=connection):
    root = tk.Tk()
    root.withdraw()

    folder_path = filedialog.askdirectory(title="Select the folder containing the Excel file")

    if not folder_path:
        print("No folder selected..")
        return

    file_path = filedialog.askopenfilename(initialdir=folder_path, title="Select the Excel file",
                                           filetypes=[("Excel files", "*.xlsx")])

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
    select_excel_file_from_path()




# for value in data_tuple:
#         # Replace NaN with "NA" for string columns and 0 for integer columns
#         new_value = []
#         for val in value:
#             if isinstance(val, str):
#                 new_value.append(val if val.strip() else "NA")
#             elif isinstance(val, (int, float)) and pd.isna(val):
#                 new_value.append(0)
#             else:
#                 new_value.append(val)


# replace_dict = {
#         'int64': 0,        # Replace NaN with 0 for integer columns
#         'float64': 0.0,    # Replace NaN with 0.0 for floating-point columns
#         'object': ''       # Replace NaN with an empty string for string columns
#     }

# data_tuple = [tuple([excel_file_name] + [replace_dict.get(data_type, 'NA') if pd.isna(val) else val for val, data_type in zip(row, data_types)]) for _, row in excel_file.iterrows()]

