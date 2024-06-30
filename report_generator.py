import pandas as pd
import pyodbc
from getpass import getpass
import sys
import re
import numpy as np

def connect_to_db(server, database, auth_type, username=None, password=None):
    try:
        if auth_type == 'windows':
            conn_str = (
                f"DRIVER={{SQL Server}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"Trusted_Connection=yes;"
            )
        else:
            conn_str = (
                f"DRIVER={{SQL Server}};"
                f"SERVER={server};"
                f"DATABASE={database};"
                f"UID={username};"
                f"PWD={password};"
            )
        conn = pyodbc.connect(conn_str)
        return conn
    except pyodbc.Error as e:
        print(f"Error connecting to database: {e}")
        sys.exit(1)

def validate_query(query, database):
    pattern = re.compile(rf"\b{database}\b", re.IGNORECASE)
    if not pattern.search(query):
        print(f"Error: Your query references a different database. Please use only the '{database}' database.")
        sys.exit(1)

def execute_query(conn, query):
    try:
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        print(f"Error executing query: {e}")
        sys.exit(1)

def group_by_report(df, group_by_columns, aggregation_columns, aggregation_funcs):
    grouped_df = df.groupby(group_by_columns, dropna=False)[aggregation_columns].agg(aggregation_funcs)
    return grouped_df

def case_report_multiple_conditions(df, conditions_dict):
    if not conditions_dict:
        return df

    result_df = df.copy()
    filter_mask = np.ones(len(df), dtype=bool)

    for col, condition_map in conditions_dict.items():
        condition_list = []
        result_list = []
        
        for value, result in condition_map.items():
            if value.startswith('>='):
                condition = result_df[col].astype(float) >= float(value[2:])
            elif value.startswith('<='):
                condition = result_df[col].astype(float) <= float(value[2:])
            elif value.startswith('<'):
                condition = result_df[col].astype(float) < float(value[1:])
            elif value.startswith('>'):
                condition = result_df[col].astype(float) > float(value[1:])
            else:
                condition = result_df[col] == value
            condition_list.append(condition)
            result_list.append(result)
        
        result_df[col] = np.select(condition_list, result_list, default=result_df[col])
        filter_mask = filter_mask & np.any(condition_list, axis=0)

    result_df = result_df[filter_mask]
    return result_df

def pivot_report(df, index, columns, values):
    pivot_df = df.pivot_table(index=index, columns=columns, values=values, aggfunc='sum')
    pivot_df.reset_index(inplace=True)
    return pivot_df

def unpivot_report(df, id_vars, value_vars):
    unpivoted_df = pd.melt(df, id_vars=id_vars, value_vars=value_vars)
    return unpivoted_df

def unpivot_report_from_pivot(df, id_vars, value_vars):
    # Convert value_vars to list of columns from the pivot result
    id_vars = [col for col in id_vars if col in df.columns]
    unpivot_df = df.melt(id_vars=id_vars, value_vars=value_vars)
    return unpivot_df

def export_to_excel(report_df, filename):
    try:
        # Check if the filename ends with '.xlsx'
        if not filename.endswith('.xlsx'):
            raise ValueError("Invalid file format. File name should end with '.xlsx'")
        
        with pd.ExcelWriter(filename, engine='openpyxl') as writer:
            report_df.to_excel(writer, sheet_name='Generated Report', index=True)
        
        print(f"Report exported to {filename}")
    except ValueError as ve:
        print(f"Error exporting to Excel: {ve}")
    except Exception as e:
        print(f"Unexpected error exporting to Excel: {e}")

def main():
    server = input("Enter SQL Server name: ")
    database = input("Enter database name: ")
    
    auth_type = input("Enter authentication type (windows/sql): ").strip().lower()
    if auth_type not in ['windows', 'sql']:
        print("Invalid authentication type!")
        sys.exit(1)
    
    if auth_type == 'sql':
        username = input("Enter SQL Server username: ")
        password = getpass("Enter SQL Server password: ")
        conn = connect_to_db(server, database, auth_type, username, password)
    else:
        conn = connect_to_db(server, database, auth_type)
    
    query = input("Enter your SQL query: ")
    validate_query(query, database)
    df = execute_query(conn, query)
    
    print("Original DataFrame:")
    print(df)
    
    original_df = df.copy()
    
    print("Select the report type: ")
    print("1. Group By Report")
    print("2. Case Report")
    print("3. Pivot Report")
    print("4. Unpivot Report")
    report_type = int(input("Enter the number of report type: "))
    
    if report_type == 1:
        group_by_columns = input("Enter columns to group by (comma separated): ").split(',')
        aggregation_columns = input("Enter columns to aggregate (comma separated): ").split(',')
        aggregation_funcs = input("Enter aggregation functions for each column (comma separated, e.g., sum,mean): ").split(',')
        aggregation_funcs = {col: func for col, func in zip(aggregation_columns, aggregation_funcs)}
        report_df = group_by_report(df, group_by_columns, aggregation_columns, aggregation_funcs)
        print("Generated Report:")
        print(report_df)
        export_filename = input("Enter the filename to export the report (include the '.xlsx' extension): ")
        export_to_excel(report_df, export_filename)
    
    elif report_type == 2:
        condition_columns = input("Enter columns for conditions (comma separated): ").split(',')
        conditions_dict = {}
        for col in condition_columns:
            print(f"Enter conditions for column '{col}' in the format 'value:result'")
            print("(e.g., 'Black:Dark,Silver:Light,>500:High,<=500:Low')")
            conditions = input(f"Enter conditions for {col} (comma separated): ").split(',')
            conditions_dict[col.strip()] = {}
            for condition in conditions:
                value, result = condition.split(':')
                conditions_dict[col.strip()][value.strip()] = result.strip()
        report_df = case_report_multiple_conditions(df, conditions_dict)
        print("Generated Report:")
        print(report_df)
        export_filename = input("Enter the filename to export the report (include the '.xlsx' extension): ")
        export_to_excel(report_df, export_filename)
    
    elif report_type == 3:
        index = input("Enter index column: ")
        columns = input("Enter columns: ")
        values = input("Enter values: ")
        pivot_df = pivot_report(df, index, columns, values)
        print("Pivot Table:")
        print(pivot_df)
        
        print("Select next action: ")
        print("1. Print Report")
        print("2. Unpivot Report")
        next_action = int(input("Enter the number of next action: "))
        
        if next_action == 1:
            export_filename = input("Enter the filename to export the report (include the '.xlsx' extension): ")
            export_to_excel(pivot_df, export_filename)
        elif next_action == 2:
            id_vars = input("Enter id_vars (comma separated): ").split(',')
            value_vars = pivot_df.columns[1:]
            unpivot_df = unpivot_report_from_pivot(pivot_df, id_vars, value_vars)
            print("Unpivot Table:")
            print(unpivot_df)
            export_filename = input("Enter the filename to export the report (include the '.xlsx' extension): ")
            export_to_excel(unpivot_df, export_filename)
        else:
            print("Invalid action selected!")
            sys.exit(1)
    elif report_type == 4:
        id_vars = input("Enter id_vars (comma separated): ").split(',')
        value_vars = input("Enter value_vars (comma separated): ").split(',')
        report_df = unpivot_report(df, id_vars, value_vars)
        print("Unpivot Table:")
        print(report_df)
        export_filename = input("Enter the filename to export the report (include the '.xlsx' extension): ")
        export_to_excel(report_df, export_filename)
    else:
        print("Invalid report type selected!")
        sys.exit(1)

if __name__ == "__main__":
    main()