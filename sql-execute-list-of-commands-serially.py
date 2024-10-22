# Scenario
    # I have a CSV file that contains hundreds of SQL commands that I want to run serially.
    # Each command will be executed in the context of a new connection in Azure SQL Database.
    # This script requires ODBC Driver 17 for SQL Server

import csv
import pyodbc

server = 'server_name.database.windows.net'
database = 'database_name'
username = 'SQL auth username'
password = 'password'
driver= '{ODBC Driver 17 for SQL Server}'

# Replace 'your_csv_file.csv' with the path to your actual CSV file
with open('your_csv_file.csv', newline='') as csvfile:
    commands = csv.reader(csvfile, delimiter='\n', quotechar='"')
    for row in commands:
        command = row[0]
        if command.strip() != '':
            try:
                conn = pyodbc.connect('DRIVER=' + driver + ';SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
                cursor = conn.cursor()
                cursor.execute(command)
                print('Executed command:', command)
                print('Execution successful')
            except Exception as e:
                print('Error executing command:', command)
                print('Error message:', str(e))
            finally:
                cursor.close()
                conn.close()
