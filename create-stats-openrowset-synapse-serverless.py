import pyodbc
import os

"""
This Python script is intended to help create statistics on Synapse Serverless SQL pool for Parquet tables.

ATTENTION: This does not create the statistics directly in your SQL pool. Open the txt file, copy the commands, and execute them in a new session in your SQL pool.

You can adapt this to create statistics for CSV or Delta tables as well.
If you plan to use this in a production environment, make sure to test it accordingly.
Make sure to update your Serverless SQL endpoint name, database name, and the Data Lake URL for the table you want to create statistics.
"""

# Configuration variables
SERVER_NAME = 'your_server_name-ondemand.sql.azuresynapse.net'  # Update this to your server name
DATABASE_NAME = 'your_database_name'  # Update this to your database name
DATA_LAKE_URL = 'https://your_storage_account.blob.core.windows.net/your_container/your_file.parquet'  # Update this to your data lake URL
OUTPUT_FILE_PATH = r'c:\temp\create_stats_openrowset.txt'  # Output file path

def ensure_output_directory(file_path):
    """Ensures that the output directory exists."""
    output_dir = os.path.dirname(file_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

def get_column_names(cursor):
    """Fetches the 'name' column from the result set."""
    # Get the index of the 'name' column
    columns = [column[0] for column in cursor.description]
    name_index = columns.index('name')

    # Fetch all rows and extract the 'name' column
    rows = cursor.fetchall()
    column_names = [row[name_index] for row in rows]
    return column_names

def write_statistics_commands(column_names, file_path):
    """Writes the statistics creation commands to the output file."""
    with open(file_path, 'w') as f:
        for column_name in column_names:
            content = (
                "EXEC sys.sp_create_openrowset_statistics N'SELECT [{0}] FROM OPENROWSET("
                "BULK ''{1}'', FORMAT = ''PARQUET'') AS [q1]'".format(column_name, DATA_LAKE_URL)
            )
            f.write(content + '\n\n')

def main():
    # Ensure the output directory exists
    ensure_output_directory(OUTPUT_FILE_PATH)

    # Connection string
    conn_str = (
        'Driver={ODBC Driver 17 for SQL Server};'
        f'Server={SERVER_NAME};'
        f'Database={DATABASE_NAME};'
        'Authentication=ActiveDirectoryInteractive;'
    )

    try:
        # Connect to the Synapse SQL Serverless pool
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Execute the stored procedure
        command = (
            "EXEC sp_describe_first_result_set N'SELECT * FROM OPENROWSET("
            f"BULK ''{DATA_LAKE_URL}'', FORMAT = ''PARQUET'') AS [q1]'"
        )
        cursor.execute(command)

        # Get column names
        column_names = get_column_names(cursor)

        # Write statistics commands to the output file
        write_statistics_commands(column_names, OUTPUT_FILE_PATH)
        print(f"Statistics commands have been written to {OUTPUT_FILE_PATH}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the connection
        if 'conn' in locals():
            conn.close()

if __name__ == '__main__':
    main()
