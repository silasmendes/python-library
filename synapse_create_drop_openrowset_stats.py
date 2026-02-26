import pyodbc
import os

"""
DISCLAIMER:
Although the author is a Microsoft employee, this script is provided as-is, without any official 
support or endorsement from Microsoft. Use it at your own risk.

PURPOSE:
This Python script helps create and drop statistics for Parquet or Delta tables in Synapse Serverless SQL pool.

DETAILS:
- To view table schema, this code connects to Serverless SQL pool and runs the `sp_describe_first_result_set` stored procedure.
- It does NOT create statistics or make changes to your Serverless SQL pool.
- Requires the `pyodbc` package. Install it before running the script:
      `pip install pyodbc`

OUTPUT:
Generates two files:
- create_stats_openrowset.txt
- drop_stats_openrowset.txt
You can review and execute these files in your Synapse SQL, preferably using SSMS or Azure Data Studio.

VARIABLES TO SET BEFORE RUNNING:
- SERVER_NAME: your_server_name-ondemand.sql.azuresynapse.net
- DATABASE_NAME: The target database in Synapse.
- DATA_LAKE_URL: Path to your Parquet/Delta table in the Data Lake.
- TABLE_FORMAT: 'PARQUET' or 'DELTA'
"""

# Configuration variables
SERVER_NAME = 'your_server_name-ondemand.sql.azuresynapse.net'  # Update this with your Serverless SQL endpoint
DATABASE_NAME = 'your_database_name'                            # Update this to your database name
DATA_LAKE_URL = 'https://your_storage_account.dfs.core.windows.net/your_container/your_folder'  # Update this to your data lake URL

# Set this to 'PARQUET' or 'DELTA'
TABLE_FORMAT = 'PARQUET'

# Output file paths
CREATE_STATS_FILE_PATH = r'c:\temp\create_stats_openrowset.txt'
DROP_STATS_FILE_PATH = r'c:\temp\drop_stats_openrowset.txt'


def ensure_output_directory(file_path):
    """Ensures that the output directory exists."""
    output_dir = os.path.dirname(file_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)


def get_column_names(cursor):
    """Fetches the 'name' column from the sp_describe_first_result_set output."""
    columns = [column[0] for column in cursor.description]
    name_index = columns.index('name')

    rows = cursor.fetchall()
    column_names = [row[name_index] for row in rows]
    return column_names


def write_create_statistics_commands(column_names, file_path):
    """
    Writes the CREATE STATISTICS commands to the output file.
    """
    with open(file_path, 'w') as f:
        for column_name in column_names:
            # PRINT statement
            f.write(f"PRINT 'Creating stats for column [{column_name}]...'\n")
            f.write("GO\n")

            # CREATE statement
            create_cmd = (
                "EXEC sys.sp_create_openrowset_statistics N'SELECT [{0}] "
                "FROM OPENROWSET(BULK ''{1}'', FORMAT = ''{2}'') AS [q1]';"
            ).format(column_name, DATA_LAKE_URL, TABLE_FORMAT)
            f.write(create_cmd + "\n")
            f.write("GO\n\n")


def write_drop_statistics_commands(column_names, file_path):
    """
    Writes the DROP STATISTICS commands to the output file.
    """
    with open(file_path, 'w') as f:
        for column_name in column_names:
            # PRINT statement
            f.write(f"PRINT 'Dropping stats for column [{column_name}]...'\n")
            f.write("GO\n")

            # DROP statement
            drop_cmd = (
                "EXEC sys.sp_drop_openrowset_statistics N'SELECT [{0}] "
                "FROM OPENROWSET(BULK ''{1}'', FORMAT = ''{2}'') AS [q1]';"
            ).format(column_name, DATA_LAKE_URL, TABLE_FORMAT)
            f.write(drop_cmd + "\n")
            f.write("GO\n\n")


def main():
    # Ensure the output directories exist
    ensure_output_directory(CREATE_STATS_FILE_PATH)
    ensure_output_directory(DROP_STATS_FILE_PATH)

    # Connection string for Azure Synapse Serverless (in this case using AAD + MFA)
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

        # Check the table schema
        command = (
            "EXEC sp_describe_first_result_set N'SELECT * FROM OPENROWSET("
            f"BULK ''{DATA_LAKE_URL}'', FORMAT = ''{TABLE_FORMAT}'') AS [q1]'"
        )
        cursor.execute(command)

        # Get column names
        column_names = get_column_names(cursor)

        # Write CREATE statistics commands
        write_create_statistics_commands(column_names, CREATE_STATS_FILE_PATH)
        print(f"Create statistics commands written to {CREATE_STATS_FILE_PATH}")

        # Write DROP statistics commands
        write_drop_statistics_commands(column_names, DROP_STATS_FILE_PATH)
        print(f"Drop statistics commands written to {DROP_STATS_FILE_PATH}")

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        # Close the connection
        if 'conn' in locals():
            conn.close()


if __name__ == '__main__':
    main()


