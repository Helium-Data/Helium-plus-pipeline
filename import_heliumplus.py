import mysql.connector
import os
import config_heliumplus
import pandas as pd


def database_list_to_import(database_csv, output_csv):
    """
    Filters rows in a CSV file based on filenames in a specified folder and exports the result.

    Parameters:
        folder_path (str): Path to the folder containing files.
        database_csv (str): Path to the CSV file with columns 'filename' and 'databasename'.
        output_csv (str): Path to save the filtered results. Defaults to 'import_database.csv'.
    """
    folder_path = os.path.abspath(os.getcwd()) +'/dumps-sql'

    # Extract filenames (without extensions) from the specified folder
    filenames_to_match = [
        os.path.splitext(f)[0] for f in os.listdir(folder_path) 
        if os.path.isfile(os.path.join(folder_path, f))
    ]

    # Read the database CSV file
    df = pd.read_csv(database_csv)

    # Filter rows where the filename is in the extracted list
    filtered_df = df[df['filename'].isin(filenames_to_match)]

    # Save the filtered results to the output CSV file
    filtered_df.to_csv(output_csv, index=False)
    print(f"Filtered results saved to {output_csv}")


def import_mysql_dump(host, user, password, database, port, dump_file_path):
    try:
        # Connect to MySQL server
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            port=port
        )
        
        cursor = conn.cursor()
        
        # # Drop database if exists
        # cursor.execute(f"DROP DATABASE IF EXISTS {database}")
        
        # # Create database
        # cursor.execute(f"CREATE DATABASE {database}")
        
        # Connect to the specific database
        conn.database = database
        
        # Read the dump file line by line
        with open(dump_file_path, 'r') as file:
            sql_command = ""
            for line in file:
                if line.strip().endswith(';'):
                    sql_command += line.strip()
                    try:
                        cursor.execute(sql_command)
                        conn.commit()
                        print(f"Executed: {sql_command}")
                    except mysql.connector.Error as err:
                        print(f"Error: {err}")
                        print(f"Command: {sql_command}")
                    sql_command = ""
                else:
                    sql_command += line.strip() + " "
        
        # Close the cursor and connection
        cursor.close()
        conn.close()
    
    except mysql.connector.Error as err:
        print(f"Error: {err}")


# Main function to sync data from MySQL to BigQuery
def main():

    # MySQL database details
    host = 'localhost'
    user = config_heliumplus.mysql_username
    password = config_heliumplus.mysql_password
    port = 6667  # Specify your MySQL server port

    # create list of database to import
    database_list_to_import(database_csv = 'databasename.csv', output_csv = 'import_database.csv')

    # read from the database list
    database_list = pd.read_csv(os.path.abspath(os.getcwd()) +'/import_database.csv')
    database_list = database_list.values.tolist()
     
    imported_databases = []  # List to store the names of imported databases

    for x in database_list:
        database = x[1]
        dump_file_path = os.path.join(os.path.abspath(os.getcwd()), 'dumps-sql', f'{x[0]}.sql')

        # Import the MySQL database dump
        import_mysql_dump(host, user, password, database, port, dump_file_path)

        # Add the database name to the list after each import completes
        imported_databases.append(database)

    # Print all imported databases after the loop is complete
    for db in imported_databases:
        print(f"Importing {db} file in progress........")
        print(f"Import {db} completed........")


# Run the sync function
if __name__ == '__main__':
        main()