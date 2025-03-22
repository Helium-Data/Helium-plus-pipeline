import mysql.connector
from mysql.connector import errorcode
from google.cloud import bigquery
import pandas as pd
import os
from datetime import datetime, date
import config_heliumplus
from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
import base64
import warnings

# Suppress specific warnings
warnings.filterwarnings("ignore", category=UserWarning, message="pandas only support SQLAlchemy connectable")


# Set up Google Cloud credentials
client = bigquery.Client.from_service_account_json(os.path.abspath(os.getcwd()) + '/heliumhealth-1ce77f433fc7.json')


def extract_data_from_mysql(database_name, table_name):
    """Extract data from MySQL and handle the case where the table does not exist."""
    mysql_config = {
        'user': config_heliumplus.mysql_username,
        'password': config_heliumplus.mysql_password,
        'host': 'localhost',
        'port': 6667,
        'database': f'{database_name}',
    }

    try:
        conn = mysql.connector.connect(**mysql_config)
        query = f'SELECT * FROM {table_name}'
        df = pd.read_sql(query, conn)
        conn.close()
        return df

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_NO_SUCH_TABLE:
            print(f"Table {table_name} does not exist in MySQL. Skipping this table.")
            return pd.DataFrame()  # Return an empty DataFrame
        else:
            print(f"Error: {err}")
            return pd.DataFrame()  # Return an empty DataFrame for other errors


def generate_bq_schema(df):
    schema = []
    for column, dtype in df.dtypes.items():
        if pd.api.types.is_datetime64_any_dtype(dtype):
            schema.append(bigquery.SchemaField(column, "DATETIME"))
        elif pd.api.types.is_integer_dtype(dtype):
            schema.append(bigquery.SchemaField(column, "INTEGER"))
        elif pd.api.types.is_float_dtype(dtype):
            schema.append(bigquery.SchemaField(column, "FLOAT"))
        else:
            schema.append(bigquery.SchemaField(column, "STRING"))
    return schema


def map_pandas_dtypes(schema):
    pandas_dtypes = {}
    for field in schema:
        if field.field_type == "DATETIME":
            pandas_dtypes[field.name] = 'datetime64[ns]'
        elif field.field_type == "INTEGER":
            pandas_dtypes[field.name] = 'Int64'  # Use 'Int64' for nullable integers
        elif field.field_type == "FLOAT":
            pandas_dtypes[field.name] = 'float64'
        else:
            pandas_dtypes[field.name] = 'object'
    return pandas_dtypes


def encrypt_data(data, key):
    iv = os.urandom(16)
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    padder = padding.PKCS7(algorithms.AES.block_size).padder()
    padded_data = padder.update(data.encode()) + padder.finalize()
    encryptor = cipher.encryptor()
    encrypted_data = encryptor.update(padded_data) + encryptor.finalize()
    return base64.b64encode(iv + encrypted_data).decode()


def encrypt_sensitive_columns(df, key, sensitive_columns):
    for col in sensitive_columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: encrypt_data(x, key) if pd.notnull(x) else x)
    return df

def check_table_exists(client, dataset_id, table_name):
    table_ref = client.dataset(dataset_id).table(table_name)
    try:
        client.get_table(table_ref)
        return True
    except Exception as e:
        return False


def table_list_to_merge(table_csv, output_csv):
    """
    Filters rows in a CSV file based on filenames in a specified folder and exports the result.

    """
    folder_path = os.path.abspath(os.getcwd()) +'/dumps-sql'

    # Extract filenames (without extensions) from the specified folder
    filenames_to_match = [
        os.path.splitext(f)[0] for f in os.listdir(folder_path) 
        if os.path.isfile(os.path.join(folder_path, f))
    ]

    # Read the database CSV file
    df = pd.read_csv(table_csv)

    # Filter rows where the filename is in the extracted list
    pattern = '|'.join(filenames_to_match)
    filtered_df = df[df['databasename'].str.contains(pattern, na=False, case=False)]

    # Save the filtered results to the output CSV file
    filtered_df.to_csv(output_csv, index=False)


def merge_data_in_bigquery(client, dataset_id, tablename, tablename_temp, common_columns, columns_entries, df):
    table_ref = client.dataset(dataset_id).table(tablename)
    if not check_table_exists(client, dataset_id, tablename):
        print(f"Table {tablename} does not exist. Creating and inserting data.")
        schema = generate_bq_schema(df)
        job_config = bigquery.LoadJobConfig(schema=schema)
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"Created table {tablename} and inserted {job.output_rows} rows.")
        return

    table = client.get_table(table_ref)
    target_columns = {schema_field.name for schema_field in table.schema}

    new_columns = [col for col in df.columns if col not in target_columns]
    if new_columns:
        new_schema_fields = generate_bq_schema(df[new_columns])
        schema_update = list(table.schema)
        for field in new_schema_fields:
            print(f"Adding column '{field.name}' with type '{field.field_type}'")
            schema_update.append(field)
        table.schema = schema_update
        client.update_table(table, ["schema"])
        print(f"Added new columns to {tablename}: {new_columns}")

    columns_temp = [f"source.{col}" for col in common_columns]
    columns_insert = [f"source.{col}" for col in df.columns if col not in common_columns]

    if "id" in columns_entries:
        merge_query = f"""
            MERGE INTO `{dataset_id}.{tablename}` AS target
            USING `{dataset_id}.{tablename_temp}` AS source
            ON target.id = source.id
            WHEN MATCHED THEN
            UPDATE SET {', '.join([f"target.{col} = source.{col}" for col in common_columns])}
            WHEN NOT MATCHED THEN
            INSERT ({', '.join(common_columns + [col for col in df.columns if col not in common_columns])})
            VALUES ({', '.join(columns_temp + columns_insert)});
        """
        job = client.query(merge_query)
        results = job.result()
        print("Merge operation completed successfully.")
    else:
        truncate_query = f"DELETE FROM `{dataset_id}.{tablename}` WHERE true"
        truncate_job = client.query(truncate_query)
        truncate_results = truncate_job.result()
        print(f"Truncate operation completed successfully.")
        insert_query = f"INSERT INTO `{dataset_id}.{tablename}` SELECT * FROM `{dataset_id}.{tablename_temp}`"
        insert_job = client.query(insert_query)
        insert_results = insert_job.result()
        print(f"Insert operation completed successfully.")



def main():
    table_list_to_merge(table_csv = 'tablename.csv', output_csv = 'merge_table.csv')

    tables_list = pd.read_csv(os.path.abspath(os.getcwd()) + '/merge_table.csv')
    tables_list = tables_list.values.tolist()

    for x in tables_list:
        try:
            df = extract_data_from_mysql(x[0], x[1])

            if df.empty:
                print(f"Table {x[0]}.{x[1]}.============================= No data to process")
                continue  # Skip this table and move to the next iteration

            table_id = f'{x[0]}.{x[1]}'
            encryption_key = os.urandom(32)
            sensitive_columns = ['fname', 'lname', 'mname', 'phonenumber', 'email', 'address', 'KinsFirstName', 'KinsLastName', 'KinsPhone', 'KinsAddress']

            df = encrypt_sensitive_columns(df, encryption_key, sensitive_columns)
            schema = generate_bq_schema(df)
            pandas_dtypes = map_pandas_dtypes(schema)

            for column, dtype in pandas_dtypes.items():
                df[column] = df[column].astype(dtype)

            temp_table_id = f'{table_id}_temp'
            job_config = bigquery.LoadJobConfig(schema=schema)
            job = client.load_table_from_dataframe(df, temp_table_id, job_config=job_config)
            job.result()

            print(f'Loaded {job.output_rows} rows into temporary table {temp_table_id}.')

            common_columns = df.columns.tolist()
            merge_data_in_bigquery(client, x[0], x[1], f'{x[1]}_temp', common_columns, df.columns, df)

            client.delete_table(temp_table_id)
            print(f'Deleted temporary table {temp_table_id}.==================== completed')

        except Exception as e:
            print(f"Table {x[0]}.{x[1]} =========================== doesn't exist")
            continue  # Continue to the next table if there's an error


# Run the sync function
if __name__ == '__main__':
    main()