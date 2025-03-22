# Step 1: Install necessary libraries
# !pip install mysql-connector-python google-cloud-bigquery pandas

import mysql.connector
from google.cloud import bigquery
import pandas as pd
import os
from datetime import datetime, date
import config_heliumplus

from cryptography.hazmat.primitives.ciphers import Cipher, algorithms, modes
from cryptography.hazmat.primitives import padding
from cryptography.hazmat.backends import default_backend
import base64


# Step 2: Set up Google Cloud credentials
client = bigquery.Client.from_service_account_json(os.path.abspath(os.getcwd()) + '/heliumhealth-1ce77f433fc7.json')


# Step 3: Extract data from MySQL
def extract_data_from_mysql(database_name,table_name):
    # MySQL connection details
    mysql_config = {
        'user': config_heliumplus.mysql_username,
        'password': config_heliumplus.mysql_password,
        'host': 'localhost',
        'port': 6667,
        'database': f'{database_name}',
         
    }

    # SQL query to extract data
    query = f'SELECT * FROM {table_name}'
    

    # Connect to MySQL and extract data into a DataFrame
    conn = mysql.connector.connect(**mysql_config)
    df = pd.read_sql(query, conn)

    # Convert date columns from datetime.date to datetime.datetime
    for col in df.columns:
        if df[col].dtype == 'object' and df[col].apply(lambda x: isinstance(x, date)).all():
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # for col in df.columns:
    #     # Check if the column is of object type (string)
    #     if df[col].dtype == 'object':
    #         # Try converting a small portion of the column to datetime to check if it is date-like
    #         try:
    #             # Convert a sample of the column to datetime to see if it succeeds
    #             pd.to_datetime(df[col].head(10), errors='raise')
    #             # If conversion is successful, convert the entire column
    #             df[col] = pd.to_datetime(df[col], errors='coerce')
    #         except Exception as e:
    #             # If conversion fails, it means the column is likely not a date
    #             continue


    conn.close()
    return df


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


def detect_and_convert_date_columns(df):
# Automatically detect and convert columns that are likely to be dates
    for col in df.columns:
        # Check if the column is of object type (string)
        if df[col].dtype == 'object':
            # Try converting a small portion of the column to datetime to check if it is date-like
            try:
                # Convert a sample of the column to datetime to see if it succeeds
                pd.to_datetime(df[col].head(10), errors='raise')
                # If conversion is successful, convert the entire column
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception as e:
                # If conversion fails, it means the column is likely not a date
                continue
    return df


def drop_bigquery_table_if_exists(client, table_id):
    try:
        client.delete_table(table_id)
        print(f"Deleted table '{table_id}'.")
    except Exception as e:
        print(f"Table '{table_id}' does not exist. Proceeding with data load.")


def encrypt_data(data, key):
    # Generate a random 16 bytes IV (Initialization Vector)
    iv = os.urandom(16)
    
    # Create a cipher object using the key and IV
    cipher = Cipher(algorithms.AES(key), modes.CBC(iv), backend=default_backend())
    
    # Pad the data to be encrypted
    padder = padding.PKCS7(algorithms.AES.block_size).padder()
    padded_data = padder.update(data.encode()) + padder.finalize()
    
    # Encrypt the padded data
    encryptor = cipher.encryptor()
    encrypted_data = encryptor.update(padded_data) + encryptor.finalize()
    
    # Return the IV and encrypted data encoded in base64 for storage
    return base64.b64encode(iv + encrypted_data).decode()


def encrypt_sensitive_columns(df, key, sensitive_columns):
    """Encrypts sensitive columns in the DataFrame."""
    for col in sensitive_columns:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: encrypt_data(x, key) if pd.notnull(x) else x)
    return df


def load_data_to_bigquery(df, database_name, table_name):
    # BigQuery client
    # client = bigquery.Client()
    
    # Generate a 32-byte key for encryption
    encryption_key = os.urandom(32)

    sensitive_columns = ['fname','lname','mname','phonenumber','email','address','KinsFirstName','KinsLastName','KinsPhone','KinsAddress']   

    # Define the BigQuery table ID
    table_id = f'heliumhealth.{database_name}.{table_name}'


    # Drop existing table if it exists
    drop_bigquery_table_if_exists(client, table_id)

    # Encrypt sensitive columns
    df = encrypt_sensitive_columns(df, encryption_key, sensitive_columns)

    # Generate schema from DataFrame
    schema = generate_bq_schema(df)

    # Map schema to pandas dtypes
    pandas_dtypes = map_pandas_dtypes(schema)

    # Apply the correct dtypes to the DataFrame
    for column, dtype in pandas_dtypes.items():
        df[column] = df[column].astype(dtype)


    # Create an empty DataFrame with the schema if df is empty
    if df.empty:
        df = pd.DataFrame({field.name: pd.Series(dtype=pandas_dtypes[field.name]) for field in schema})

    # Load data into BigQuery
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)

    # Wait for the load job to complete
    job.result()

    print(f'Loaded {job.output_rows} rows into {table_id}.')



# Main function to sync data from MySQL to BigQuery
def main():
    tables_list = pd.read_csv(os.path.abspath(os.getcwd()) +'/tablename.csv')
    tables_list = tables_list.values.tolist()
    
    # # Define BigQuery project
    # project_id = 'heliumhealth'
    

    for x in tables_list:

        # Extract data from MySQL
        # df = extract_data_from_mysql(x[0],x[1])
        df = extract_data_from_mysql(x[0], x[1])


        # Load data into BigQuery
        load_data_to_bigquery(df,x[0],x[1])

# Run the sync function
if __name__ == '__main__':
    main()