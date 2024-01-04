import os
import csv
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv()

DATABASE_NAME = os.environ.get("DATABASE_NAME")
DATABASE_USERNAME = os.environ.get("DATABASE_USERNAME")
PASSWORD = os.environ.get("PASSWORD")
HOST = os.environ.get("HOST")
PORT = os.environ.get("PORT")

# conn = psycopg2.connect(database=DATABASE_NAME, user=DATABASE_USERNAME,password=PASSWORD, host=HOST, port=PORT)


# Function to create tables dynamically
def create_table(database_cursor, database_table_name):
    # Generate CREATE TABLE statement
    create_table_query = sql.SQL('CREATE TABLE IF NOT EXISTS {} (name VARCHAR(255), address VARCHAR(255))').format(
        sql.Identifier(database_table_name)
    )

    # Execute the CREATE TABLE statement
    database_cursor.execute(create_table_query)


# Function to insert data into tables
def insert_data(database_cursor, database_table_name, table_data):
    # Generate INSERT INTO statement
    insert_query = sql.SQL('INSERT INTO {} (name, address) VALUES (%s, %s)').format(
        sql.Identifier(database_table_name)
    )

    # Execute the INSERT INTO statement for each row in the data
    database_cursor.executemany(insert_query, table_data)


# PostgresSQL connection parameters
conn_params = {
    'database': DATABASE_NAME,
    'user': DATABASE_USERNAME,
    'password': PASSWORD,
    'host': HOST,
    'port': PORT
}

# Connect to PostgresSQL
conn = psycopg2.connect(**conn_params)
cursor = conn.cursor()

# Folder containing CSV files
folder_path = '/home/navyaadvisor/Desktop/csv/'
print(folder_path)

# Iterate through CSV files
for filename in os.listdir(folder_path):
    if filename.endswith(".csv"):
        table_name = os.path.splitext(filename)[0]
        full_path = os.path.join(folder_path, filename)

        # Read CSV data
        with open(full_path, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
            next(csv_reader)  # Skip header row
            data = [row for row in csv_reader]

        # Create table if it does not exist
        create_table(cursor, table_name)

        # Insert data into the table
        if data:
            insert_data(cursor, table_name, data)
            print(f"Data from '{filename}' inserted into table '{table_name}' successfully.")
        else:
            print(f"No data found in '{filename}'.")

# Commit changes and close connection
conn.commit()
cursor.close()
conn.close()
