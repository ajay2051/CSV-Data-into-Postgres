import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

DATABASE_NAME = os.environ.get("DATABASE_NAME")
DATABASE_USERNAME = os.environ.get("DATABASE_USERNAME")
PASSWORD = os.environ.get("PASSWORD")
HOST = os.environ.get("HOST")
PORT = os.environ.get("PORT")

# Replace with your PostgresSQL connection details
db_credentials = {
    'host': HOST,
    'database': DATABASE_NAME,
    'user': DATABASE_USERNAME,
    'password': PASSWORD,
}

# Replace with the directory path containing your CSV files
csv_directory = '/home/navyaadvisor/Desktop/csv/'

# Get a list of all CSV files in the specified directory
csv_files = [f for f in os.listdir(csv_directory) if f.endswith('.csv')]

# Create an empty DataFrame to store the concatenated data
df_stock = pd.DataFrame()

# Iterate over each CSV file and concatenate the data
for csv_file in csv_files:
    file_path = os.path.join(csv_directory, csv_file)
    df_temp = pd.read_csv(file_path)
    df_stock = pd.concat([df_stock, df_temp], ignore_index=True)

# Create a PostgresSQL engine
engine = create_engine(f'postgresql://{db_credentials["user"]}:{db_credentials["password"]}@{db_credentials["host"]}/{db_credentials["database"]}')

# Insert data into the PostgresSQL database
df_stock.to_sql('stock', engine, if_exists='replace', index=False)

# Close the database connection
engine.dispose()

print("Data inserted successfully into the 'stock' table.")
