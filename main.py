import os
import csv
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_NAME = os.environ.get("DATABASE_NAME")
DATABASE_USERNAME = os.environ.get("DATABASE_USERNAME")
PASSWORD = os.environ.get("PASSWORD")
HOST = os.environ.get("HOST")
PORT = os.environ.get("PORT")


# Connect to PostgresSQL database
conn = psycopg2.connect(database=DATABASE_NAME, user=DATABASE_USERNAME,
                        password=PASSWORD, host=HOST, port=PORT)
cur = conn.cursor()
conn.commit()
cur.close()
conn.close()

