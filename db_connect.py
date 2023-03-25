import psycopg2
from dotenv import dotenv_values
from io import StringIO

env_data= dotenv_values('.env')

connection = psycopg2.connect(
    host=env_data['host'],user=env_data['username'],
    password=env_data['password'],port=env_data['port'],
    database='postgres'
)
