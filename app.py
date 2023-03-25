from flask import Flask
import os
from flask import Flask,json, flash, request, redirect, url_for
from werkzeug.utils import secure_filename
from datetime import datetime
from db_connect import connection
import pandas as pd
from io import StringIO 
import time

UPLOAD_FOLDER = './data/'
ALLOWED_EXTENSIONS = {'txt', 'pdf', 'png', 'jpg', 'jpeg', 'gif','xls','csv'}
conn = None
cursor = None

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def read_file(filepath):
    print(filepath)

def initate_db():
    global conn,cursor
    conn = connection
    cursor  = connection.cursor()
    return cursor

def timing(func):
    def wrapper(*args,**kwargs):
        start = time.time()
        func(*args, **kwargs)
        end = time.time()
        print(f'Time Taken to execute {func.__name__} function is {end-start}')
    return wrapper


@timing
def model_creation(schema_name,table_name):
    # cursor = conn.cursor()
    cursor = initate_db()
    create_schema = f"""CREATE SCHEMA IF NOT EXISTS {schema_name};"""
    create_table = f"""CREATE TABLE IF NOT EXISTS {schema_name}.{table_name} ( 
                    id serial PRIMARY KEY,
                    Name VARCHAR NOT NULL,
                    Age INT,
                    Address VARCHAR NOT NULL,
                    Salary INT
                    );"""
    cursor.execute(create_schema)
    cursor.execute(create_table)
    # cursor.commit()
    conn.commit()
    

@timing
def read_data_asString_buffer(filepath):
    df =  pd.read_csv(filepath)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer,header=False)
    return csv_buffer

def db_close():
    global cursor,conn

    cursor.close()
    conn.close()
@timing
def insert_values_from_file(filepath,schema_name,table_name):
    df =  pd.read_csv(filepath)
    csv_buffer = StringIO()
    df.to_csv(csv_buffer,header=False)
    csv_buffer.seek(0)

    if csv_buffer:
        cursor = initate_db()
        cursor.execute(f'SET search_path TO {schema_name};')

        cursor.copy_from(csv_buffer, f"{table_name}", sep=",")
        conn.commit()


@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        # check if the post request has the file part
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        # If the user does not select a file, the browser submits an
        # empty file without a filename.
        if file.filename == '':
            flash('No selected file')
            return redirect(request.url)
        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'],filename)
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            read_file(file_path)
            user_id = request.form.get('create_usr_id')
            schema = request.form.get('schema')
            file = request.form.get('file')
            timestamp = datetime.now().strftime(format="%Y_%m_%d_%H_%M_%S")
            strpfilename = filename.split('.csv')[0]
            table_name = f"{strpfilename}_{timestamp}"
            # print(f"table name is {table_name}, timestamp is {timestamp} \
            #     user_id is {user_id}")
            
            model_creation(schema_name=schema,table_name=table_name)
            
            insert_values_from_file(filepath=file_path,schema_name=schema,table_name=table_name)
            db_close()
            return f"File UPloaded successfully!!! table name is {table_name}, timestamp is {timestamp} \
                user_id is {user_id}"
        
    return '''
    <!doctype html>
    <title>Upload new File</title>
    <h1>Upload new File</h1>
    <form method=post enctype=multipart/form-data>
      
      <input type=file name=file placeholder="file path">
      <input type=text name=create_usr_id placeholder="user id">
      <input type=text name=schema placeholder="schema name">
      <input type=submit value=Upload>
    </form>
    '''

if __name__ == "__main__":
  app.run()
