import os
import sys
import csv
import cx_Oracle
import pathlib
from datetime import datetime
from subprocess import call

def read_file_contents(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            lines = file.readlines()
        connection_string = lines[0].strip()
        headers = lines[1].strip().split(', ') 
        query = '\n'.join(lines[2:]).strip()  
        print(f"File read successfully: {file_path}")
        return connection_string, query, headers
    except Exception as e:
        print(f"Failed to read file {file_path}: {str(e)}")
        raise


def parse_connection_string(connection_string):
    try:
        user = ''
        password = ''
        dsn = ''
        if 'User Id=' in connection_string:
            user = connection_string.split('User Id=')[1].split(';')[0].strip()
        if 'Password=' in connection_string:
            password = connection_string.split('Password=')[1].split(';')[0].strip()
        if 'Data Source=' in connection_string:
            dsn = connection_string.split('Data Source=')[1].split(';')[0].strip()
        print(f"Connection string parsed successfully for user: {user}")
        return user, password, dsn
    except Exception as e:
        print(f"Failed to parse connection string: {str(e)}")
        raise

def to_csv(data, headers):
    try:
        with open('output.csv', 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            csvwriter.writerow(headers) 
            for row in data:
                csvwriter.writerow([str(field).replace('"', '""') for field in row])
        print("Data converted to CSV format successfully")
        return 'output.csv'
    except Exception as e:
        print(f"Error converting data to CSV: {str(e)}")
        raise


def prepare_storage_path(base_path, subdir):
    try:
        now = datetime.now()
        date_now = now.strftime('%Y-%m-%d')
        storage_path = pathlib.Path(base_path) / f"{subdir}{date_now}"
        storage_path.mkdir(parents=True, exist_ok=True)
        print(f"Storage directory created: {storage_path}")
        return storage_path
    except Exception as e:
        print(f"Error creating storage path {subdir}: {str(e)}")
        raise

def write_result_to_file(csv_data, file_path):
    try:
        now = datetime.now()
        time = now.strftime('%H-%M-%S')
        base_file_name = pathlib.Path(file_path).stem
        output_file_name = f"{base_file_name}_{time}.csv"
        output_path = pathlib.Path(file_path) / output_file_name
        with open(output_path, 'w') as f:
            f.write(csv_data)
        print(f"Result written to CSV file {output_path}")
    except Exception as e:
        print(f"Failed to write result to file: {str(e)}")
        raise

def main_thread(file_path):
    try:
        connection_string, query, headers = read_file_contents(file_path)
        user, password, dsn = parse_connection_string(connection_string)
        connection = cx_Oracle.connect(user, password, dsn)
        cursor = connection.cursor()
        result = cursor.execute(query)
        csv_data = to_csv(result.fetchall(), headers)
        storage_path = prepare_storage_path(os.getcwd(), 'StorageResults/')
        write_result_to_file(csv_data, storage_path)
    except Exception as e:
        print(f"Error while processing file {file_path}: {str(e)}")
    finally:
        if connection:
            connection.close()
            print(f"Connection closed for file {file_path}")

def run_queries_in_folder(folder_path):
    print(f"Processing SQL files in folder: {folder_path}")
    files = [f for f in os.listdir(folder_path) if f.endswith('.sql')]
    for file in files:
        main_thread(os.path.join(folder_path, file))
    print("running S3 uploader...")
    call(["python3", "s3_uploader.py"])


run_queries_in_folder(os.getcwd())
