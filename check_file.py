from google.cloud import storage
from google.cloud import bigquery
from datetime import timedelta
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import *
from airflow.models import Variable 


default_arg = {
    'owner' : 'data_engineer',
    'start_date': datetime(2023,2,23)
}

def exist_file_in_gcs(bucket_name,file_path):
    storage_client = storage.client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_path)
    return blob.exists()

def check_file_in_gcs():
    try:
        bucket_name = 'us-central1-practiceset-d10a9e0b-bucket'
        file_path = 'https://storage.cloud.google.com/us-central1-practiceset-d10a9e0b-bucket/dags/sd.csv'
        file_exists = exist_file_in_gcs(bucket_name,file_path)
        if file_exists:
            print(f' file available on{file_path}')
        print(f'file is not on {file_path}')
    except Exception as self:
        print(f"not define {self}")

def get_from_gcs():
    if check_file_in_gcs == True:
        bq_client = bigquery.Client()
        storage_client = storage.Client()
        bucket = storage_client.get_bucket('us-central1-practiceset-d10a9e0b-bucket')
        blob = bucket.get_blob('us-central1-practiceset-d10a9e0b-bucket/dags/sd.csv')
        contents = blob.download_as_string()


        data_set_id = 'samradhe007.RS'
        table_id = 'us-central1-practiceset-d10a9e0b-bucket/dags/sd.csv'

        dataset_ref = bq_client.dataset(data_set_id)
        table_ref = dataset_ref.table(table_id)

        job_config = bigquery.LoadJobConfig()
        job_config.source_format = bigquery.SourceFormat.CSV
        job_config.skip_leading_rows = 1

        with open('your_file.csv', 'rb') as source_file:
            job = bq_client.load_table_from_file(source_file, table_ref, job_config=job_config)

        job.result()
        print('Data loaded to BigQuery table {}.'.format(table_id))


    

with DAG('check_dag', default_args=default_arg)as dag:
    start_dag = DummyOperator(
        task_id = 'start_dag'
    )

    find_file = PythonOperator(
        task_id = 'find_file',
        python_callable=check_file_in_gcs
    )
    store_file = PythonOperator(
        task_id = 'stor_file',
        python_callable=get_from_gcs
    )
    end_dag = DummyOperator(
        task_id = 'end_dag'
    )

    start_dag >> find_file >> get_from_gcs >> end_dag