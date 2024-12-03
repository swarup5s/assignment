import boto3

from airflow.models import Variable
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

from datetime import datetime, timedelta
import pandas as pd

from sqlalchemy import create_engine



"""
    These can be also stored in Airflow Variables ot Airflow Connections
    depending on the use case and nature of the data
"""

MYSQL_CONN_ID = 'musql_connection'
DATABASE_NAME = 'students_info'
TABLE_NAME = 'STUDENTS'
S3_BUCKET = 'assignment-eu-central-1'
S3_KEY = 'bronze/students/STUDENTS.parquet'
WHERE_CONDITION = None # 'percentage_of_marks > 98'
AWS_CONN_ID = 'aws_dev'
AWS_PREFIX_KEY = '/data/raw/STUDENTS.parquet'


def get_tags_from_datahub(datahub_env, table_name):
    """
    Fetch tags from DataHub
    """
    return {allowed_columns: ['student_code', 'honors_subject', 'percentage_of_marks'],
            blocked_columns: ['first_name', 'last_name', 'email', 'phone_no', 'date_of_birth']}



# Allowed and Blocked Columns
# Approacxh 1: Fetch from DataHub. I would prefer this approach if DataHub is available. I designed and implemented the same in my current project. So I'm biased towards this :D
datahub_tags = get_tags_from_datahub(datahub_env='dev', table_name='STUDENTS')
if datahub_tags:
    allowed_columns = datahub_tags['allowed_columns']
    blocked_columns = datahub_tags['blocked_columns']
else:
    # Approach 2: Fetch from Airflow Variables
    data_governance = Variable.get("data_governance", deserialize_json=True)
    allowed_columns = data_governance['allowed_columns'] # ['student_code', 'honors_subject', 'percentage_of_marks']
    blocked_columns = data_governance['blocked_columns'] #['first_name', 'last_name', 'email', 'phone_no', 'date_of_birth']

def get_credentials(conn_id, vault_url:None):
    if vault_url:
        # fetch credentials from vault. This would be a better approach. We are using this in our current project
        pass
    else:
        conn = BaseHook.get_connection(conn_id)
        return conn.access, conn.secret

def upload_to_s3(full_filename, bucket, key, vault_uri):

    access_key, secret_key = get_credentials(conn_id=AWS_CONN_ID, vault_uri=vault_uri)

    s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name='eu-central-1')
    try:
        s3_client.upload_file(Filename=full_filename, Bucket=bucket, Key=key)
        print(f"File {full_filename} uploaded to s3://{bucket}/{key}")
    except Exception as e:
        print(f"Error uploading file: {e}")


default_args = {
    'owner': 'swarup',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    'source_data_pipeline_pyspark',
    default_args=default_args,
    description='Extracting students data from MySQL, validate, and upload them to S3',
    schedule_interval='0 0 * * *',
    catchup=False
) as dag:
    
    spark_fetch = SparkSubmitOperator(
                                    task_id='spark_fetch',
                                    conn_id=AWS_CONN_ID,
                                    driver_memory = '10g',
                                    num_executors = 6,
                                    executor_cores = 4,      
                                    executor_memory='8g',
                                    application='location_of_spark_application.py',
                                    application_args=["-t", TABLE_NAME, "-d", DATABASE_NAME, "-p", "output_path"]
                                    )

    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_args=[AWS_PREFIX_KEY, S3_BUCKET, S3_KEY, None]
    )

    # Or DistCP can be also used as Bashoperator

    spark_fetch >> upload_to_s3