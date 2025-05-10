# Import Python dependencies needed for the workflow
from urllib import request
from minio import Minio, S3Error
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
import os
import urllib.error


def download_parquet(**kwargs):
    folder_path: str = '/tmp/data/raw'
    os.makedirs(folder_path, exist_ok=True)
    url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    filename: str = "yellow_tripdata"
    extension: str = ".parquet"
    month_start = pendulum.datetime(2025, 1, 1) # Start date
    month = month_start.subtract(months=1).format('YYYY-MM') # get tthe last month
    # month: str = pendulum.now().subtract(months=2).format('YYYY-MM')
    file_url = url + filename + '_' + month + extension
    local_file = os.path.join(folder_path, f"yellow_tripdata_{month}.parquet")
    print(f"Downloading {file_url} to {local_file}")
    try:
        urllib.request.urlretrieve(file_url, local_file)
        print(f"Downloaded {file_url} to {local_file}")
    except urllib.error.URLError as e:
        raise RuntimeError(f"Failed to download the parquet file : {str(e)}") from e


# Python Function
def upload_file(**kwargs):
    ###############################################
    # Upload generated file to Minio

    client = Minio(
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = 'spark'

    # month: str = pendulum.now().subtract(months=2).format('YYYY-MM')
    month_start = pendulum.datetime(2025, 1, 1) # Start date
    month = month_start.subtract(months=1).format('YYYY-MM') # get tthe last month
    print(client.list_buckets())
    filename = f"yellow_tripdata_{month}.parquet"
    local_path = os.path.join("/tmp/data/raw", filename)

    client.fput_object(
        bucket_name=bucket,
        object_name=filename,
        file_path=os.path.join("/tmp/data/raw", f"yellow_tripdata_{month}.parquet")
    )
    # On supprime le fichié récement téléchargés, pour éviter la redondance. On suppose qu'en arrivant ici, l'ajout est
    # bien réalisé
    os.remove(local_path)


###############################################
with DAG(dag_id='grab_nyc_data_to_minio',
         start_date=days_ago(1),
         schedule_interval=None,
         catchup=False,
         tags=['minio/read/write'],
         ) as dag:
    ###############################################
    # Create a task to call your processing function
    t1 = PythonOperator(
        task_id='download_parquet',
        provide_context=True,
        python_callable=download_parquet
    )
    t2 = PythonOperator(
        task_id='upload_file_task',
        provide_context=True,
        python_callable=upload_file
    )
###############################################

###############################################
# first upload the file, then read the other file.
t1 >> t2
###############################################
