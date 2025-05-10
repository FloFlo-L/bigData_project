import os
import logging
from minio import Minio
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 300,  # in seconds
}

# Crée le moteur SQLAlchemy pour Postgres
engine = create_engine('postgresql://postgres:admin@data-warehouse:5432/yellow_tripdata')

def download_from_minio(**kwargs):
    """
    Télécharge tous les fichiers Parquet depuis Minio
    """
    client = Minio(
        "minio:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket = 'spark'
    raw_dir = '/tmp/data/raw'
    os.makedirs(raw_dir, exist_ok=True)
    downloaded = []

    # Vérifie que le bucket existe
    if not client.bucket_exists(bucket):
        raise AirflowFailException(f"Bucket '{bucket}' introuvable dans Minio")

    try:
        for obj in client.list_objects(bucket, recursive=True):
            if not obj.object_name.endswith('.parquet'):
                continue

            local_path = os.path.join(raw_dir, obj.object_name)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)

            client.fget_object(bucket, obj.object_name, local_path)
            logging.info(f"Téléchargé: {obj.object_name} → {local_path}")
            downloaded.append(local_path)

    except Exception as e:
        raise AirflowFailException(f"Erreur lors du téléchargement depuis Minio: {e}")

    if not downloaded:
        logging.warning("Aucun fichier parquet téléchargé depuis Minio")

    # Partage la liste des fichiers via XCom
    kwargs['ti'].xcom_push(key='local_files', value=downloaded)


def insert_into_postgres(**kwargs):
    """
    Insère chaque fichier Parquet dans la table Postgres
    """
    local_files = kwargs['ti'].xcom_pull(key='local_files', task_ids='download_from_minio')
    if not local_files:
        raise AirflowFailException("Aucun fichier à insérer dans Postgres")

    for path in local_files:
        try:
            df = pd.read_parquet(path)
            # Normalisation des noms de colonnes en minuscules
            df.columns = [c.lower() for c in df.columns]

            # Insertion dans Postgres via pandas
            df.to_sql(
                name='yellow_tripdata',
                con=engine,
                if_exists='append',
                index=False
            )
            logging.info(f"Inséré dans Postgres: {path}")

        except Exception as e:
            logging.error(f"Échec insertion pour {path}: {e}")
            raise AirflowFailException(f"Insertion échouée pour {path}: {e}")

        finally:
            # Nettoyage local
            try:
                os.remove(path)
                logging.info(f"Supprimé localement: {path}")
            except OSError as oe:
                logging.warning(f"Impossible de supprimer {path}: {oe}")

with DAG(
    dag_id='Minio_to_Postgres',
    default_args=default_args,
    description='Téléchargement des Parquet depuis Minio vers Postgres',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['minio', 'postgres', 'etl'],
) as dag:

    t3 = PythonOperator(
        task_id='download_from_minio',
        python_callable=download_from_minio,
        provide_context=True,
    )

    t4 = PythonOperator(
        task_id='insert_into_postgres',
        python_callable=insert_into_postgres,
        provide_context=True,
    )

    t3 >> t4