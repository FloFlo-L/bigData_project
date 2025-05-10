import logging
import pandas as pd
from sqlalchemy import create_engine, text
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowFailException

# Connexions
SRC_CONN = 'postgresql://postgres:admin@data-warehouse:5432/yellow_tripdata'
DST_CONN = 'postgresql://postgres:admin@data-mart:5432/yellow_tripdata'

# Engines SQLAlchemy
engine_src = create_engine(SRC_CONN)
engine_dst = create_engine(DST_CONN)

# Default DAG args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': 300,
}

with DAG(
    dag_id='migrate_datawarehouse_to_datamart',
    default_args=default_args,
    description='Migration des dimensions et faits vers Data Mart via pandas',
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['migration', 'postgres'],
) as dag:

    def test_connections(**kwargs):
        """
        Vérifie la connexion aux bases source et destination.
        """
        try:
            with engine_src.connect() as conn:
                conn.execute(text('SELECT 1'))
            logging.info('Connection to source database OK')
        except Exception as e:
            logging.error(f"Source DB connection failed: {e}")
            raise AirflowFailException(f"Source DB connection failed: {e}")
        try:
            with engine_dst.connect() as conn:
                conn.execute(text('SELECT 1'))
            logging.info('Connection to destination database OK')
        except Exception as e:
            logging.error(f"Destination DB connection failed: {e}")
            raise AirflowFailException(f"Destination DB connection failed: {e}")

    def migrate_dim_vendor(**kwargs):
        df = pd.read_sql(
            'SELECT DISTINCT vendorid AS vendor_id FROM public.yellow_tripdata WHERE vendorid IS NOT NULL',
            engine_src
        )
        try:
            existing = pd.read_sql('SELECT vendor_id FROM dim_vendor', engine_dst)
            df = df[~df['vendor_id'].isin(existing['vendor_id'])]
        except Exception:
            pass
        if not df.empty:
            df.to_sql('dim_vendor', engine_dst, if_exists='append', index=False)
            logging.info(f"Loaded {len(df)} rows into dim_vendor")

    def migrate_dim_ratecode(**kwargs):
        df = pd.read_sql(
            'SELECT DISTINCT ratecodeid AS ratecode_id FROM public.yellow_tripdata WHERE ratecodeid IS NOT NULL',
            engine_src
        )
        try:
            existing = pd.read_sql('SELECT ratecode_id FROM dim_ratecode', engine_dst)
            df = df[~df['ratecode_id'].isin(existing['ratecode_id'])]
        except Exception:
            pass
        if not df.empty:
            df.to_sql('dim_ratecode', engine_dst, if_exists='append', index=False)
            logging.info(f"Loaded {len(df)} rows into dim_ratecode")

    def migrate_dim_payment(**kwargs):
        df = pd.read_sql(
            'SELECT DISTINCT payment_type AS payment_type_id FROM public.yellow_tripdata WHERE payment_type IS NOT NULL',
            engine_src
        )
        try:
            existing = pd.read_sql('SELECT payment_type_id FROM dim_payment', engine_dst)
            df = df[~df['payment_type_id'].isin(existing['payment_type_id'])]
        except Exception:
            pass
        if not df.empty:
            df.to_sql('dim_payment', engine_dst, if_exists='append', index=False)
            logging.info(f"Loaded {len(df)} rows into dim_payment")

    def migrate_dim_location(**kwargs):
        df1 = pd.read_sql(
            'SELECT DISTINCT pulocationid AS location_id FROM public.yellow_tripdata WHERE pulocationid IS NOT NULL',
            engine_src
        )
        df2 = pd.read_sql(
            'SELECT DISTINCT dolocationid AS location_id FROM public.yellow_tripdata WHERE dolocationid IS NOT NULL',
            engine_src
        )
        df = pd.concat([df1, df2]).drop_duplicates(subset=['location_id'])
        try:
            existing = pd.read_sql('SELECT location_id FROM dim_location', engine_dst)
            df = df[~df['location_id'].isin(existing['location_id'])]
        except Exception:
            pass
        if not df.empty:
            df['location_description'] = df['location_id'].apply(lambda x: f"Location {x}")
            df.to_sql('dim_location', engine_dst, if_exists='append', index=False)
            logging.info(f"Loaded {len(df)} rows into dim_location")

    def migrate_dim_datetime(**kwargs):
        q = (
            "SELECT tpep_pickup_datetime AS dt FROM public.yellow_tripdata UNION "
            "SELECT tpep_dropoff_datetime FROM public.yellow_tripdata"
        )
        df = pd.read_sql(q, engine_src)
        df = df.dropna().drop_duplicates(subset=['dt'])
        df['date_value'] = df['dt'].dt.date
        df['time_value'] = df['dt'].dt.time
        df['day'] = df['dt'].dt.day
        df['month'] = df['dt'].dt.month
        df['year'] = df['dt'].dt.year
        df['hour'] = df['dt'].dt.hour
        df['minute'] = df['dt'].dt.minute
        df['day_of_week'] = df['dt'].dt.day_name()
        df = df.drop(columns=['dt'])
        df = df.reset_index(drop=True).reset_index().rename(columns={'index':'datetime_id'})
        df.to_sql('dim_datetime', engine_dst, if_exists='replace', index=False)
        logging.info(f"Replaced dim_datetime with {len(df)} rows")

    def migrate_fact_trips(**kwargs):
        """
        Charge la table fact_trips en joignant sur les dimensions.
        """
        df = pd.read_sql('SELECT * FROM public.yellow_tripdata', engine_src)
        df.columns = [c.lower() for c in df.columns]

        dim_v = pd.read_sql('SELECT vendor_id AS vendor_key FROM dim_vendor', engine_dst)
        dim_r = pd.read_sql('SELECT ratecode_id AS ratecode_key FROM dim_ratecode', engine_dst)
        dim_p = pd.read_sql('SELECT payment_type_id AS payment_type_key FROM dim_payment', engine_dst)
        dim_l = pd.read_sql('SELECT location_id AS location_key FROM dim_location', engine_dst)
        dim_dt = pd.read_sql('SELECT datetime_id, date_value, time_value FROM dim_datetime', engine_dst)
        dim_dt['datetime'] = pd.to_datetime(dim_dt['date_value'].astype(str) + ' ' + dim_dt['time_value'].astype(str))

        df = df.merge(dim_v, left_on='vendorid', right_on='vendor_key', how='left')
        df = df.merge(dim_r, left_on='ratecodeid', right_on='ratecode_key', how='left')
        df = df.merge(dim_p, left_on='payment_type', right_on='payment_type_key', how='left')
        df = df.merge(dim_l.rename(columns={'location_key':'pickup_location_key'}), left_on='pulocationid', right_on='pickup_location_key', how='left')
        df = df.merge(dim_l.rename(columns={'location_key':'dropoff_location_key'}), left_on='dolocationid', right_on='dropoff_location_key', how='left')

        df['pickup_dt'] = pd.to_datetime(df['tpep_pickup_datetime'])
        df = df.merge(dim_dt[['datetime_id','datetime']], left_on='pickup_dt', right_on='datetime', how='left')
        df = df.rename(columns={'datetime_id':'pickup_datetime_id'}).drop(columns=['datetime','pickup_dt'])

        df['dropoff_dt'] = pd.to_datetime(df['tpep_dropoff_datetime'])
        df = df.merge(dim_dt[['datetime_id','datetime']], left_on='dropoff_dt', right_on='datetime', how='left')
        df = df.rename(columns={'datetime_id':'dropoff_datetime_id'}).drop(columns=['datetime','dropoff_dt'])

        fact_cols = [
            'vendor_key','pickup_datetime_id','dropoff_datetime_id',
            'passenger_count','trip_distance','ratecode_key',
            'store_and_fwd_flag','pickup_location_key','dropoff_location_key',
            'payment_type_key','fare_amount','extra','mta_tax',
            'tip_amount','tolls_amount','improvement_surcharge',
            'total_amount','congestion_surcharge','airport_fee'
        ]
        fact_df = df[fact_cols]
        if not fact_df.empty:
            fact_df.to_sql('fact_trips', engine_dst, if_exists='append', index=False)
            logging.info(f"Loaded {len(fact_df)} rows into fact_trips")

    # Tasks
    t0 = PythonOperator(task_id='test_connections', python_callable=test_connections)
    t1 = PythonOperator(task_id='migrate_dim_vendor', python_callable=migrate_dim_vendor)
    t2 = PythonOperator(task_id='migrate_dim_ratecode', python_callable=migrate_dim_ratecode)
    t3 = PythonOperator(task_id='migrate_dim_payment', python_callable=migrate_dim_payment)
    t4 = PythonOperator(task_id='migrate_dim_location', python_callable=migrate_dim_location)
    t5 = PythonOperator(task_id='migrate_dim_datetime', python_callable=migrate_dim_datetime)
    t6 = PythonOperator(task_id='migrate_fact_trips', python_callable=migrate_fact_trips)

    t0 >> [t1, t2, t3, t4, t5] >> t6
