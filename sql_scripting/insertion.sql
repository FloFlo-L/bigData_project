-- insertion.sql
-- ---------------
-- Alimente le Data Mart (modèle flocon) depuis le Data Warehouse via dblink

-- 1) Active l’extension dblink
CREATE EXTENSION IF NOT EXISTS dblink;

-- 2) Connexion vers le Data Warehouse (URI libpq)
--    Remplacez user:password@host:port/dbname si besoin
--    Ici, service Docker-compose est nommé "data-warehouse"
--    Hostname = data-warehouse, port = 5432, db = yellow_taxi
--    User = postgres, password = admin

-- 3) Remplissage des dimensions

-- 3.1 dim_vendor
INSERT INTO dim_vendor (vendor_id, vendor_name)
SELECT vid,
       CASE vid
         WHEN 1 THEN 'Vendor 1'
         WHEN 2 THEN 'Vendor 2'
         ELSE 'Unknown'
       END
FROM dblink(
       'postgresql://postgres:admin@data-warehouse:5432/yellow_taxi',
       'SELECT DISTINCT "VendorID" FROM public.yellow_tripdata'
     ) AS t(vid INT);

-- 3.2 dim_datetime
INSERT INTO dim_datetime (
    date_value,
    time_value,
    day, month, year,
    hour, minute,
    day_of_week
)
SELECT DISTINCT
    dt::DATE,
    dt::TIME,
    EXTRACT(DAY   FROM dt)::INT,
    EXTRACT(MONTH FROM dt)::INT,
    EXTRACT(YEAR  FROM dt)::INT,
    EXTRACT(HOUR  FROM dt)::INT,
    EXTRACT(MINUTE FROM dt)::INT,
    TO_CHAR(dt, 'FMDay')
FROM dblink(
       'postgresql://postgres:admin@data-warehouse:5432/yellow_taxi',
       $$
         SELECT tpep_pickup_datetime AS dt FROM public.yellow_tripdata
         UNION
         SELECT tpep_dropoff_datetime     FROM public.yellow_tripdata
       $$
     ) AS s(dt TIMESTAMP);

-- 3.3 dim_passenger_count (on exclut les NULL pour respecter la contrainte NOT NULL)
INSERT INTO dim_passenger_count (passenger_count)
SELECT DISTINCT pc
FROM dblink(
       'postgresql://postgres:admin@data-warehouse:5432/yellow_taxi',
       'SELECT DISTINCT passenger_count FROM public.yellow_tripdata'
     ) AS s(pc INT)
WHERE pc IS NOT NULL;

-- 3.4 dim_ratecode
INSERT INTO dim_ratecode (ratecode_id, ratecode_name)
SELECT DISTINCT rc,
       CASE rc
         WHEN 1 THEN 'Standard rate'
         WHEN 2 THEN 'JFK'
         WHEN 3 THEN 'Newark'
         WHEN 4 THEN 'Nassau/Westchester'
         ELSE 'Other'
       END
FROM dblink(
       'postgresql://postgres:admin@data-warehouse:5432/yellow_taxi',
       'SELECT DISTINCT "RatecodeID" FROM public.yellow_tripdata'
     ) AS s(rc INT)
WHERE rc IS NOT NULL;

-- 3.5 dim_store_flag
INSERT INTO dim_store_flag (flag, description)
SELECT sf,
       CASE sf
         WHEN 'Y' THEN 'Store and Forward'
         WHEN 'N' THEN 'Not Store and Forward'
         ELSE 'Unknown'
       END
FROM dblink(
       'postgresql://postgres:admin@data-warehouse:5432/yellow_taxi',
       'SELECT DISTINCT store_and_fwd_flag FROM public.yellow_tripdata'
     ) AS s(sf CHAR(1))
WHERE sf IS NOT NULL;

-- 3.6 dim_location
INSERT INTO dim_location (location_id, location_description)
SELECT loc,
       'Location ' || loc
FROM dblink(
       'postgresql://postgres:admin@data-warehouse:5432/yellow_taxi',
       $$
         SELECT "PULocationID" AS loc FROM public.yellow_tripdata
         UNION
         SELECT "DOLocationID"       FROM public.yellow_tripdata
       $$
     ) AS s(loc INT);

-- 3.7 dim_payment
INSERT INTO dim_payment (payment_type_id, payment_type_name)
SELECT pt,
       CASE pt
         WHEN 1 THEN 'Credit Card'
         WHEN 2 THEN 'Cash'
         WHEN 3 THEN 'No Charge'
         WHEN 4 THEN 'Dispute'
         WHEN 5 THEN 'Unknown'
         ELSE 'Other'
       END
FROM dblink(
       'postgresql://postgres:admin@data-warehouse:5432/yellow_taxi',
       'SELECT DISTINCT payment_type FROM public.yellow_tripdata'
     ) AS s(pt INT);


-- 4) Remplissage de la table de faits fact_trips
INSERT INTO fact_trips (
    vendor_id,
    pickup_datetime_id,
    dropoff_datetime_id,
    passenger_count,
    trip_distance,
    ratecode_id,
    store_flag,
    pulocation_id,
    dolocation_id,
    payment_type_id,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee
)
SELECT
    src.vendid,
    pd.datetime_id,
    dd.datetime_id,
    src.passenger_count,
    src.trip_distance,
    src.ratecodeid,
    src.store_and_fwd_flag,
    src.pulocationid,
    src.dolocationid,
    src.payment_type,
    src.fare_amount,
    src.extra,
    src.mta_tax,
    src.tip_amount,
    src.tolls_amount,
    src.improvement_surcharge,
    src.total_amount,
    src.congestion_surcharge,
    src.airport_fee
FROM dblink(
       'postgresql://postgres:admin@data-warehouse:5432/yellow_taxi',
       $$
         SELECT
           "VendorID"              AS vendid,
           tpep_pickup_datetime,
           tpep_dropoff_datetime,
           passenger_count,
           trip_distance,
           "RatecodeID"            AS ratecodeid,
           store_and_fwd_flag,
           "PULocationID"          AS pulocationid,
           "DOLocationID"          AS dolocationid,
           payment_type,
           fare_amount,
           extra,
           mta_tax,
           tip_amount,
           tolls_amount,
           improvement_surcharge,
           total_amount,
           congestion_surcharge,
           "Airport_fee"           AS airport_fee
         FROM public.yellow_tripdata
       $$
     ) AS src(
         vendid                   INT,
         tpep_pickup_datetime     TIMESTAMP,
         tpep_dropoff_datetime    TIMESTAMP,
         passenger_count          INT,
         trip_distance            NUMERIC,
         ratecodeid               INT,
         store_and_fwd_flag       CHAR(1),
         pulocationid             INT,
         dolocationid             INT,
         payment_type             INT,
         fare_amount              NUMERIC,
         extra                    NUMERIC,
         mta_tax                  NUMERIC,
         tip_amount               NUMERIC,
         tolls_amount             NUMERIC,
         improvement_surcharge    NUMERIC,
         total_amount             NUMERIC,
         congestion_surcharge     NUMERIC,
         airport_fee              NUMERIC
     )
LEFT JOIN dim_datetime pd
  ON pd.date_value = src.tpep_pickup_datetime::date
 AND pd.time_value = src.tpep_pickup_datetime::time
LEFT JOIN dim_datetime dd
  ON dd.date_value = src.tpep_dropoff_datetime::date
 AND dd.time_value = src.tpep_dropoff_datetime::time;