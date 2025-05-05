-- creation.sql
-- ---------------

-- 1. Dimension : Fournisseur
CREATE TABLE dim_vendor (
    vendor_id   INT   PRIMARY KEY,
    vendor_name VARCHAR(255)
);

-- 2. Dimension : DateTime
CREATE TABLE dim_datetime (
    datetime_id  SERIAL PRIMARY KEY,
    date_value   DATE    NOT NULL,
    time_value   TIME    NOT NULL,
    day          INT,
    month        INT,
    year         INT,
    hour         INT,
    minute       INT,
    day_of_week  VARCHAR(10)
);

-- 3. Dimension : Nombre de passagers
CREATE TABLE dim_passenger_count (
    passenger_count INT PRIMARY KEY
);

-- 4. Dimension : Ratecode
CREATE TABLE dim_ratecode (
    ratecode_id   INT   PRIMARY KEY,
    ratecode_name VARCHAR(255)
);

-- 5. Dimension : Store and Fwd Flag
CREATE TABLE dim_store_flag (
    flag        CHAR(1) PRIMARY KEY,  -- 'Y' ou 'N'
    description VARCHAR(50)
);

-- 6. Dimension : Localisation
CREATE TABLE dim_location (
    location_id          INT   PRIMARY KEY,
    location_description VARCHAR(100)
);

-- 7. Dimension : Payment Type
CREATE TABLE dim_payment (
    payment_type_id   INT   PRIMARY KEY,
    payment_type_name VARCHAR(50)
);

-- 8. Table de faits : Fact_Trips
CREATE TABLE fact_trips (
    trip_id                  SERIAL PRIMARY KEY,
    vendor_id                INT    REFERENCES dim_vendor(vendor_id),
    pickup_datetime_id       INT    REFERENCES dim_datetime(datetime_id),
    dropoff_datetime_id      INT    REFERENCES dim_datetime(datetime_id),
    passenger_count          INT    REFERENCES dim_passenger_count(passenger_count),
    trip_distance            NUMERIC,
    ratecode_id              INT    REFERENCES dim_ratecode(ratecode_id),
    store_flag               CHAR(1) REFERENCES dim_store_flag(flag),
    pulocation_id            INT    REFERENCES dim_location(location_id),
    dolocation_id            INT    REFERENCES dim_location(location_id),
    payment_type_id          INT    REFERENCES dim_payment(payment_type_id),
    fare_amount              NUMERIC,
    extra                    NUMERIC,
    mta_tax                  NUMERIC,
    tip_amount               NUMERIC,
    tolls_amount             NUMERIC,
    improvement_surcharge    NUMERIC,
    total_amount             NUMERIC,
    congestion_surcharge     NUMERIC,
    airport_fee              NUMERIC
);