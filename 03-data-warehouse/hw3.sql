-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE plucky - spirit -412403.nytaxi.external_green_tripdata OPTIONS (
        format = 'PARQUET',
        uris = ['gs://de-zoomcamp-bq-1/green_taxi/*']
    );
-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE plucky - spirit -412403.nytaxi.green_tripdata_nonpartitioned AS
SELECT *
FROM plucky - spirit -412403.nytaxi.external_green_tripdata;
-- Question 1
SELECT COUNT(*)
FROM plucky - spirit -412403.nytaxi.external_green_tripdata;
-- Question 2
-- External table
SELECT COUNT(DISTINCT(PULocationID))
FROM plucky - spirit -412403.nytaxi.external_green_tripdata;
-- Internal table
SELECT COUNT(DISTINCT(PULocationID))
FROM plucky - spirit -412403.nytaxi.green_tripdata_nonpartitioned;
-- Question 3
SELECT COUNT(*)
FROM plucky - spirit -412403.nytaxi.green_tripdata_nonpartitioned
WHERE fare_amount = 0;
-- Question 4
CREATE OR REPLACE TABLE plucky - spirit -412403.nytaxi.green_tripdata_partitioned PARTITION BY DATE(lpep_pickup_datetime) CLUSTER BY PUlocationID AS (
        SELECT *
        FROM plucky - spirit -412403.nytaxi.external_green_tripdata
    );
-- Question 5
SELECT DISTINCT(PULocationID)
FROM plucky - spirit -412403.nytaxi.green_tripdata_nonpartitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';
SELECT DISTINCT(PULocationID)
FROM plucky - spirit -412403.nytaxi.green_tripdata_partitioned
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';