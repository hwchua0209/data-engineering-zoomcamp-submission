-- Load fhv csv file from GCS as external table
CREATE OR REPLACE EXTERNAL TABLE plucky-spirit-412403.trips_data_all.fhv_data_external
  OPTIONS (
    format = 'CSV',
    uris = ['gs://de-zoomcamp-bq-2/fhv/*']
    );

-- Convert external table to unpartitioned table
CREATE OR REPLACE TABLE plucky-spirit-412403.trips_data_all.fhv_data AS
SELECT * FROM plucky-spirit-412403.trips_data_all.fhv_data_external;
