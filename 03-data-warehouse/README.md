# Module 3 - Data Warehouse (GCS BigQuery) Study Notes

In this module, we would explore concepts related to data warehouse. The choice of data warehouse for this module is GCS BigQuery.

## OLAP vs OLTP
There are 2 types of database architectures that help us store and analyze business data, namely OLAP (online analytical processing) and OLTP (online transaction processing). 

The key differences can be summarize to the following table

|  | OLTP  | OLAP |
| --- | --- | --- |
| Purpose | Control and run essential business operations in real time  | Plan, solve problems, support decisions, discover hidden insights |
| Data source | Real-time and transactional data from a single source  | Historical and aggregated data from multiple sources |
| Operations | Based on INSERT, UPDATE, DELETE commands  | Based on SELECT commands to aggregate data for reporting |
| Data updates | Short, fast updates initiated by user  | Data periodically refreshed with scheduled, long-running batch jobs |
| Volume of data | Smaller storage requirements. Think gigabytes (GB). | Large storage requirements. Think terabytes (TB) and petabytes (PB). |
| Response time | Shorter response times, typically in milliseconds  | Longer response times, typically in seconds or minutes |
| Backup and recovery | Regular backups required to ensure business continuity and meet legal and governance requirements  | Lost data can be reloaded from OLTP database as needed in lieu of regular backups |
| Database design | Normalized databases for efficiency  | Denormalized databases for analysis |
| Example | MySQL, PostgreSQL, Oracle Database  | Amazon Redshift, Google BigQuery, Snowflake |

> **NOTE:**
> **Normalized databases** reduce data redundancy and ensure data integrity by dividing data into multiple, connected tables based on specific rules (normal forms).

> **NOTE:**
> **Denormalized databases** improve query performance at the expense of some data redundancy by combining data from multiple tables into a single table or flattens data structures.
## Data Warehouse
Data warehouse uses a ETL process unlike datalake which uses a ELT process. 

## Google BigQuery

### Load data to Google Cloud Storage (GCS)
In order to automate the loading of NY taxi data from [NY Taxi record page](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) to Google Cloud Storage, I had created a data ingestion script which utilizes wget and Cloud Storage Python API. The script can be run with the following command:
```python
python3 data_ingestion_to_gcs.py [--year YEAR] [--color COLOR] [--bucket BUCKET] [--blob BLOB]
```

### External Table
Google BigQuery allows querying data stored outside of BigQuery. External tables are similar to standard BigQuery tables, in that these tables store their metadata and schema in BigQuery storage. However, their data resides in an external source. In our case, we would be querying from Google Cloud Storage. 

To create external table from GCS path, we could execute the following sql statement
```sql
-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `taxi-rides-ny.nytaxi.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://nyc-tl-data/yellow_taxi/yellow_tripdata_2019-*.parquet', 'gs://nyc-tl-data/yellow_taxi/yellow_tripdata_2020-*.parquet']
);
```

You may perform SQL query on the external table.
> **NOTE:** 
> BQ is unable to determine the number of rows for external table.

To convert external table to internal table, we could execute the following sql statement.
```sql
-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE taxi-rides-ny.nytaxi.yellow_tripdata_non_partitoned AS
SELECT * FROM taxi-rides-ny.nytaxi.external_yellow_tripdata;
```
### Partitioned in BQ

## References
1. [DTalks-DataEng-Data Warehouse](https://docs.google.com/presentation/d/1a3ZoBAXFk8-EhUsd7rAZd-5p_HpltkzSeujjRGB2TAI/edit#slide=id.g10eebc44ce4_0_0)
2. [What’s the Difference Between OLAP and OLTP?](https://aws.amazon.com/compare/the-difference-between-olap-and-oltp/)