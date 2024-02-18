{{
    config(
        materialized='table'
    )
}}

with 
fhv_trip_data as (
    select * from {{ ref('stg_fhv_data') }}
),

dim_zones as (
    select * from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select 
    fhv.dispatching_base_num,
    fhv.pickup_datetime,
    fhv.pickup_year,
    fhv.dropoff_datetime,
    fhv.pickup_locationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhv.dropoff_locationid,
    fhv.sr_flag,
    fhv.affiliated_base_number,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone, 
from fhv_trip_data as fhv
inner join dim_zones as pickup_zone
on pickup_zone.locationid = fhv.pickup_locationid
inner join dim_zones as dropoff_zone
on dropoff_zone.locationid = fhv.dropoff_locationid