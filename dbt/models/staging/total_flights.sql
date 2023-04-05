{{ config(materialized='view') }}
 
select
    -- flight info
    cast(FlightDate as timestamp) as FlightDate,
    cast(Origin as string) as Origin,
    cast(Dest as string) as Dest,
    Cancelled,
    Diverted,
    cast(CRSDepTime as integer) as CRSDepTime,
    cast(DepTime as numeric) as DepTime,
    cast(DepDelayMinutes as numeric) as DepDelayMinutes,
    cast(CRSArrTime as integer) as CRSArrTime,
    cast(ArrTime as numeric) as ArrTime,
    cast(ArrDelayMinutes as numeric) as ArrDelayMinutes,

    -- airline info
    cast(Airline as string) as Airline,
    cast(Flight_Number_Marketing_Airline as integer) as Flight_Number_Marketing_Airline,

    -- taxi info
    cast(TaxiOut as numeric) as TaxiOut,
    cast(TaxiIn as numeric) as TaxiIn,

    -- airport info
    cast(OriginAirportID as integer) as OriginAirportID,
    cast(OriginCityName as string) as OriginCityName,
    cast(OriginStateName as string) as OriginStateName,
    cast(DestAirportID as integer) as DestAirportID,
    cast(DestCityName as string) as DestCityName,
    cast(DestStateName as string) as DestStateName,

   -- identifiers
    {{ dbt_utils.surrogate_key(['vendorid', 'tpep_pickup_datetime']) }} as tripid,
    cast(vendorid as integer) as vendorid,
    cast(ratecodeid as integer) as ratecodeid,
    cast(pulocationid as integer) as  pickup_locationid,
    cast(dolocationid as integer) as dropoff_locationid,
    
    -- timestamps
    cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
    
    -- trip info
    store_and_fwd_flag,
    cast(passenger_count as integer) as passenger_count,
    cast(trip_distance as numeric) as trip_distance,
    -- yellow cabs are always street-hail
    1 as trip_type,
    
    -- payment info
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(0 as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as integer) as payment_type,
    {{ get_payment_type_description('payment_type') }} as payment_type_description, 
    cast(congestion_surcharge as numeric) as congestion_surcharge
from {{ source('staging','flights_external') }}


-- dbt build --m <model.sql> --var 'is_test_run: false'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}

