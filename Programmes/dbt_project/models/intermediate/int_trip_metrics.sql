with base as (
    select * from {{ ref('stg_yellow_taxi_trips') }}
)
select
    *,
    datediff('minute', try_to_timestamp(tpep_pickup_datetime), try_to_timestamp(tpep_dropoff_datetime)) as trip_duration_minutes,
    extract(hour from try_to_timestamp(tpep_pickup_datetime)) as pickup_hour,
    extract(day from try_to_timestamp(tpep_pickup_datetime)) as pickup_day,
    extract(month from try_to_timestamp(tpep_pickup_datetime)) as pickup_month,
    case when datediff('second', try_to_timestamp(tpep_pickup_datetime), try_to_timestamp(tpep_dropoff_datetime)) > 0
        then trip_distance * 3600.0 / datediff('second', try_to_timestamp(tpep_pickup_datetime), try_to_timestamp(tpep_dropoff_datetime))
        else null end as avg_speed_mph,
    case when fare_amount > 0 then tip_amount / fare_amount else null end as tip_pct
from base
