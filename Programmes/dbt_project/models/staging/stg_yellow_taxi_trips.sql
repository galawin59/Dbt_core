with source as (
    select * from RAW.yellow_taxi_trips_2024_01
    union all select * from RAW.yellow_taxi_trips_2024_02
    -- Ajoute tous les mois nécessaires
)
select
    -- Nettoyage des valeurs aberrantes
    *
from source
where
    trip_distance between 0.1 and 100
    and total_amount > 0
    and fare_amount > 0
    and tpep_pickup_datetime is not null
    and tpep_dropoff_datetime is not null
    and try_to_timestamp(tpep_pickup_datetime) < try_to_timestamp(tpep_dropoff_datetime)
    and pulocationid is not null
    and dolocationid is not null
