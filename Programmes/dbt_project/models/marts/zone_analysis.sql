select
    pulocationid,
    count(*) as trip_count,
    avg(total_amount) as avg_revenue,
    sum(total_amount) as total_revenue
from {{ ref('int_trip_metrics') }}
group by pulocationid
order by trip_count desc
