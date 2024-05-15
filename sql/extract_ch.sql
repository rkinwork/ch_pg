select toStartOfMonth(pickup_datetime),
       payment_type,
       count(trip_id)              as rides,
       floor(sum(total_amount), 2) as amount
from default.trips
where toStartOfMonth(pickup_datetime) = toDate({dt:String})
group by toStartOfMonth(pickup_datetime), payment_type