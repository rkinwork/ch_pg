delete
from finance.rides r
where r.dt in (select distinct cast(dt as date) from finance.rides_stg);

insert into finance.rides(dt, payment_type, ride, amount)
SELECT cast(dt as date),
       payment_type,
       cast(rides as int),
       cast(amount as Decimal(12, 2))
from finance.rides_stg;