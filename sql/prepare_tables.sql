create table if not exists finance.rides_stg
(
    dt           varchar,
    payment_type varchar,
    rides        varchar,
    amount       varchar
);

truncate table finance.rides_stg;

create table if not exists finance.rides
(
    dt           date,
    payment_type varchar,
    ride         int,
    amount       Decimal(12, 2)
);