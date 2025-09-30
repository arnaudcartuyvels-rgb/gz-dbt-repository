-- models/staging/raw/stg_raw__ship.sql
with source as (
  select * from {{ source('raw', 'ship') }}
),
renamed as (
  select
    orders_id,
    shipping_fee,                       -- keep only one of the two
    cast(ship_cost as float64) as ship_cost
  from source
)
select * from renamed
