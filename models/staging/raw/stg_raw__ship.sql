-- models/staging/raw/stg_raw__ship.sql
with source as (
  select * from {{ source('raw','ship') }}
),
renamed as (
  select
    orders_id,
    shipping_fee,                             -- keep this one
    cast(ship_cost as float64) as ship_cost,
    cast(logCost  as float64) as log_cost     -- << new
  from source
)
select * from renamed
