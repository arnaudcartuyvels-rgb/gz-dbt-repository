with orders as (
  select * from {{ ref('int_orders_margin') }}
),
ship as (
  select
    orders_id,
    shipping_fee,
    ship_cost,
    log_cost
  from {{ ref('stg_raw__ship') }}
)

select
  o.orders_id,
  o.date_date,
  round(
    o.margin
    + coalesce(s.shipping_fee, 0)
    - coalesce(s.log_cost,   0)
    - coalesce(s.ship_cost,  0)
  , 2) as operational_margin
from orders o
left join ship s using (orders_id)

