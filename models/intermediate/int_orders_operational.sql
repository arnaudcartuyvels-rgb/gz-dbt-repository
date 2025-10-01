with orders as (
  select *
  from {{ ref('int_orders_margin') }}
),
ship as (
  select
    orders_id,
    shipping_fee,
    log_cost,      
    ship_cost      
  from {{ ref('stg_raw__ship') }}
)

select
  o.orders_id,
  o.date_date,
  round(
    o.margin
    + coalesce(s.shipping_fee, 0)
    - coalesce(s.ship_cost, 0)
  , 2) as operational_margin,
  coalesce(s.shipping_fee, 0) as shipping_fee,
  coalesce(s.log_cost,   0)   as log_cost,
  coalesce(s.ship_cost,  0)   as ship_cost
from orders o
left join ship s using (orders_id)
