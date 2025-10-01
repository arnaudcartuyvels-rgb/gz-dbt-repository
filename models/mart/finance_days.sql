-- models/mart/finance_days.sql

with orders as (
  -- per-order metrics (date, revenue, quantity, purchase_cost, margin)
  select * from {{ ref('int_orders_margin') }}
),
ship as (
  -- per-order shipping costs/fees (typed)
  select
    orders_id,
    shipping_fee,
    cast(logCost as float64)  as log_cost,
    ship_cost
  from {{ ref('stg_raw__ship') }}
)

select
  o.date_date                                        as date,
  count(distinct o.orders_id)                        as transactions,
  sum(o.revenue)                                     as revenue,
  safe_divide(sum(o.revenue), count(distinct o.orders_id)) as average_basket,
  sum(
    o.margin
    + coalesce(s.shipping_fee, 0)
    - coalesce(s.log_cost,     0)
    - coalesce(s.ship_cost,    0)
  )                                                  as operational_margin,
  sum(o.purchase_cost)                               as purchase_cost,
  sum(coalesce(s.shipping_fee, 0))                   as shipping_fees,
  sum(coalesce(s.log_cost,     0))                   as log_costs,
  sum(o.quantity)                                    as quantity
from orders o
left join ship s
  on s.orders_id = o.orders_id
group by
  o.date_date
order by
  o.date_date
