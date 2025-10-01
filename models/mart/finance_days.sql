-- models/mart/finance_days.sql

with orders as (
  -- per-order metrics (date, revenue, quantity, purchase_cost, margin)
  select * from {{ ref('int_orders_margin') }}
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
  o.date_date                                                as date,
  count(distinct o.orders_id)                                as transactions,
  round(sum(o.revenue), 2)                                   as revenue,
  round(safe_divide(sum(o.revenue), count(distinct o.orders_id)), 2) as average_basket,
  round(
    sum(
      o.margin
      + coalesce(s.shipping_fee, 0)
      - coalesce(s.log_cost,    0)
      - coalesce(s.ship_cost,   0)
    ),
    2
  )                                                          as operational_margin,
  round(sum(o.purchase_cost), 2)                             as purchase_cost,
  round(sum(coalesce(s.shipping_fee, 0)), 2)                 as shipping_fees,
  round(sum(coalesce(s.log_cost,      0)), 2)                as log_costs,
  sum(o.quantity)                                            as quantity
from orders o
left join ship s
  on s.orders_id = o.orders_id
group by
  o.date_date
order by
  o.date_date

