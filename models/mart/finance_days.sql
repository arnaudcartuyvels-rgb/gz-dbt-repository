-- models/mart/finance_days.sql
with ord as (
  select orders_id, date_date, revenue, quantity, purchase_cost
  from {{ ref('int_orders_margin') }}
),
op as (
  select orders_id, date_date, operational_margin, shipping_fee, log_cost, ship_cost
  from {{ ref('int_orders_operational') }}
)

select
  ord.date_date                                   as date,
  count(distinct ord.orders_id)                   as transactions,
  ROUND (sum(ord.revenue),2)                                as revenue,
  ROUND (safe_divide(sum(ord.revenue), count(distinct ord.orders_id)),2) as average_basket,
  ROUND (sum(op.operational_margin),2)                      as operational_margin,
  ROUND (sum(ord.purchase_cost),2)                          as purchase_cost,
  ROUND (sum(op.shipping_fee),2)                            as shipping_fees,
  ROUND (sum(op.log_cost),2)                                as log_costs,
  ROUND (sum(ord.quantity),2)                               as quantity
from ord
left join op using (orders_id, date_date)
group by ord.date_date
order by ord.date_date



