-- models/mart/finance/finance_days.sql

with ord as (
    select
        orders_id,
        date_date,
        revenue,
        quantity,
        purchase_cost,
        margin                  
    from {{ ref('int_orders_margin') }}
),

op as (
    select
        orders_id,
        date_date,
        operational_margin,
        shipping_fee,
        log_cost,
        ship_cost
    from {{ ref('int_orders_operational') }}
)

select
  ord.date_date                                        as date,
  count(distinct ord.orders_id)                        as transactions,
  round(sum(ord.revenue), 2)                           as revenue,
  round(safe_divide(sum(ord.revenue), count(distinct ord.orders_id)), 2) as average_basket,
  round(sum(op.operational_margin), 2)                 as operational_margin,
  round(sum(ord.purchase_cost), 2)                     as purchase_cost,
  round(sum(op.shipping_fee), 2)                       as shipping_fees,
  round(sum(op.log_cost), 2)                           as log_costs,
  round(sum(op.ship_cost), 2)                          as ship_cost,  
  round(sum(ord.quantity), 2)                          as quantity,
  round(sum(ord.margin), 2)                            as margin    
from ord
left join op using (orders_id, date_date)
group by ord.date_date
order by ord.date_date


