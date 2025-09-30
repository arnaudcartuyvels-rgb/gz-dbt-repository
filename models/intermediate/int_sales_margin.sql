with
sales as (
  select
    date_date,
    orders_id,
    products_id,
    cast(quantity as int64)   as quantity,
    cast(revenue  as float64) as revenue
  from {{ ref('stg_raw__sales') }}
),
product as (
  select
    products_id,
    cast(purchase_price as float64) as purchase_price
  from {{ ref('stg_raw__product') }}
)

select
  s.date_date,
  s.orders_id,
  s.products_id,
  s.quantity,
  s.revenue,
  p.purchase_price,
  s.quantity * coalesce(p.purchase_price, 0)                as purchase_cost,
  s.revenue - (s.quantity * coalesce(p.purchase_price, 0))  as margin
from sales s
left join product p
  on p.products_id = s.products_id
