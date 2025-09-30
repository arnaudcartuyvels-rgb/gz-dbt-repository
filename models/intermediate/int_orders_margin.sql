with line as (
  -- lignes produit avec coûts/marge
  select *
  from {{ ref('int_sales_margin') }}
)

select
  orders_id,
  any_value(date_date) as date_date,   -- une seule date par commande
  sum(revenue)        as revenue,
  sum(quantity)       as quantity,
  sum(purchase_cost)  as purchase_cost,
  sum(margin)         as margin
from line
group by orders_id
