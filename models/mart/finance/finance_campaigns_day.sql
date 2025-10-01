{{ config(materialized='view') }}

with c as (
  -- ads aggregated by day
  select
    date,
    ads_cost,
    ads_impression,
    ads_clicks
  from {{ ref('int_campaigns_day') }}
),
f as (
  -- finance KPIs at day grain
  select * from {{ ref('finance_days') }}
)

select
  f.date,

  -- make ads_cost zero when there's no matching row on the right,
  -- so ads_margin never becomes NULL
  f.operational_margin - coalesce(c.ads_cost, 0) as ads_margin,

  -- keep required columns in requested order; coalesce ad fields AFTER the join
  f.average_basket,
  f.operational_margin,
  coalesce(c.ads_cost,       0) as ads_cost,
  coalesce(c.ads_impression, 0) as ads_impression,
  coalesce(c.ads_clicks,     0) as ads_clicks,
  f.quantity,
  f.revenue,
  f.purchase_cost,
  f.margin,
  f.shipping_fees,
  f.log_costs,
  f.ship_cost
from f
left join c
  on f.date = c.date

