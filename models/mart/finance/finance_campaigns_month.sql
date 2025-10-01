select
  date_trunc(date, month) as datemonth,
  round(sum(operational_margin) - sum(ads_cost), 2) as ads_margin,
  round(safe_divide(sum(revenue), nullif(sum(quantity), 0)), 2) as average_basket,
  round(sum(operational_margin), 2) as operational_margin,
  round(sum(ads_cost), 2) as ads_cost,
  sum(ads_impression) as ads_impression,
  sum(ads_clicks) as ads_clicks,
  sum(quantity) as quantity,
  round(sum(revenue), 2) as revenue,
  round(sum(purchase_cost), 2) as purchase_cost,
  round(sum(margin), 2) as margin,
  round(sum(shipping_fee), 2) as shipping_fee,
  round(sum(log_cost), 2) as log_cost,
  round(sum(ship_cost), 2) as ship_cost
from {{ ref('finance_campaigns_day') }}
group by datemonth
order by datemonth desc

