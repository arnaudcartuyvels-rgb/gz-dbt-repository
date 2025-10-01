with all_campaigns as (

    select * from {{ ref('stg_raw__adwords') }}
    union all
    select * from {{ ref('stg_raw__bing') }}
    union all
    select * from {{ ref('stg_raw__criteo') }}
    union all
    select * from {{ ref('stg_raw__facebook') }}

)

select * from all_campaigns
