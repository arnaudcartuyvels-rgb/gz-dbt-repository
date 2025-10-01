with 

source as (

    select * from {{ source('raw', 'adwords') }}

),

renamed as (

    select
        date_date,
        paid_source,
        campaign_key,
        camPGN_name as campaign_name,       -- rename
        cast(ads_cost as float64) as ads_cost,  -- cast
        impression,
        click

    from source

)

select * from renamed
