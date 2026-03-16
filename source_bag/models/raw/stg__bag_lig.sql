{{
    config(
        materialized='view'
    )
}}

select *
from {{ source('datalake', 'source_bag_lig')}}