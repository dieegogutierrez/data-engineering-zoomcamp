{{
    config(
        materialized='view'
    )
}}

with 

tripdata as (

    select * 
    from {{ source('staging', 'fhv_tripdata') }}
    where dispatching_base_num is not null 
    AND EXTRACT(YEAR FROM pickup_datetime) = 2019
)

select
    -- identifiers
    {{ dbt_utils.generate_surrogate_key(['dispatching_base_num', 'pickup_datetime']) }} as tripid,
    {{ dbt.safe_cast("dispatching_base_num", api.Column.translate_type("string")) }} as dispatching_base_id,
    {{ dbt.safe_cast("p_ulocation_id", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("d_olocation_id", api.Column.translate_type("integer")) }} as dropoff_locationid,
    {{ dbt.safe_cast("sr_flag", api.Column.translate_type("integer")) }} as sr_flag,
    {{ dbt.safe_cast("affiliated_base_number", api.Column.translate_type("string")) }} as affiliated_base_id,

    -- timestamps
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(drop_off_datetime as timestamp) as dropoff_datetime,
from tripdata

-- dbt build --select <model.sql> --vars '{'is_test_run: false}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}