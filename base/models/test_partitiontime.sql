{{
    config(
        materialized="incremental",
        on_schema_change='sync_all_columns',
        partition_by={
            "field": "date_hour",
            "data_type": "datetime",
            "granularity": "hour",
            "time_ingestion_partitioning": true
        },
        time_column='date_hour'
    )
}}

{% if not is_incremental() %}

    select 10 as id,
    cast('2020-01-01 01:00:00' as datetime) as date_hour,
    3 as field_3,
    2 as field_2
    union all
    select 30 as id,
    cast('2020-01-01 02:00:00' as datetime) as date_hour,
    3 as field_3,
    2 as field_2

{% else %}

    select 20 as id,
    cast('2020-01-01 01:00:00' as datetime) as date_hour,
    3 as field_3,
    2 as field_2
    union all
    select 40 as id,
    cast('2020-01-01 02:00:00' as datetime) as date_hour,
    3 as field_3,
    2 as field_2

{% endif %}