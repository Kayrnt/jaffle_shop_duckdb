
with orders as (

    select * from {{ ref('orders') }}

),
final as (

    select
        orders.customer_id,
        SUM(amount) as total_amount
    from orders

{% if sqlmesh_incremental is defined %}
   WHERE
     order_date BETWEEN '{{ start_ds }}' AND '{{ end_ds }}'
{% endif %}
    group by orders.customer_id

)

select *
from final
