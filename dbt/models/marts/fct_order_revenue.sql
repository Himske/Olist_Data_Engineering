select
    orders.order_id,
    orders.customer_id,
    orders.order_status,
    orders.order_purchase_timestamp,
    count(items.order_item_key) as item_count,
    sum(items.price + items.freight_value) as gross_revenue
from {{ ref('stg_orders') }} as orders
inner join {{ ref('stg_order_items') }} as items
    on orders.order_id = items.order_id
group by
    orders.order_id,
    orders.customer_id,
    orders.order_status,
    orders.order_purchase_timestamp
