select
    order_id,
    order_item_id,
    product_id,
    seller_id,
    shipping_limit_date,
    price,
    freight_value,
    concat(order_id, '-', cast(order_item_id as string)) as order_item_key
from parquet.`{{ env_var('SILVER_ROOT', 'wasb://olist-data@storageemulator/silver') }}/order_items/batch_date={{ var('batch_date', 'adhoc') }}`
