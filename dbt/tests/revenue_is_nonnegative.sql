select order_id
from {{ ref('fct_order_revenue') }}
where gross_revenue < 0
