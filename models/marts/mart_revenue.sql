with
    orders
    as
    (

        select *
        from {{ ref
    ('int_orders_enriched') }}

),

order_items as
(

    select *
from {{ ref
('stg_order_items') }}

),

products as
(

    select *
from {{ ref
('stg_products') }}

),

sellers as
(

    select *
from {{ ref
('stg_sellers') }}

),

category_translation as
(

    select *
from {{ source
('olist', 'product_category_translation') }}

),

joined as
(

    select
    -- time dimensions
    date_trunc(o.purchase_date, month)      as purchase_month,
    extract(year from o.purchase_date)      as purchase_year,
    extract(month from o.purchase_date)     as purchase_month_num,

    -- geography
    s.state                                 as seller_state,
    o.customer_state,

    -- product
    p.category_name_portuguese,
    coalesce(
            ct.string_field_1,
            p.category_name_portuguese
        )                                       as category_name_english,

    -- seller
    oi.seller_id,

    -- financials
    count(distinct o.order_id)              as order_count,
    count(oi.order_item_id)                 as item_count,
    round(sum(oi.price), 2)                 as gross_revenue,
    round(sum(oi.freight_value), 2)         as freight_revenue,
    round(sum(oi.item_total), 2)            as total_revenue

from order_items oi
    left join orders o
    on oi.order_id = o.order_id
    left join products p
    on oi.product_id = p.product_id
    left join sellers s
    on oi.seller_id = s.seller_id
    left join category_translation ct
    on p.category_name_portuguese = ct.string_field_0
where
        o.order_status = 'delivered'

group by 1, 2, 3, 4, 5, 6, 7, 8

)

select * from joined