with source as (

    select * from {{ source('olist', 'order_items') }}

),

renamed as (

    select
        order_id,
        order_item_id,
        product_id,
        seller_id,
        price,
        freight_value,
        price + freight_value               as item_total,
        shipping_limit_date                 as shipping_limit_at

    from source

)

select * from renamed