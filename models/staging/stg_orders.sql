with source as (

    select * from {{ source('olist', 'orders') }}

),

renamed as (

    select
        -- ids
        order_id,
        customer_id,

        -- status
        order_status,

        -- timestamps
        order_purchase_timestamp                as purchased_at,
        order_approved_at                       as approved_at,
        order_delivered_carrier_date            as shipped_at,
        order_delivered_customer_date           as delivered_at,
        order_estimated_delivery_date           as estimated_delivery_at,

        -- derived
        date(order_purchase_timestamp)          as purchase_date,

        case
            when order_status = 'delivered'
            and order_delivered_customer_date <= order_estimated_delivery_date
            then true
            else false
        end                                     as is_on_time

    from source

)

select * from renamed