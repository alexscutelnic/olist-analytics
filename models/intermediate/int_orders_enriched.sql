with orders as (

    select * from {{ ref('stg_orders') }}

),

order_items as (

    select * from {{ ref('stg_order_items') }}

),

order_payments as (

    select * from {{ ref('stg_order_payments') }}

),

customers as (

    select * from {{ ref('stg_customers') }}

),

-- aggregate items per order
order_items_agg as (

    select
        order_id,
        count(*)                            as item_count,
        sum(price)                          as items_subtotal,
        sum(freight_value)                  as freight_total,
        sum(item_total)                     as order_subtotal

    from order_items
    group by order_id

),

-- aggregate payments per order
order_payments_agg as (

    select
        order_id,
        sum(payment_value)                  as payment_total,
        count(distinct payment_type)        as payment_methods_used,
        max(payment_installments)           as max_installments

    from order_payments
    group by order_id

),

-- join everything together
joined as (

    select
        -- order identifiers
        o.order_id,
        o.customer_id,
        c.customer_unique_id,

        -- geography
        c.city                              as customer_city,
        c.state                             as customer_state,

        -- order status & timing
        o.order_status,
        o.purchased_at,
        o.approved_at,
        o.shipped_at,
        o.delivered_at,
        o.estimated_delivery_at,
        o.purchase_date,
        o.is_on_time,

        -- delivery days
        date_diff(
            date(o.delivered_at),
            date(o.purchased_at),
            day
        )                                   as days_to_deliver,

        -- financials
        oi.item_count,
        oi.items_subtotal,
        oi.freight_total,
        oi.order_subtotal,
        op.payment_total,
        op.payment_methods_used,
        op.max_installments

    from orders o
    left join customers c
        on o.customer_id = c.customer_id
    left join order_items_agg oi
        on o.order_id = oi.order_id
    left join order_payments_agg op
        on o.order_id = op.order_id

)

select * from joined