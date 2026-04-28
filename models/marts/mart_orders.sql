with orders as (

    select * from {{ ref('int_orders_enriched') }}

),

reviews as (

    select
        order_id,
        review_score,
        comment_message

    from {{ ref('stg_order_reviews') }}

),

final as (

    select
        -- identifiers
        o.order_id,
        o.customer_id,
        o.customer_unique_id,

        -- geography
        o.customer_city,
        o.customer_state,

        -- order details
        o.order_status,
        o.purchase_date,
        o.purchased_at,
        o.delivered_at,
        o.is_on_time,
        o.days_to_deliver,

        -- financials
        o.item_count,
        o.items_subtotal,
        o.freight_total,
        o.order_subtotal,
        o.payment_total,
        o.max_installments,

        -- customer satisfaction
        r.review_score,
        case
            when cast(r.review_score as int64) >= 4 then 'positive'
            when cast(r.review_score as int64) = 3  then 'neutral'
            when cast(r.review_score as int64) <= 2 then 'negative'
            else 'no review'
        end                                     as sentiment,

        -- delivery performance
        case
            when o.order_status != 'delivered'  then 'not delivered'
            when o.is_on_time = true            then 'on time'
            else 'late'
        end                                     as delivery_status

    from orders o
    left join reviews r
        on o.order_id = r.order_id

)

select * from final