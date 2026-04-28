with
    orders
    as
    (

        select *
        from {{ ref
    ('mart_orders') }}

),

-- one row per unique customer
customer_orders as
(

    select
    customer_unique_id,

    count(distinct order_id)            as total_orders,
    min(purchase_date)                  as first_order_date,
    max(purchase_date)                  as last_order_date,
    date_diff(
            max(purchase_date),
            min(purchase_date),
            day
        )                                   as customer_lifespan_days,

    round(sum(payment_total), 2)        as total_spend,
    round(avg(payment_total), 2)        as avg_order_value,
    round(avg(review_score), 2)         as avg_review_score,

    countif(is_on_time = true)          as on_time_deliveries,
    countif(delivery_status = 'late')   as late_deliveries

from orders
where order_status = 'delivered'
group by 1

)
,

final as
(

    select
    *,
    case
            when total_orders = 1   then 'one-time'
            when total_orders <= 3  then 'repeat'
            else 'loyal'
        end                                 as customer_segment,

    round(
            safe_divide(on_time_deliveries, total_orders) * 100,
            1
        )                                   as on_time_pct

from customer_orders

)

select *
from final