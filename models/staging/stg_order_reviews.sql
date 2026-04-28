with
    source
    as
    (

        select *
        from {{ source
    ('olist', 'order_reviews') }}

),

renamed as
(

    select
    review_id,
    order_id,
    safe_cast(review_score as int64)    as review_score,
    review_comment_title                as comment_title,
    review_comment_message              as comment_message,
    review_creation_date                as created_at,
    review_answer_timestamp             as answered_at

from source

)

select *
from renamed