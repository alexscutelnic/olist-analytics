with source as (

    select * from {{ source('olist', 'products') }}

),

renamed as (

    select
        product_id,
        product_category_name               as category_name_portuguese,
        product_weight_g                    as weight_g,
        product_length_cm                   as length_cm,
        product_height_cm                   as height_cm,
        product_width_cm                    as width_cm

    from source

)

select * from renamed