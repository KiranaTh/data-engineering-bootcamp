WITH 
    source AS (
        SELECT * FROM {{ source('greenery', 'order_items') }}
    ),
    renamed AS (
        SELECT
            order_id as order_guid
            , quantity
            , product_id as product_guid
        FROM source
    )

select * from renamed
