WITH
    orders AS (
        SELECT * FROM {{ ref('stg_greenery__orders') }}
    ),
    final AS (
        SELECT count(DISTINCT order_guid) AS order_count
        FROM orders
    )

SELECT * FROM final