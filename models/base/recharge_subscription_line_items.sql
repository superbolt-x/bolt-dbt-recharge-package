WITH orders AS 
    (SELECT order_id, created_at, order_date
    FROM {{ ref('recharge_orders') }}
    ),

    subscriptions AS 
    (SELECT subscription_id, 1 as is_subscription
    FROM {{ ref('recharge_subscriptions') }}
    ),

    line_items AS 
    (SELECT order_id, line_item_title, subscription_id,
        price*quantity as line_item_gross_revenue
    FROM {{ ref('recharge_line_items') }}
    ),

    staging AS 
    (SELECT *
    FROM line_items 
    INNER JOIN subscriptions USING(subscription_id)
    LEFT JOIN orders USING(order_id)
    )

SELECT *,
    ROW_NUMBER() OVER (PARTITION BY subscription_id ORDER BY created_at) as subscription_order_index
FROM staging