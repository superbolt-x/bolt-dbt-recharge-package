{%- set schema_name,
        table_name
        = 'recharge_raw', 'subscription' -%}
        
{%- set selected_fields = [
    "id",
    "customer_id",
    "created_at",
    "cancelled_at",    
    "price",
    "quantity",
    "cancellation_reason",
    "product_title",
    "variant_title",
    "shopify_product_id",
    "shopify_variant_id",
    "sku",
    "order_interval_unit",
    "order_interval_frequency"
] -%}


WITH subscriptions AS 
    (SELECT 

        {% for column in selected_fields -%}
        {{ get_recharge_clean_field(table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ source(schema_name, table_name) }}
    ),

    utm AS 
    (SELECT subscription_id, source, medium, campaign
    FROM {{ ref('recharge_utm') }}
    WHERE origin = 'subscription'
    )


SELECT *,
    subscription_id as unique_key
FROM subscriptions 
LEFT JOIN utm USING(subscription_id)