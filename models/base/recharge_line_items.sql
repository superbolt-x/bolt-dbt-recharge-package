{%- set schema_name,
        table_name
        = 'recharge_raw', 'order_line_item' -%}
        
{%- set selected_fields = [
    "order_id",
    "index",
    "subscription_id",
    "title",
    "product_title",
    "variant_title",
    "price",
    "quantity",
    "sku",
    "shopify_product_id",
    "shopify_variant_id"
] -%}

WITH staging AS 
    (SELECT 

        {% for column in selected_fields -%}
        {{ get_recharge_clean_field(table_name, column)}},
        {% endfor %}
        order_id||'-'||line_item_index as line_item_id

    FROM {{ source(schema_name, table_name) }})

SELECT *,
    line_item_id as unique_key
FROM staging 
