{%- set schema_name,
        table_name
        = 'recharge_raw', 'order' -%}
        
{%- set selected_fields = [
    "id",
    "customer_id",
    "charge_id",
    "first_name",
    "last_name",
    "email",
    "transaction_id",
    "charge_status",
    "status",
    "total_price",
    "type",
    "shopify_order_id",
    "shopify_order_number",
    "created_at",
    "shipping_address_first_name",
    "shipping_address_last_name",
    "shipping_address_address_1",
    "shipping_address_address_2",
    "shipping_address_city",
    "shipping_address_province",
    "shipping_address_country",
    "shipping_address_zip",
    "shipping_address_phone"
] -%}

WITH staging AS 
    (SELECT 

        {% for column in selected_fields -%}
        {{ get_recharge_clean_field(table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ source(schema_name, table_name) }})

SELECT *,
    created_at::date as order_date,
    order_id as unique_key
FROM staging 
