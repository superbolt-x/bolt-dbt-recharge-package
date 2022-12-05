{%- set schema_name,
        table_name
        = 'recharge_raw', 'product' -%}
        
{%- set selected_fields = [
    "id",
    "title",
    "handle",
    "shopify_product_id",
    "created_at"
] -%}

WITH staging AS 
    (SELECT 

        {% for column in selected_fields -%}
        {{ get_recharge_clean_field(table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ source(schema_name, table_name) }})

SELECT *,
    product_id as unique_key
FROM staging 
