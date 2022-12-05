{%- set schema_name,
        table_name
        = 'recharge_raw', 'customer' -%}
        
{%- set selected_fields = [
    "id",
    "shopify_customer_id",
    "first_name",
    "last_name",    
    "email",
    "created_at",
    "number_active_subscriptions",
    "number_subscriptions",
    "status"
] -%}


WITH staging AS 
    (SELECT 

        {% for column in selected_fields -%}
        {{ get_recharge_clean_field(table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ source(schema_name, table_name) }}
    )


SELECT *,
    customer_id as unique_key
FROM staging 