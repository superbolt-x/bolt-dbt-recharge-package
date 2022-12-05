{%- set schema_name,
        table_name
        = 'recharge_raw', 'utm_tag' -%}
        
{%- set selected_fields = [
    "customer_id",
    "charge_id",
    "subscription_id",
    "origin",
    "campaign",
    "content",
    "medium",
    "source"
] -%}

WITH staging AS 
    (SELECT 

        {% for column in selected_fields -%}
        {{ get_recharge_clean_field(table_name, column)}},
        {% endfor %}
        COALESCE(customer_id::varchar,'')||'_'||COALESCE(charge_id::varchar, '')||'_'||COALESCE(subscription_id::varchar, '') as utm_id

    FROM {{ source(schema_name, table_name) }})

SELECT *,
    utm_id as unique_key
FROM staging 
