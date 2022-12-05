{%- macro get_recharge_clean_field(table_name, column_name) -%}

 {%- if table_name == 'customer' -%}
        {%- if column_name == 'id' -%}
        {{column_name}} as customer_id

        {%- else -%}
        {{column_name}}

    {%- endif %}

{%- elif table_name == 'product' %}
    {%- if column_name in ('id','title','handle') -%}
        {{column_name}} as product_{{column_name}}

    {%- else -%}
        {{column_name}}

    {%- endif %}

{%- elif table_name == 'subscription' -%}

    {%- if column_name == 'id' -%}
        {{column_name}} as subscription_{{column_name}}

    {%- else -%}
    {{column_name}}

    {%- endif -%}

{%- elif table_name == 'order_line_item' -%}

    {%- if column_name in ('index','title') -%}
        {{column_name}} as line_item_{{column_name}}

    {%- else -%}
    {{column_name}}

    {%- endif -%}

{%- elif table_name == 'order' -%}

    {%- if column_name == 'id' -%}
        {{column_name}} as order_{{column_name}}

    {%- elif column_name == 'total_price' -%}
        {{column_name}} as total_revenue

    {%- else -%}
    {{column_name}}

    {%- endif -%}
 
{%- else -%}
    {{column_name}}

{%- endif -%}
  
{%- endmacro -%}