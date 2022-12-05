{{ config (
    alias = target.database + '_recharge_sales'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH subscriptions AS 
    (SELECT *,
        {{ get_date_parts('order_date') }}
    FROM {{ ref('recharge_subscription_line_items')}}
    ),

    {%- for date_granularity in date_granularity_list %}

    sales_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        COALESCE(SUM(line_item_gross_revenue),0) as subscription_gross_revenue,
        COALESCE(SUM(CASE WHEN subscription_order_index = 1 THEN line_item_gross_revenue END),0) as new_subscriptions_gross_revenue,
        COALESCE(SUM(CASE WHEN subscription_order_index > 1 THEN line_item_gross_revenue END),0) as recurring_subscriptions_gross_revenue,
        COALESCE(COUNT(DISTINCT order_id),0) as subscription_orders,
        COALESCE(COUNT(DISTINCT CASE WHEN subscription_order_index = 1 THEN order_id END),0) as new_subscriptions_orders,
        COALESCE(COUNT(DISTINCT CASE WHEN subscription_order_index > 1 THEN order_id END),0) as recurring_subscriptions_orders
    FROM subscriptions
    GROUP BY date_granularity, {{date_granularity}}
    )
    {%- if not loop.last %},{%- endif %}
    {%- endfor %}

{% for date_granularity in date_granularity_list -%}
SELECT *
FROM sales_{{date_granularity}}
{% if not loop.last %}UNION ALL
{% endif %}

{%- endfor %}

