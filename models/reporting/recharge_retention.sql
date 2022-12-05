{{ config (
    alias = target.database + '_recharge_retention'
)}}

{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH subscriptions AS 
    (SELECT subscription_id, customer_id, created_at, cancelled_at,
        MIN(created_at) OVER (PARTITION BY customer_id) as first_created_at,
        MAX(COALESCE(cancelled_at::date, current_date)) OVER (PARTITION BY customer_id) as last_cancelled_date
    FROM {{ ref('recharge_subscriptions')}}
    ),

    subscribers AS 
    (SELECT customer_id, 
        COUNT(*) as total_subscriptions,
        COUNT(CASE WHEN cancelled_at IS NULL THEN subscription_id END) as total_active_subscriptions
    FROM subscriptions 
    GROUP BY customer_id
    ),

    acquisition AS 
    (SELECT created_at::date as date, 
        COUNT(*) as new_subscriptions,
        COUNT(DISTINCT CASE WHEN created_at = first_created_at THEN customer_id END) as new_subscribers
    FROM subscriptions 
    GROUP BY date
    ),

    churn AS 
    (SELECT cancelled_at::date as date, 
        COUNT(*) as churned_subscriptions,
        COUNT(DISTINCT CASE WHEN total_active_subscriptions = 0 THEN customer_id END) as churned_subscribers
    FROM subscriptions
    LEFT JOIN subscribers USING(customer_id)
    WHERE cancelled_at IS NOT NULL
    GROUP BY date
    ),

    staging AS 
    (SELECT *,
         {{ get_date_parts('date') }}
    FROM utilities.dates
    LEFT JOIN acquisition USING(date)
    LEFT JOIN churn USING(date)
    WHERE new_subscriptions IS NOT NULL OR churned_subscriptions IS NOT NULL
    ),

    {%- for date_granularity in date_granularity_list %}

    active_subscription_{{date_granularity}} AS 
    (SELECT 
        date_granularity,
        date, 
        COUNT(*) as active_subscriptions
    FROM 
        (SELECT DISTINCT '{{date_granularity}}' as date_granularity,
            DATE_TRUNC('{{date_granularity}}', date)::date as date
        FROM utilities.dates)
    INNER JOIN 
        (SELECT subscription_id, customer_id, created_at::date as created_date, 
            CASE WHEN cancelled_at IS NULL THEN current_date ELSE cancelled_at::date END AS cancelled_date
        FROM subscriptions) 
        ON date BETWEEN DATE_TRUNC('{{date_granularity}}', created_date) AND DATE_TRUNC('{{date_granularity}}', cancelled_date)
    GROUP BY 1,2
    ),

    active_subscriber_{{date_granularity}} AS 
    (SELECT 
        date_granularity,
        date, 
        COUNT(*) as active_subscribers
    FROM 
        (SELECT DISTINCT '{{date_granularity}}' as date_granularity,
        DATE_TRUNC('{{date_granularity}}', date)::date as date
        FROM utilities.dates)
    INNER JOIN 
        (SELECT DISTINCT customer_id, first_created_at::date as first_created_date,
            last_cancelled_date
        FROM subscriptions) 
        ON date BETWEEN DATE_TRUNC('{{date_granularity}}', first_created_date) AND DATE_TRUNC('{{date_granularity}}', last_cancelled_date)
    GROUP BY 1,2
    ),

    retention_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date,
        COALESCE(SUM(new_subscriptions),0) as new_subscriptions,
        COALESCE(SUM(new_subscribers),0) as new_subscribers,
        COALESCE(SUM(churned_subscriptions),0) as churned_subscriptions,
        COALESCE(SUM(churned_subscribers),0) as churned_subscribers
    FROM staging
    GROUP BY date_granularity, {{date_granularity}}
    )
    {%- if not loop.last %},{%- endif %}
    {%- endfor %}

{% for date_granularity in date_granularity_list -%}
SELECT 
    *,
    COALESCE(churned_subscriptions::float/NULLIF(active_subscriptions,0),0) as subscription_churn_rate,
    COALESCE(churned_subscribers::float/NULLIF(active_subscribers,0),0) as subscriber_churn_rate
FROM retention_{{date_granularity}}
LEFT JOIN active_subscription_{{date_granularity}} USING(date_granularity, date)
LEFT JOIN active_subscriber_{{date_granularity}} USING(date_granularity, date)
{% if not loop.last %}UNION ALL
{% endif %}

{%- endfor %}

