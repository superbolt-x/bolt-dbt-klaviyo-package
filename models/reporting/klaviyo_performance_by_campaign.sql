{{ config (
    alias = target.database + '_klaviyo_performance_by_campaign'
)}}
{%- set shopify_schema_name, shopify_table_name = 'shopify', 'shopify_orders' -%}
{%- set shopify_table_exists = check_source_exists(shopify_schema_name, shopify_table_name) %}

WITH campaign_ab_testing AS 
    (SELECT email_id, CASE WHEN COUNT(DISTINCT email_subject) > 1 THEN 'Variation' ELSE 'N/A' END AS ab_testing
    FROM {{ ref('klaviyo_actions')}}
    WHERE email_type = 'Campaign'
    GROUP BY email_id
    ),

    all_campaigns_staging AS 
    (SELECT 
        email_date, 
        email_id,
        person_id, 
        MAX(unique_received) as received,
        MAX(unique_opened) as opened,
        MAX(unique_clicked) as clicked,
        MAX(unique_unsubscribed) as unsubscribed,
        COALESCE(SUM(orders),0) as orders,
        COALESCE(SUM(revenue),0) as revenue
        {%- if shopify_table_exists %}
        , COALESCE(SUM(first_orders),0) as first_orders
        , COALESCE(SUM(repeat_orders),0) as repeat_orders
        , COALESCE(SUM(first_order_revenue),0) as first_order_revenue
        , COALESCE(SUM(repeat_order_revenue),0) as repeat_order_revenue
        {%- endif %}
    FROM {{ ref('klaviyo_actions')}}
    WHERE email_type = 'Campaign'
    GROUP BY email_date, email_id, person_id
    ),

    all_campaigns AS 
    (SELECT 
        email_date, 
        c.email_name,
        c.email_subject,
        CASE WHEN ab_testing = 'Variation' THEN 'Variations Combined' ELSE 'N/A' END AS is_ab_testing,
        SUM(received) as received,
        SUM(opened) as opened,
        COALESCE(SUM(opened)::decimal/NULLIF(SUM(received),0),0) as open_rate,
        SUM(clicked) as clicked, 
        COALESCE(SUM(clicked)::decimal/NULLIF(SUM(received),0),0) as click_rate,
        SUM(unsubscribed) as unsubscribed, 
        COALESCE(SUM(unsubscribed)::decimal/NULLIF(SUM(received),0),0) as unsubscribe_rate,
        COALESCE(SUM(orders),0) as orders,
        COALESCE(SUM(orders)::decimal/NULLIF(SUM(received),0),0) as order_rate,
        COALESCE(SUM(revenue),0) as revenue
        {%- if shopify_table_exists %}
        , COALESCE(SUM(first_orders),0) as first_orders
        , COALESCE(SUM(repeat_orders),0) as repeat_orders
        , COALESCE(SUM(first_order_revenue),0) as first_order_revenue
        , COALESCE(SUM(repeat_order_revenue),0) as repeat_order_revenue
        {%- endif %}
    FROM all_campaigns_staging
    LEFT JOIN  
        (SELECT campaign_id as email_id, campaign_name as email_name, campaign_name as email_subject
        FROM {{ ref('klaviyo_campaigns')}}
        ) c USING(email_id) 
    LEFT JOIN campaign_ab_testing USING(email_id)
    GROUP BY email_date, email_name, email_subject, is_ab_testing
    ),

    ab_testing_campaigns_staging AS 
    (SELECT 
        email_date, 
        email_name,
        email_subject,
        person_id, 
        MAX(unique_received) as received,
        MAX(unique_opened) as opened,
        MAX(unique_clicked) as clicked,
        MAX(unique_unsubscribed) as unsubscribed,
        COALESCE(SUM(orders),0) as orders,
        COALESCE(SUM(revenue),0) as revenue
        {%- if shopify_table_exists %}
        , COALESCE(SUM(first_orders),0) as first_orders
        , COALESCE(SUM(repeat_orders),0) as repeat_orders
        , COALESCE(SUM(first_order_revenue),0) as first_order_revenue
        , COALESCE(SUM(repeat_order_revenue),0) as repeat_order_revenue
        {%- endif %}
    FROM {{ ref('klaviyo_actions')}}
    LEFT JOIN campaign_ab_testing USING(email_id)
    WHERE email_type = 'Campaign'
    AND ab_testing = 'Variation'
    GROUP BY email_date, email_name, email_subject, person_id
    ),

    ab_testing_campaigns AS     
    (SELECT 
        email_date, 
        email_name,
        email_subject,
        'Variation' AS is_ab_testing,
        SUM(received) as received,
        SUM(opened) as opened,
        COALESCE(SUM(opened)::decimal/NULLIF(SUM(received),0),0) as open_rate,
        SUM(clicked) as clicked, 
        COALESCE(SUM(clicked)::decimal/NULLIF(SUM(received),0),0) as click_rate,
        SUM(unsubscribed) as unsubscribed, 
        COALESCE(SUM(unsubscribed)::decimal/NULLIF(SUM(received),0),0) as unsubscribe_rate,
        COALESCE(SUM(orders),0) as orders,
        COALESCE(SUM(orders)::decimal/NULLIF(SUM(received),0),0) as order_rate,
        COALESCE(SUM(revenue),0) as revenue
        {%- if shopify_table_exists %}
        , COALESCE(SUM(first_orders),0) as first_orders
        , COALESCE(SUM(repeat_orders),0) as repeat_orders
        , COALESCE(SUM(first_order_revenue),0) as first_order_revenue
        , COALESCE(SUM(repeat_order_revenue),0) as repeat_order_revenue
        {%- endif %}
    FROM ab_testing_campaigns_staging
    GROUP BY email_date, email_name, email_subject, is_ab_testing
    )

SELECT *, 
    email_date||'_'||email_name||'_'||email_subject||'_'||is_ab_testing as unique_key
FROM    
    (SELECT * 
    FROM ab_testing_campaigns
    UNION ALL 
    SELECT * 
    FROM all_campaigns)