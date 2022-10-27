{{ config (
    alias = target.database + '_klaviyo_performance_by_flow'
)}}
{%- set shopify_schema_name, shopify_table_name = 'shopify', 'shopify_orders' -%}
{%- set shopify_table_exists = check_source_exists(shopify_schema_name, shopify_table_name) %}
{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH flows AS 
    (SELECT email_id, flow_name 
    FROM {{ ref('klaviyo_flows') }}
    ),

    all_flow_messages_staging AS 
    (SELECT *,
        {{ get_date_parts('email_date') }}
    FROM 
        (SELECT 
            email_date, 
            email_id,
            message_id,
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
        WHERE email_type = 'Flow'
        GROUP BY email_date, email_id, message_id, email_name, email_subject, person_id 
        )
    ),

    all_flows_staging AS 
    (SELECT *, 
        {{ get_date_parts('email_date') }}
    FROM 
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
        WHERE email_type = 'Flow'
        GROUP BY email_date, email_id, email_name, email_subject, person_id 
        )
    ),

    {% for date_granularity in date_granularity_list %}
    all_flow_messages_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date, 
        flow_name,
        email_name, 
        email_subject,
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
    FROM all_flow_messages_staging 
    LEFT JOIN flows USING(email_id)
    WHERE flow_name is not null
    GROUP BY date_granularity, date, flow_name, email_name, email_subject
    ),

    all_flows_{{date_granularity}} AS 
    (SELECT 
        '{{date_granularity}}' as date_granularity,
        {{date_granularity}} as date, 
        flow_name,
        'Messages Combined' as email_name, 
        'Messages Combined' as email_subject,
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
    FROM all_flows_staging 
    LEFT JOIN flows USING(email_id)
    WHERE flow_name is not null
    GROUP BY date_granularity, date, flow_name
    )
    {%- if not loop.last %},{%- endif %}
    {%- endfor %}

SELECT *, 
    date_granularity||'_'||date||'_'||flow_name||'_'||email_name||'_'||email_subject as unique_key
FROM    
    (
    {% for date_granularity in date_granularity_list -%}  
    SELECT * 
    FROM all_flow_messages_{{date_granularity}}
    UNION ALL 
    SELECT * 
    FROM all_flows_{{date_granularity}}
    {% if not loop.last %}UNION ALL
    {% endif %}
    {%- endfor %}
    )