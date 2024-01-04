{{ config (
    alias = target.database + '_klaviyo_performance_by_flow'
)}}

{%- set sho_schema_name, sho_table_name = 'shopify_base', 'shopify_orders' -%}
{%- set sho_table_exists = check_source_exists(sho_schema_name, sho_table_name) %}
{%- set date_granularity_list = ['day','week','month','quarter','year'] -%}

WITH 
    {%- for date_granularity in date_granularity_list %}

    performance_{{date_granularity}} AS 
    (SELECT 
        date_trunc('{{date_granularity}}',date) as date,
        '{{date_granularity}}' as date_granularity,
        flow_name,
        date::varchar||date_granularity||flow_name as unique_key,
        sum(coalesce(total_recipients,0)) as total_recipients,
        sum(coalesce(received,0)) as received,
        sum(coalesce(opens,0)) as opens,
        sum(coalesce(clicks,0)) as clicks,
        sum(coalesce(bounced,0)) as bounced,
        sum(coalesce(unsubscribed,0)) as unsubscribed,
        sum(coalesce(subscribed,0)) as subscribed,
        sum(coalesce(total_orders,0)) as total_orders,
        sum(coalesce(total_revenue,0)) as total_revenue
        {%- if shopify_table_exists %}
        , sum(coalesce(first_orders,0)) as first_orders
        , sum(coalesce(repeat_orders,0)) as repeat_orders
        , sum(coalesce(first_orders_revenue,0)) as first_orders_revenue
        , sum(coalesce(repeat_orders_revenue,0)) as repeat_orders_revenue
        {%- endif %}
    FROM {{ ref('klaviyo_campaigns') }}
    LEFT JOIN {{ ref('klaviyo_placed_order') }} USING(date,campaign_id)
    where flow_name != ''
    GROUP BY 
        date_granularity,
        date,
        flow_name
        unique_key
    ){% if not loop.last %},
    {% endif %}
    {%- endfor %}

    ({% for date_granularity in date_granularity_list -%}
    SELECT *
    FROM performance_{{date_granularity}}
    {% if not loop.last %}UNION ALL
    {% endif %}

    {%- endfor %}
