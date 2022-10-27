{{ config( 
        materialized='incremental',
        unique_key='unique_key'
) }}

{%- set schema_name, action_table_name = 'klaviyo_raw', 'event' -%}
{%- set shopify_schema_name, shopify_table_name = 'shopify', 'shopify_orders' -%}
{%- set top_funnel_actions = ['Received Email','Opened Email','Clicked Email','Unsubscribed'] -%}
{%- set top_funnel_actions_list = "'"~top_funnel_actions|join("','")~"'" -%}

{%- set top_funnel_selected_fields = [
    "id",
    "person_id",
    "flow_id",
    "flow_message_id",
    "datetime",
    "type",
    "property_subject",
    "property_campaign_name"
] -%}

WITH top_funnel_raw AS 
    (SELECT 
        {% for column in top_funnel_selected_fields -%}
        {{ get_klaviyo_clean_field(action_table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ source(schema_name, action_table_name) }}
    WHERE type IN ({{top_funnel_actions_list}})
    {% if is_incremental() -%}
    -- this filter will only be applied on an incremental run
    AND event_date >= (select max(event_date) from {{ this }})
    {% endif %}
    ),

    campaigns AS 
    (SELECT campaign_id as email_id, sent_date, campaign_status
    FROM {{ ref('klaviyo_campaigns') }}
    ),

    top_funnel AS 
    (SELECT 
        event_date, 
        person_id, 
        email_id, 
        message_id, 
        email_subject,
        email_name,
        {%- for action in top_funnel_actions %}
        MAX(CASE WHEN type = '{{action}}' THEN 1 ELSE 0 END) as unique_{{action.split(' Email')[0]|lower}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM top_funnel_raw
    GROUP BY event_date, person_id, email_id, message_id, email_subject, email_name
    ),

    {%- set shopify_table_exists = check_source_exists(shopify_schema_name, shopify_table_name) %}
    placed_order AS 
    (SELECT 
        person_id, 
        event_date, 
        email_id, 
        message_id, 
        email_subject, 
        email_name,
        COALESCE(COUNT(order_id),0) as orders,
        COALESCE(SUM(order_revenue),0) as revenue
        {%- if shopify_table_exists %}
        , COALESCE(COUNT(CASE WHEN customer_order_index = 1 THEN order_id END),0) as first_orders
        , COALESCE(COUNT(CASE WHEN customer_order_index > 1 THEN order_id END),0) as repeat_orders
        , COALESCE(SUM(CASE WHEN customer_order_index = 1 THEN order_revenue END),0) as first_order_revenue
        , COALESCE(SUM(CASE WHEN customer_order_index > 1 THEN order_revenue END),0) as repeat_order_revenue
        {%- endif %}
    FROM {{ ref('klaviyo_orders') }}
    -- Exclude the Placed Order events that got attributed to themselves
    INNER JOIN (SELECT event_id as attributed_event_id, email_subject, email_name FROM top_funnel_raw) USING(attributed_event_id)
    {%- if shopify_table_exists %}
    WHERE cancelled_at IS NULL
    {%- endif %}
    GROUP BY person_id, event_date, email_id, message_id, email_subject, email_name
    ),

    staging AS 
    (SELECT *,
        CASE WHEN campaign_status = 'Cancelled' THEN 'Cancelled Campaign' WHEN sent_date IS NULL THEN 'Flow' ELSE 'Campaign' END as email_type,
        CASE WHEN sent_date IS NULL THEN event_date ELSE sent_date END as email_date
    FROM top_funnel 
    FULL JOIN placed_order USING(person_id, event_date, email_id, message_id, email_name, email_subject)
    LEFT JOIN campaigns USING(email_id)
    )

SELECT *,
    person_id||'_'||event_date||'_'||email_id||'_'||message_id||'_'||email_name||'_'||email_subject as unique_key
FROM staging