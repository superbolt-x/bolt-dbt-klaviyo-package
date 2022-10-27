{%- set schema_name, table_name = 'klaviyo_raw', 'event' -%}
{%- set shopify_schema_name, shopify_table_name = 'shopify', 'shopify_orders' -%}

{%- set selected_fields = [
    "person_id",
    "datetime",
    "type",
    "property_event_id",
    "property_value",
    "property_attribution"
] -%}

WITH placed_order AS 
    (SELECT 
        {% for column in selected_fields -%}
        {{ get_klaviyo_clean_field(table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}
    FROM {{ source(schema_name, table_name) }}
    WHERE type = 'Placed Order'
    AND property_attribution IS NOT NULL
    )

    {%- set shopify_table_exists = check_source_exists(shopify_schema_name, shopify_table_name) %}
    {%- if shopify_table_exists %}
    ,shopify AS 
    (SELECT order_id, customer_order_index, cancelled_at
    FROM {{ source(shopify_schema_name, shopify_table_name) }}
    )
    {%- endif %}
    
SELECT *,
    order_id as unique_key
FROM placed_order
{%- if shopify_table_exists %}
LEFT JOIN shopify USING(order_id)
{%- endif %}