{%- set selected_fields = [
    "id",
    "subject",
    "name",
    "status",
    "sent_at",
    "campaign_type"
] -%}

{%- set schema_name, table_name = 'klaviyo_raw', 'campaign' -%}

WITH 
    emails AS 
    (SELECT 
        'Campaign' as email_type,
        {% for column in selected_fields -%}
        {{ get_klaviyo_clean_field(table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ source(schema_name, table_name) }})

SELECT *,
    campaign_id as unique_key
FROM emails