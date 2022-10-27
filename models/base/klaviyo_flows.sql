{%- set selected_fields = [
    "id",
    "name",
    "status"
] -%}

{%- set schema_name, table_name = 'klaviyo_raw', 'flow' -%}

WITH 
    emails AS 
    (SELECT 
        'Flow' as email_type,
        {% for column in selected_fields -%}
        {{ get_klaviyo_clean_field(table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ source(schema_name, table_name) }})

SELECT *,
    email_id as unique_key
FROM emails