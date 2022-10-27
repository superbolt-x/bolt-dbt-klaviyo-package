{%- set selected_fields = [
    "id",
    "created",
    "email"
] -%}

{%- set schema_name, table_name = 'klaviyo_raw', 'person' -%}

WITH 
    subscribers AS 
    (SELECT 
        {% for column in selected_fields -%}
        {{ get_klaviyo_clean_field(table_name, column)}}
        {%- if not loop.last %},{% endif %}
        {% endfor %}

    FROM {{ source(schema_name, table_name) }})

SELECT *
FROM subscribers