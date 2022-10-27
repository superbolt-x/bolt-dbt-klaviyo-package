{%- macro get_klaviyo_clean_field(table_name, column_name) %}

    {%- if table_name == 'event' -%}
        {%- if column_name == 'id' -%}
        {{column_name}} as event_id

        {%- elif column_name == 'datetime' -%}
        ("{{column_name}}"::timestamp at time zone '{{var('time_zone')}}')::date as event_date

        {%- elif column_name == 'flow_id' -%}
        COALESCE(flow_id, campaign_id) as email_id

        {%- elif column_name == 'flow_message_id' -%}
        COALESCE({{column_name}},'') as message_id

        {%- elif column_name == 'property_subject' -%}
        {{column_name}} as email_subject

        {%- elif column_name == 'property_campaign_name' -%}
        {{column_name}} as email_name

        {%- elif column_name == 'property_event_id' -%}
        {{column_name}}::bigint as order_id

        {%- elif column_name == 'property_value' -%}
        {{column_name}} as order_revenue

        {%- elif column_name == 'property_attribution' -%}
        JSON_EXTRACT_PATH_TEXT({{column_name}},'$attributed_event_id') as attributed_event_id,
        DECODE(JSON_EXTRACT_PATH_TEXT({{column_name}},'$flow'), '', '', JSON_EXTRACT_PATH_TEXT({{column_name}},'$message')) as message_id,
        DECODE(JSON_EXTRACT_PATH_TEXT({{column_name}},'$flow'),'',JSON_EXTRACT_PATH_TEXT({{column_name}},'$message'),JSON_EXTRACT_PATH_TEXT({{column_name}},'$flow')) as email_id

        {%- else -%}
        {{column_name}}

        {%- endif -%}

    {%- elif table_name == 'flow' -%}
        {%- if column_name == 'id' -%}
        {{ column_name }} as email_id
        
        {%- else -%}
        {{column_name}} as flow_{{column_name}}

        {%- endif -%}
    
    {%- elif table_name == 'campaign' -%}
        {%- if column_name == 'sent_at' -%}
        ({{ column_name }}::timestamp at time zone '{{var('time_zone')}}')::date as sent_date

        {%- elif column_name in ('id','name','subject','status') -%}
        {{ column_name }} as campaign_{{ column_name }}
        
        {%- else -%}
        {{ column_name }}

        {%- endif -%}
    
    {%- elif table_name == 'person' -%}
        
        {%- if column_name == 'id' -%}
        {{column_name}} as person_id

        {%- elif column_name == 'created' -%}
        {{column_name}}::timestamp at time zone '{{var('time_zone')}}' as subscribed_at
        
        {%- else -%}
        {{column_name}} 

        {%- endif -%}

    {%- else -%}
    {{column_name}} 

    {%- endif -%}

{% endmacro -%}