{%- set schema_name, table_name = 'supermetrics_raw', 'klav_campaigns' -%}

WITH 
    campaigns_data AS 
    (SELECT 
        date, 
        campaign_send_datetime::date as campaign_date,
        campaign_id, 
        campaign_name, 
        campaign_subject, 
        flow_id, 
        flow_name,
        sum(coalesce(klaviyo_total_recipients,0)) as total_recipients, 
        sum(coalesce(klaviyo_received_email,0)) as received, 
        sum(coalesce(klaviyo_opened_email_unique,0)) as opens, 
        sum(coalesce(klaviyo_clicked_email_unique,0)) as clicks,
        sum(coalesce(klaviyo_bounced_email,0)) as bounced,
        sum(coalesce(klaviyo_unsubscribed,0)) as unsubscribed,
        sum(coalesce(klaviyo_subscribed_to_list,0)) as subscribed

    FROM {{ source(schema_name, table_name) }})

SELECT *,
    campaign_id||date as unique_key
FROM campaigns_data
