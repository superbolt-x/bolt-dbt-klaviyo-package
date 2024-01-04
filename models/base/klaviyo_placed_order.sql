{%- set klav_schema_name, klav_schema_name = 'supermetrics_raw', 'klav_placed_orders' -%}

{%- set sho_schema_name, sho_table_name = 'shopify_base', 'shopify_orders' -%}

with klaviyo_data as (select date, campaign_id, purchase_id::varchar as order_id
from {{ source(klav_schema_name,klav_schema_name) }}
where purchase_id != ''
and campaign_id != ''
and shopify_placed_order > 0
order by date asc)
  
, shopify_data as (select order_id::varchar, 
datediff(day,customer_acquisition_date,date) as delay,
case when customer_order_index = 1 then 1 else 0 end as first_orders,
case when customer_order_index > 1 then 1 else 0 end as repeat_orders,
case when customer_order_index = 1 then total_revenue else 0 end as first_revenue,
case when customer_order_index > 1 then total_revenue else 0 end as repeat_revenue
from {{ source(shopify_base,shopify_orders) }}
)

select
date, 
campaign_id,
sum(first_orders+repeat_orders) as total_orders,
sum(first_orders) as first_orders,
sum(repeat_orders) as repeat_orders,
sum(first_revenue+repeat_revenue) as total_revenue,
sum(first_revenue) as first_orders_revenue,
sum(repeat_revenue) as repeat_orders_revenue
from klaviyo_data left join shopify_data USING(order_id)
where campaign_id != ''
group by 1,2
