drop table if exists dmitrkozlovsk.ods_place;
create table dmitrkozlovsk.ods_place as
select
    (doc #>> '{id}')::integer as place_id,
    (doc #>> '{autocalls_enabled_flg}')::boolean as autocalls_enabled_flg,
    (doc #>> '{is_fast_food_flg}')::boolean as is_fast_food_flg,
    (doc #>> '{average_marketplace_delivery_time}')::double precision as average_marketplace_delivery_time,
    (doc #>> '{average_preparation_time}')::double precision as average_preparation_time,
    (doc #>> '{enabled}')::integer as enabled,
    doc #>> '{name}' as place_name,
    doc #>> '{couriers_type}' as couriers_type,
    doc #>> '{type}' as type,
    (doc #>> '{utc_created_dttm}')::timestamp as utc_created_dttm_place
from
     raw_data.place;
GRANT ALL PRIVILEGES ON dmitrkozlovsk.ods_place TO PUBLIC;

drop table if exists dmitrkozlovsk.ods_order;
create table dmitrkozlovsk.ods_order as
select
    (doc #>> '{id}')::integer as id,
    (doc #>> '{user_id}')::integer as user_id,
    (doc #>> '{courier_id}')::integer as courier_id,
    doc #>> '{application_platform}' as application_platform,
    (doc #>> '{asap_flg}')::boolean as asap_flg,
    (doc #>> '{cancel_reason_id}')::integer as cancel_reason_id,
    doc #>> '{city_name}' as city_name,
    (doc #>> '{cost_for_customer}')::integer as cost_for_customer,
    doc #>> '{currency_code}' as currency_code,
    doc #>> '{delivery_type}' as delivery_type,
    (doc #>> '{persons_quantity}')::integer as persons_quantity,
    (doc #>> '{pre_delivery_time}')::integer as pre_delivery_time,
    (doc #>> '{place_id}')::integer as place_id,
    (doc #>> '{rating}')::double precision as rating,
    doc #>> '{region_timezone}' as region_timezone,
    (doc #>> '{status}')::integer as status,
    (doc #>> '{time_to_delivery}')::integer as time_to_delivery,
    (doc #>> '{time_to_place}')::integer as time_to_place,
    (doc #>> '{utc_created_dttm}')::timestamp as utc_created_dttm,
    (doc #>> '{utc_cancelled_dttm}')::timestamp as utc_cancelled_dttm,
    (doc #>> '{utc_courier_assigned_dttm}')::timestamp as utc_courier_assigned_dttm,
    (doc #>> '{utc_adopted_by_courier_dttm}')::timestamp as utc_adopted_by_courier_dttm,
    (doc #>> '{utc_place_confirmed_dttm}')::timestamp as utc_place_confirmed_dttm,
    (doc #>> '{utc_courier_arrived_to_place_dttm}')::timestamp as utc_courier_arrived_to_place_dttm ,
    (doc #>> '{utc_taken_dttm}')::timestamp as utc_taken_dttm,
    (doc #>> '{utc_delivered_dttm}')::timestamp as utc_delivered_dttm,
    (doc #>> '{utc_feedback_filled_dttm}')::timestamp as utc_feedback_filled_dttm
from
     raw_data.order;
GRANT ALL PRIVILEGES ON dmitrkozlovsk.ods_order TO PUBLIC;

drop table if exists dmitrkozlovsk.ods_cancel_reason;
create table dmitrkozlovsk.ods_cancel_reason as
select
    (doc #>> '{id}')::integer as cancel_reason_id,
    doc #>> '{code}' as code,
    doc #>> '{name}' as reason_name,
    doc #>> '{type}' as type,
    (doc #>> '{utc_created_dttm}')::timestamp as utc_created_dttm_reason,
    (doc #>> '{utc_deactivated_dttm}')::timestamp as utc_deactivated_dttm
from
     raw_data.cancel_reason;
GRANT ALL PRIVILEGES ON dmitrkozlovsk.ods_cancel_reason TO PUBLIC;

drop table if exists dmitrkozlovsk.cdm_orders;
create table dmitrkozlovsk.cdm_orders as
select
    t1.*
    ,t2.autocalls_enabled_flg
    ,t2.is_fast_food_flg
    ,t2.average_marketplace_delivery_time
    ,t2.average_preparation_time
    ,t2.enabled
    ,t2.place_name
    ,t2.couriers_type
    ,t2.type as place_type
    ,t3.code
    ,t3.reason_name
    ,t3.type as cancel_reason_type
    ,case
        when t1.cancel_reason_id is not null then 1
        else 0
    end as is_cancelled
from
    dmitrkozlovsk.ods_order t1
    left join dmitrkozlovsk.ods_place t2
        on t1.place_id = t2.place_id
    left join dmitrkozlovsk.ods_cancel_reason t3
        on t1.cancel_reason_id = t3.cancel_reason_id;
GRANT ALL PRIVILEGES ON dmitrkozlovsk.cdm_orders TO PUBLIC;

drop table if exists dmitrkozlovsk.agg_ceo_new;
create table dmitrkozlovsk.agg_ceo_new as
select
    'day' as time_type
    ,date_trunc('day', utc_created_dttm) as time_value
    ,city_name
    ,is_fast_food_flg
    ,asap_flg
    ,place_name
    ,delivery_type
    ,sum(case when status = 4 then 1 else 0 end) as finish_count
    ,sum(case when status = 4 and rating is not null then 1 else 0 end) as finish_rating_count
    ,sum(rating) as rating_sum
    ,sum(case when not code ~ '^(duplicate|client)' then 1 else 0 end) as cancelled_count
    ,sum(cost_for_customer) as earned_sum
    ,count(distinct user_id) as user_count
from
     dmitrkozlovsk.cdm_orders
group by
    1, 2, 3, 4, 5, 6, 7
union all
select
    'week' as time_type
    ,date_trunc('week', utc_created_dttm) as time_value
    ,city_name
    ,is_fast_food_flg
    ,asap_flg
    ,place_name
    ,delivery_type
    ,sum(case when status = 4 then 1 else 0 end) as finish_count
    ,sum(case when status = 4 and rating is not null then 1 else 0 end) as finish_rating_count
    ,sum(rating) as rating_sum
    ,sum(case when not code ~ '^(duplicate|client)' then 1 else 0 end) as cancelled_count
    ,sum(cost_for_customer) as earned_sum
    ,count(distinct user_id) as user_count
from
     dmitrkozlovsk.cdm_orders
group by
    1, 2, 3, 4, 5, 6, 7
union all
select
    'month' as time_type
    ,date_trunc('month', utc_created_dttm) as time_value
    ,city_name
    ,is_fast_food_flg
    ,asap_flg
    ,place_name
    ,delivery_type
    ,sum(case when status = 4 then 1 else 0 end) as finish_count
    ,sum(case when status = 4 and rating is not null then 1 else 0 end) as finish_rating_count
    ,sum(rating) as rating_sum
    ,sum(case when not code ~ '^(duplicate|client)' then 1 else 0 end) as cancelled_count
    ,sum(cost_for_customer) as earned_sum
    ,count(distinct user_id) as user_count
from
     dmitrkozlovsk.cdm_orders
group by
    1, 2, 3, 4, 5, 6, 7;
GRANT ALL PRIVILEGES ON dmitrkozlovsk.agg_ceo_new TO PUBLIC;


drop table if exists dmitrkozlovsk.agg_dev;
create table dmitrkozlovsk.agg_dev as
select
    'day' as time_type
    ,date_trunc('day', utc_created_dttm) as time_value
    ,application_platform
    ,split_part(code, '.', 1) as cancel_type
    ,code as cancel_subtype
    ,sum(case when status = 4 then 1 else 0 end) as finish_count
    ,sum(case when code is null then 0 else 1 end) as canceled_count
from
     dmitrkozlovsk.cdm_orders
group by
    1, 2, 3, 4, 5
union all
select
    'week' as time_type
    ,date_trunc('week', utc_created_dttm) as time_value
    ,application_platform
    ,split_part(code, '.', 1) as cancel_type
    ,code as cancel_subtype
    ,sum(case when status = 4 then 1 else 0 end) as finish_count
    ,sum(case when code is null then 0 else 1 end) as canceled_count
from
     dmitrkozlovsk.cdm_orders
group by
    1, 2, 3, 4, 5
union all
select
    'month' as time_type
    ,date_trunc('month', utc_created_dttm) as time_value
    ,application_platform
    ,split_part(code, '.', 1) as cancel_type
    ,code as cancel_subtype
    ,sum(case when status = 4 then 1 else 0 end) as finish_count
    ,sum(case when code is null then 0 else 1 end) as canceled_count
from
     dmitrkozlovsk.cdm_orders
group by
    1, 2, 3, 4, 5;
GRANT ALL PRIVILEGES ON dmitrkozlovsk.agg_dev TO PUBLIC;

-- Тут начал делать agg_delivery
drop table if exists dmitrkozlovsk.agg_delivery;
create table dmitrkozlovsk.agg_delivery as
select
    'day' as time_type
    ,date_trunc('day', utc_created_dttm) as time_value
    ,city_name
    ,is_fast_food_flg
    ,asap_flg
    ,place_name
    ,
    ,delivery_type
    ,sum(case when status = 4 then 1 else 0 end) as finish_count

from
     dmitrkozlovsk.cdm_orders
group by
    1, 2, 3, 4, 5
union all
select
    'week' as time_type
    ,date_trunc('week', utc_created_dttm) as time_value

from
     dmitrkozlovsk.cdm_orders
group by
    1, 2, 3, 4, 5
union all
select
    'month' as time_type
    ,date_trunc('month', utc_created_dttm) as time_value

from
     dmitrkozlovsk.cdm_orders
group by
    1, 2, 3, 4, 5;
GRANT ALL PRIVILEGES ON dmitrkozlovsk.agg_delivery TO PUBLIC;
