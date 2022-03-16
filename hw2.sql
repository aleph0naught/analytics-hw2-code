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
    ,sum(case when status = 4 then cost_for_customer else 0 end) as earned_sum
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
    ,sum(case when status = 4 then cost_for_customer else 0 end) as earned_sum
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
    ,sum(case when status = 4 then cost_for_customer else 0 end) as earned_sum
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


drop table if exists dmitrkozlovsk.agg_delivery;
create table dmitrkozlovsk.agg_delivery as
select
    'day' as time_type
    ,date_trunc('day', utc_created_dttm) as time_value
    ,city_name
    ,is_fast_food_flg
    ,asap_flg
    ,place_name
    ,case when code ~ '^(courier)' then 1 else 0 end as courier_cancelled
    ,case when split_part(code, '.', 1) = 'courier' then split_part(code, '.', 2) end as courier_cancel_reason
    ,delivery_type
    ,sum(case when status = 4 then 1 else 0 end) as finish_count
    ,sum(case when code is not null and code ~ '^(courier)' then 1 else 0 end) as courier_canceled_count
    ,sum(case when code is not null and not code ~ '^(duplicate|courier)' then 1 else 0 end) as other_canceled_count
    ,sum(case when code is not null and code ~ '^(courier)' then cost_for_customer else 0 end) as courier_sum
    ,sum(case when status = 4 then cost_for_customer else 0 end) as earned_sum
    ,sum(case when code is not null and not code ~ '^(duplicate|courier)' then cost_for_customer else 0 end) as other_sum
    ,count(distinct courier_id) as user_count
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then extract(hours from utc_delivered_dttm - utc_taken_dttm)*60 + extract(minutes from utc_delivered_dttm - utc_taken_dttm) else 0 end) as delivered_to_taken_time
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then extract(hours from utc_taken_dttm - utc_courier_arrived_to_place_dttm)*60 + extract(minutes from utc_taken_dttm - utc_courier_arrived_to_place_dttm)else 0 end) as taken_to_courier_arrived_time
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then extract(hours from utc_courier_arrived_to_place_dttm - utc_adopted_by_courier_dttm) * 60 + extract(minutes from utc_courier_arrived_to_place_dttm - utc_adopted_by_courier_dttm) else 0 end) as courier_arrived_to_adopted_time
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then extract(hours from utc_adopted_by_courier_dttm - utc_courier_assigned_dttm)*60 + extract(minutes from utc_adopted_by_courier_dttm - utc_courier_assigned_dttm) else 0 end) as adopted_to_courier_assigned_time
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then extract(hours from utc_courier_assigned_dttm - utc_created_dttm)*60+extract(minutes from utc_courier_assigned_dttm - utc_created_dttm) else 0 end) as courier_assigned_to_created_time
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then time_to_delivery - pre_delivery_time else 0 end) as pre_time_diff
    ,sum(time_to_delivery) as time_to_delivery_sum
    ,sum(extract(hours from utc_delivered_dttm - utc_created_dttm)*60+extract(minutes from utc_delivered_dttm - utc_created_dttm)) as click_to_eat
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then 1 else 0 end) as time_diff_count
from
     dmitrkozlovsk.cdm_orders
group by
    1, 2, 3, 4, 5, 6, 7, 8, 9
union all
select
    'week' as time_type
    ,date_trunc('week', utc_created_dttm) as time_value
    ,city_name
    ,is_fast_food_flg
    ,asap_flg
    ,place_name
    ,case when code ~ '^(courier)' then 1 else 0 end as courier_cancelled
    ,case when split_part(code, '.', 1) = 'courier' then split_part(code, '.', 2) end as courier_cancel_reason
    ,delivery_type
    ,sum(case when status = 4 then 1 else 0 end) as finish_count
    ,sum(case when code is not null and code ~ '^(courier)' then 1 else 0 end) as courier_canceled_count
    ,sum(case when code is not null and not code ~ '^(duplicate|courier)' then 1 else 0 end) as other_canceled_count
    ,sum(case when code is not null and code ~ '^(courier)' then cost_for_customer else 0 end) as courier_sum
    ,sum(case when status = 4 then cost_for_customer else 0 end) as earned_sum
    ,sum(case when code is not null and not code ~ '^(duplicate|courier)' then cost_for_customer else 0 end) as other_sum
    ,count(distinct courier_id) as user_count
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then extract(hours from utc_delivered_dttm - utc_taken_dttm)*60 + extract(minutes from utc_delivered_dttm - utc_taken_dttm) else 0 end) as delivered_to_taken_time
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then extract(hours from utc_taken_dttm - utc_courier_arrived_to_place_dttm)*60 + extract(minutes from utc_taken_dttm - utc_courier_arrived_to_place_dttm)else 0 end) as taken_to_courier_arrived_time
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then extract(hours from utc_courier_arrived_to_place_dttm - utc_adopted_by_courier_dttm) * 60 + extract(minutes from utc_courier_arrived_to_place_dttm - utc_adopted_by_courier_dttm) else 0 end) as courier_arrived_to_adopted_time
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then extract(hours from utc_adopted_by_courier_dttm - utc_courier_assigned_dttm)*60 + extract(minutes from utc_adopted_by_courier_dttm - utc_courier_assigned_dttm) else 0 end) as adopted_to_courier_assigned_time
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then extract(hours from utc_courier_assigned_dttm - utc_created_dttm)*60+extract(minutes from utc_courier_assigned_dttm - utc_created_dttm) else 0 end) as courier_assigned_to_created_time
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then time_to_delivery - pre_delivery_time else 0 end) as pre_time_diff
    ,sum(time_to_delivery) as time_to_delivery_sum
    ,sum(extract(hours from utc_delivered_dttm - utc_created_dttm)*60+extract(minutes from utc_delivered_dttm - utc_created_dttm)) as click_to_eat
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then 1 else 0 end) as time_diff_count
from
     dmitrkozlovsk.cdm_orders
group by
    1, 2, 3, 4, 5, 6, 7, 8, 9
union all
select
    'month' as time_type
    ,date_trunc('month', utc_created_dttm) as time_value
    ,city_name
    ,is_fast_food_flg
    ,asap_flg
    ,place_name
    ,case when code ~ '^(courier)' then 1 else 0 end as courier_cancelled
    ,case when split_part(code, '.', 1) = 'courier' then split_part(code, '.', 2) end as courier_cancel_reason
    ,delivery_type
    ,sum(case when status = 4 then 1 else 0 end) as finish_count
    ,sum(case when code is not null and code ~ '^(courier)' then 1 else 0 end) as courier_canceled_count
    ,sum(case when code is not null and not code ~ '^(duplicate|courier)' then 1 else 0 end) as other_canceled_count
    ,sum(case when code is not null and code ~ '^(courier)' then cost_for_customer else 0 end) as courier_sum
    ,sum(case when status = 4 then cost_for_customer else 0 end) as earned_sum
    ,sum(case when code is not null and not code ~ '^(duplicate|courier)' then cost_for_customer else 0 end) as other_sum
    ,count(distinct courier_id) as user_count
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then extract(hours from utc_delivered_dttm - utc_taken_dttm)*60 + extract(minutes from utc_delivered_dttm - utc_taken_dttm) else 0 end) as delivered_to_taken_time
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then extract(hours from utc_taken_dttm - utc_courier_arrived_to_place_dttm)*60 + extract(minutes from utc_taken_dttm - utc_courier_arrived_to_place_dttm)else 0 end) as taken_to_courier_arrived_time
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then extract(hours from utc_courier_arrived_to_place_dttm - utc_adopted_by_courier_dttm) * 60 + extract(minutes from utc_courier_arrived_to_place_dttm - utc_adopted_by_courier_dttm) else 0 end) as courier_arrived_to_adopted_time
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then extract(hours from utc_adopted_by_courier_dttm - utc_courier_assigned_dttm)*60 + extract(minutes from utc_adopted_by_courier_dttm - utc_courier_assigned_dttm) else 0 end) as adopted_to_courier_assigned_time
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then extract(hours from utc_courier_assigned_dttm - utc_created_dttm)*60+extract(minutes from utc_courier_assigned_dttm - utc_created_dttm) else 0 end) as courier_assigned_to_created_time
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then time_to_delivery - pre_delivery_time else 0 end) as pre_time_diff
    ,sum(time_to_delivery) as time_to_delivery_sum
    ,sum(extract(hours from utc_delivered_dttm - utc_created_dttm)*60+extract(minutes from utc_delivered_dttm - utc_created_dttm)) as click_to_eat
    ,sum(case when delivery_type = 'native'
    and cancel_reason_id is null
    and not (utc_courier_assigned_dttm is null
            or utc_adopted_by_courier_dttm is null
            or utc_place_confirmed_dttm is null
            or utc_courier_arrived_to_place_dttm is null
            or utc_taken_dttm is null
            or utc_delivered_dttm is null) then 1 else 0 end) as time_diff_count
from
     dmitrkozlovsk.cdm_orders
group by
    1, 2, 3, 4, 5, 6, 7, 8, 9;
GRANT ALL PRIVILEGES ON dmitrkozlovsk.agg_delivery TO PUBLIC;

drop table if exists dmitrkozlovsk.agg_place;
create table dmitrkozlovsk.agg_place as
select
    'day' as time_type
    ,date_trunc('day', utc_created_dttm) as time_value
    ,city_name
    ,is_fast_food_flg
    ,asap_flg
    ,place_name
    ,cancel_reason_type
    ,sum(case when code ~ '^(place)' then 1 else 0 end) as cancelled_count
    ,sum(case when status = 4 then 1 else 0 end) as finish_count
    ,count(place_id) as place_id_num
    ,case when split_part(code, '.', 1) = 'place' then split_part(code, '.', 2) end as cancel_reason
    ,sum(case when not code ~ '^(place|duplicate)' then 1 else 0 end) as other_count
    ,sum(case when code ~ '^(place)' then cost_for_customer end) as place_revenue
    ,sum(case when not code ~ '^(place|duplicate)' then cost_for_customer end) as other_revenue
    ,sum(case when status = 4 then cost_for_customer end) as finished_revenue
from
     dmitrkozlovsk.cdm_orders
group by
    1, 2, 3, 4, 5, 6, 7, 11
union all
select
    'week' as time_type
    ,date_trunc('week', utc_created_dttm) as time_value
    ,city_name
    ,is_fast_food_flg
    ,asap_flg
    ,place_name
    ,cancel_reason_type
    ,sum(case when code ~ '^(place)' then 1 else 0 end) as cancelled_count
    ,sum(case when status = 4 then 1 else 0 end) as finish_count
    ,count(place_id) as place_id_num
    ,case when split_part(code, '.', 1) = 'place' then split_part(code, '.', 2) end as cancel_reason
    ,sum(case when not code ~ '^(place|duplicate)' then 1 else 0 end) as other_count
    ,sum(case when code ~ '^(place)' then cost_for_customer end) as place_revenue
    ,sum(case when not code ~ '^(place|duplicate)' then cost_for_customer end) as other_revenue
    ,sum(case when status = 4 then cost_for_customer end) as finished_revenue
from
     dmitrkozlovsk.cdm_orders
group by
    1, 2, 3, 4, 5, 6, 7, 11
union all
select
    'month' as time_type
    ,date_trunc('month', utc_created_dttm) as time_value
    ,city_name
    ,is_fast_food_flg
    ,asap_flg
    ,place_name
    ,cancel_reason_type
    ,sum(case when code ~ '^(place)' then 1 else 0 end) as cancelled_count
    ,sum(case when status = 4 then 1 else 0 end) as finish_count
    ,count(place_id) as place_id_num
    ,case when split_part(code, '.', 1) = 'place' then split_part(code, '.', 2) end as cancel_reason
    ,sum(case when not code ~ '^(place|duplicate)' then 1 else 0 end) as other_count
    ,sum(case when code ~ '^(place)' then cost_for_customer end) as place_revenue
    ,sum(case when not code ~ '^(place|duplicate)' then cost_for_customer end) as other_revenue
    ,sum(case when status = 4 then cost_for_customer end) as finished_revenue
from
     dmitrkozlovsk.cdm_orders
group by
    1, 2, 3, 4, 5, 6, 7, 11;
GRANT ALL PRIVILEGES ON dmitrkozlovsk.agg_place TO PUBLIC;
