/* create temp table */
create temp table pp_action_profiles_update (like mobile_commons_staging.pp_action_profiles); 

insert into pp_action_profiles_update
select
id as profile_id
,phone_number
,first_name
,last_name
,email
,status
,convert_timezone('EDT',created_at::timestamp) as created_at
,convert_timezone('EDT',updated_at::timestamp) as updated_at
,source_id
,source_type
,source_name
,source_opt_in_path_id
,convert_timezone('EDT',opted_out_at::timestamp) as opted_out_at
,opted_out_source
,address_street1
,address_street2
,address_city
,address_state
,address_postal_code
,address_country
from
(select *, row_number() over (partition by id order by updated_at::timestamp desc) as rownumber
 from anneramirez.mc_action_profiles)
 where rownumber=1;

/* remove old outdated rows */
begin transaction;

delete from mobile_commons_staging.pp_action_profiles
using pp_action_profiles_update 
where pp_action_profiles.profile_id=pp_action_profiles_update.profile_id;

/* insert new and updated rows */
insert into mobile_commons_staging.pp_action_profiles
select * from pp_action_profiles_update;

end transaction;

drop table pp_action_profiles_update;


/* UPDATE SCHEMA/TABLE NAMES AFTER MC MIGRATION */

/* create temp table */
create temp table profiles_update (like mobile_commons.profiles); 

insert into profiles_update
select
id as profile_id
,phone_number
,first_name
,last_name
,email
,status
,convert_timezone('EDT',created_at::timestamp) as created_at
,convert_timezone('EDT',updated_at::timestamp) as updated_at
,source_id
,source_type
,source_name
,source_opt_in_path_id
,convert_timezone('EDT',opted_out_at::timestamp) as opted_out_at
,opted_out_source
,address_street1
,address_street2
,address_city
,address_state
,address_postal_code
,address_country
from
(select *, row_number() over (partition by id order by updated_at::timestamp desc) as rownumber
 from anneramirez.mc_action_profiles)
 where rownumber=1;

/* remove old outdated rows */
begin transaction;

delete from mobile_commons.profiles
using pp_action_profiles_update 
where profiles.profile_id=profiles_update.profile_id;

/* insert new and updated rows */
insert into mobile_commons.profiles
select * from profiles_update;

end transaction;

drop table profiles_update;