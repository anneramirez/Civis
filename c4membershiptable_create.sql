--while testing
drop table anneramirez.ppaf_membership;
create table anneramirez.ppaf_membership as
--insert into anneramirez.ppaf_membership

--still need to look at email/phone status
--any reason to pull in non-members or expired members??
--any reason to pull lifetime/contributing dates from eavan? or just stick with rD

select
distinct ppid,
(case when max(status)=3 then 'lifetime'
 when max(status)=2 then 'contributing'
 when max(status)=1 then 'associate'
 else null end) as current_membership_status,
 (case when max(status)=3 then min(lifetime_startdate)
 when max(status)=2 then max(contributing_startdate)
 when max(status)=1 then max(associate_startdate)
 else null end) as membership_start_date,
max(email_status) as email_status
from
(select
distinct idr.resolved_id as ppid,
(case when rd.lifetime is not null then 3
 when coalesce(rd.contributing,eavan.contributing) is not null then 2
 when (coalesce(eavan.associate,rd.associate) is not null AND email.emailsubscriptionstatusid=2) then 1
 else null end) as status, --lifetime, contributing, associate
rd.lifetime::date as lifetime_startdate,
coalesce(rd.contributing,eavan.contributing)::date as contributing_startdate,
coalesce(eavan.associate,rd.associate)::date as associate_startdate,
email.email as email,
(case when email.emailsubscriptionstatusid=2 then 'opt-in' else null end) as email_status


from
/*** Customer Graph ***/
ppfa_golden.current_customer_graph idr

/*** roundData membership ***/
left join
(select
distinct con.id as contact_id,
pref.*
from
 --pull membership from preferences table
(select
rc_bios__account,
lifetime,
contributing,
associate
from

 /*lifetime*/
(select rc_bios__account,min(rc_bios__start_date) as lifetime from rounddata.rc_bios__preference
where rc_bios__code_value in ('NLACTFUND LIFETIME')
and rc_bios__active<>'false'
and isdeleted<>'true'
and delete_flag<>'Y'
and active_duplicate_detection='ACTIVE'
and (rc_bios__end_date::date>=current_date or rc_bios__end_date is null)
 group by 1
) life

 /*contributing*/
full outer join  
(select rc_bios__account,max(rc_bios__start_date) as contributing from rounddata.rc_bios__preference
where rc_bios__code_value in ('NLACTFUND ANNUAL')
and rc_bios__active<>'false'
and isdeleted<>'true'
and delete_flag<>'Y'
and active_duplicate_detection='ACTIVE'
and (rc_bios__end_date::date>=current_date or rc_bios__end_date is null)
and rc_bios__start_date>=current_date-365
 group by 1
) contr using (rc_bios__account) 
 
 /*associate*/
full outer join
(select rc_bios__account,max(rc_bios__start_date) as associate from rounddata.rc_bios__preference
where rc_bios__code_value in ('NLACTFUND ASSOCIATE')
and rc_bios__active<>'false'
and isdeleted<>'true'
and delete_flag<>'Y'
and active_duplicate_detection='ACTIVE'
and (rc_bios__end_date::date>=current_date or rc_bios__end_date is null)
and rc_bios__start_date>=current_date-365
 group by 1
) assoc using (rc_bios__account)
 ) pref
 --join to contact table so we can connect to idr
left join rounddata.contact con on pref.rc_bios__account=con.accountid
 ) rd on rd.contact_id=idr.source_primary_key


/*** EAVAN membership ***/
left join
(select
vanid,
life.datecreated::date as lifetime,
contr.datecanvassed::date as contributing,
(case when assoc.surveyresponseid=860301 then assoc.datecanvassed::date else null end) as associate
from 

 /*lifetime*/
(select vanid, min(datecreated) as datecreated from 
 vansync.ppfa_contactsactivistcodes_mym 
 where activistcodeid=4304186 
 group by vanid
) life
 
 /*contributing*/
full outer join 
(select * from
 (select *, row_number () over (partition by vanid order by datecanvassed desc, datecreated desc) as rownum from 
  vansync.ppfa_contactssurveyresponses_mym where surveyquestionid=203939 and datecanvassed::date<=current_date and datecanvassed::date>=current_date-365)
 where rownum=1
) contr using (vanid)
 
 /*associate*/
full outer join 
(select * from
 (select *, row_number () over (partition by vanid order by datecanvassed desc, datecreated desc) as rownum from 
  vansync.ppfa_contactssurveyresponses_mym where surveyquestionid=203938 and datecanvassed::date<=current_date and datecanvassed::date>=current_date-365) 
 where rownum=1 and surveyresponseid=860301
) assoc using (vanid)
group by 1,2,3,4
) eavan on eavan.vanid=idr.source_primary_key

 
/*** Mobile Commons membership ***/
--in progress


/* phone */
--pulling in from ppid right now
--need to put together MC tables first
--in progress



/* email */
left join
(select distinct vanid,email,emailsubscriptionstatusid from
 (select *, row_number () over (partition by vanid order by sub.emailsubscriptionstatusid desc, ce.preferredemail desc, coalesce(sub.datecreated,ce.datecreated) desc) as rownum
  from vansync.ppfa_contactsemails_mym ce left join vansync.ppfa_emailsubscriptions_mym sub using (email) where sub.committeeid=9816 and ce.datesuppressed is null)
 where rownum=1) email on email.vanid=idr.source_primary_key
where status is not null
 )
 group by ppid
order by membership_start_date desc
 --assoc member question id=203938 yes response id=860301 | contrib member question id=203939 yes response id=860303