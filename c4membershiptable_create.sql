truncate anneramirez.ppaf_membership;
insert into anneramirez.ppaf_membership

--still need to look at email/phone status
--full outer joins are supes obnox
--any reason to pull in non-members or expired members??
--any reason to pull lifetime/contributing dates from eavan? or just stick with rD


select
coalesce(ppidea.resolved_id,ppidrd.resolved_id) as ppid,
eavan.vanid,
--contact_guid,
rd.rc_bios__account as rdaccountid,
mc.profile_id as mcprofileid,
(case when rd.lifetime is not null then 'lifetime'
 when coalesce(rd.contributing,eavan.contributing) >= current_date-365 then 'contributing'
 when (coalesce(eavan.associate,rd.associate) >= current_date-365 AND (email.emailsubscriptionstatusid=2 or mc.status='Active Subscriber')) then 'associate'
 else 'n/a' end) as current_membership_status, --lifetime, contributing, associate, n/a
rd.lifetime::date as lifetime_startdate,
coalesce(rd.contributing,eavan.contributing)::date as contributing_startdate,
coalesce(eavan.associate,rd.associate)::date as associate_startdate,
email.email as email,
(case when email.emailsubscriptionstatusid=2 then 'opt-in' else null end) as email_status,
mc.phone_number as mobile,
(case when mc.status='Active Subscriber' then 'opt-in' else null end) as mobile_status


from
/*** EAVAN membership ***/
(select
coalesce(assoc.vanid,a.vanid) as vanid,
a.datecreated::date as lifetime,
a.datecanvassed::date as contributing,
(case when assoc.surveyresponseid=860301 then assoc.datecanvassed::date else null end) as associate
from 
(select coalesce(life.vanid,contr.vanid) as vanid, life.datecreated,contr.datecanvassed from
/*lifetime*/
(select vanid, max(datecreated) as datecreated from vansync.ppfa_contactsactivistcodes_mym  where activistcodeid=4304186 group by vanid) life
 full outer join 
/*contributing*/
 (select * from
 (select *, row_number () over (partition by vanid order by datecanvassed desc, datecreated desc) as rownum from vansync.ppfa_contactssurveyresponses_mym where surveyquestionid=203939 and datecanvassed::date<=current_date)
                  where rownum=1) contr on life.vanid=contr.vanid) a
 full outer join 
/*associate*/
(select * from
(select *, row_number () over (partition by vanid order by datecanvassed desc, datecreated desc) as rownum from vansync.ppfa_contactssurveyresponses_mym where surveyquestionid=203938 and datecanvassed::date<=current_date) 
                  where rownum=1 and surveyresponseid=860301) assoc on a.vanid=assoc.vanid
group by 1,2,3,4) eavan 


/*** roundData membership ***/
full outer join
(select pref.*,alt.valuex as vanid from
/*membership rollup*/
(select
coalesce(life.rc_bios__account,a.rc_bios__account) as rc_bios__account,
life.lifetime as lifetime,
a.contributing as contributing,
a.associate as associate
from
(select coalesce(contr.rc_bios__account,assoc.rc_bios__account) as rc_bios__account,contr.contributing,assoc.associate from
/*associate*/
(select rc_bios__account,max(rc_bios__start_date) as associate from rounddata.rc_bios__preference
where rc_bios__code_value in ('NLACTFUND ASSOCIATE')
and rc_bios__active<>'false'
and isdeleted<>'true'
and delete_flag<>'Y'
and (rc_bios__end_date::date>=current_date or rc_bios__end_date is null)
 group by 1
) assoc
full outer join  
/*contributing*/
(select rc_bios__account,max(rc_bios__start_date) as contributing from rounddata.rc_bios__preference
where rc_bios__code_value in ('NLACTFUND ANNUAL')
and rc_bios__active<>'false'
and isdeleted<>'true'
and delete_flag<>'Y'
and (rc_bios__end_date::date>=current_date or rc_bios__end_date is null)
 group by 1
) contr on assoc.rc_bios__account=contr.rc_bios__account 
) a
full outer join
/*lifetime*/
(select rc_bios__account,min(rc_bios__start_date) as lifetime from rounddata.rc_bios__preference
where rc_bios__code_value in ('NLACTFUND LIFETIME')
and rc_bios__active<>'false'
and isdeleted<>'true'
and delete_flag<>'Y'
and (rc_bios__end_date::date>=current_date or rc_bios__end_date is null)
 group by 1
) life on a.rc_bios__account=life.rc_bios__account            
) pref
/* get vanid */ 
left join (select accountx,min(valuex) as valuex from rounddata.alternate_id where typex='Van' and active='true' group by 1) alt --adjust to account for many to one
 on pref.rc_bios__account=alt.accountx
) rd on rd.vanid=eavan.vanid

/* PPID - VANID */ 
left join ppfa_golden.current_customer_graph ppidea on ppidea.source_primary_key=eavan.vanid
 
/* PPID - RDID */ 
left join ppfa_golden.current_customer_graph ppidrd on ppidrd.source_primary_key=rd.rc_bios__account

/* email */
left join
(select distinct vanid,email,emailsubscriptionstatusid from
 (select *, row_number () over (partition by vanid order by sub.emailsubscriptionstatusid desc, ce.preferredemail desc, coalesce(sub.datecreated,ce.datecreated) desc) as rownum
  from vansync.ppfa_contactsemails_mym ce left join vansync.ppfa_emailsubscriptions_mym sub using (email) where sub.committeeid=9816 and ce.datesuppressed is null)
 where rownum=1) email on email.vanid=eavan.vanid

/* phone */
--pulling in from ppid right now
--need to put together MC tables first
left join mobile_commons.profiles mc on ppidea.source_primary_key=mc.profile_id

           
order by current_membership_status, associate_startdate desc, contributing_startdate desc
 --assoc member question id=203938 yes response id=860301 | contrib member question id=203939 yes response id=860303