drop table anneramirez.ppaf_membership;
create table anneramirez.ppaf_membership as

--still need to look at email/phone status

select
null::varchar(1024) as ppid,
eavan.vanid,
--contact_guid,
rd.rc_bios__account as rdaccountid,
null::varchar(1024) as mcprofileid,
(case when rd.lifetime is not null then 'lifetime'
 when rd.contributing >= current_date-365 then 'contributing'
 when coalesce(eavan.associate,rd.associate) >= current_date-365 then 'associate'
 else 'n/a' end) as current_membership_status, --lifetime, contributing, associate, n/a
rd.lifetime::date as lifetime_startdate,
rd.contributing::date as contributing_startdate,
coalesce(eavan.associate,rd.associate)::date as associate_startdate


from
(select pref.*,alt.valuex as vanid from
(select
coalesce(life.rc_bios__account,a.rc_bios__account) as rc_bios__account,
life.lifetime as lifetime,
a.contributing as contributing,
a.associate as associate
from
(select coalesce(contr.rc_bios__account,assoc.rc_bios__account) as rc_bios__account,contr.contributing,assoc.associate from
(select rc_bios__account,max(rc_bios__start_date) as associate from rounddata.rc_bios__preference
where rc_bios__code_value in ('NLACTFUND ASSOCIATE')
and rc_bios__active<>'false'
and isdeleted<>'true'
and delete_flag<>'Y'
and (rc_bios__end_date::date>=current_date or rc_bios__end_date is null)
 group by 1
) assoc
full outer join            
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
(select rc_bios__account,min(rc_bios__start_date) as lifetime from rounddata.rc_bios__preference
where rc_bios__code_value in ('NLACTFUND LIFETIME')
and rc_bios__active<>'false'
and isdeleted<>'true'
and delete_flag<>'Y'
and (rc_bios__end_date::date>=current_date or rc_bios__end_date is null)
 group by 1
) life on a.rc_bios__account=life.rc_bios__account            
) pref
left join (select accountx,min(valuex) as valuex from rounddata.alternate_id where typex='Van' and active='true' group by 1) alt --adjust to account for many to one
 on pref.rc_bios__account=alt.accountx
) rd 
         
full outer join
(select
coalesce(assoc.vanid,a.vanid) as vanid,
a.datecreated::date as lifetime,
a.datecanvassed::date as contributing,
(case when assoc.surveyresponseid=860301 then assoc.datecanvassed::date else null end) as associate
 
 from 
 (select coalesce(life.vanid,contr.vanid) as vanid, life.datecreated,contr.datecanvassed from
   (select vanid, max(datecreated) as datecreated from vansync.ppfa_contactsactivistcodes_mym  where activistcodeid=4304186 group by vanid) life
 full outer join (select * from
 (select *, row_number () over (partition by vanid order by datecanvassed desc, datecreated desc) as rownum from vansync.ppfa_contactssurveyresponses_mym where surveyquestionid=203939 and datecanvassed::date<=current_date)
                  where rownum=1) contr on life.vanid=contr.vanid) a
 
 full outer join (select * from
 (select *, row_number () over (partition by vanid order by datecanvassed desc, datecreated desc) as rownum from vansync.ppfa_contactssurveyresponses_mym where surveyquestionid=203938 and datecanvassed::date<=current_date) 
                  where rownum=1) assoc on a.vanid=assoc.vanid
group by 1,2,3,4) eavan 
 on rd.vanid=eavan.vanid
order by current_membership_status, associate_startdate desc, contributing_startdate desc
 --assoc member question id=203938 yes response id=860301 | contrib member question id=203939 yes response id=860303