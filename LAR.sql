SELECT 
acc.c3_affiliate_number as home_c3_affiliate,
opp.accountid as account_id,
acc.auto_account_number as sf_account_number,
opp.affiliate_number as affiliate_id,
sub.affiliate_number as sub_affiliate_id,
acc.rc_bios__preferred_contact_ema as email,
con.title as title,
con.firstname as first,
con.lastname as last,
con.mailingstreet as add1,
con.mailingcity as city,
con.mailingstate as state,
con.mailingpostalcode as zip,
acc.rc_bios__preferred_contact_pho as phone,
par.rc_giving__expected_giving_amo::decimal(19,2) as parent_original_amount,
opp.amount::decimal(19,2) as opp_amount, --probably base calculation on this
sub.amount::decimal(19,2) as sub_amount,
opp.final_amount as opp_final_amount,
(case when opp.gau_credit_code ='C325015' then (opp.amount *.9)::decimal(19,2)
when opp.gau_credit_code ='C340100UR422ONL' then (opp.amount *.56)::decimal(19,2)
 else null end) as payout_amount,
opp.gau_credit_code as gau_credit_code,
sub."gau_1" as sub_gau_credit_code,
opp.closedate::date as gift_date,
opp.id as sf_gift_id,
par.acquired_batch_seq as contribution_id,
opp.gau_debit_code,
(case when opp.gau_debit_code in ('C312610', 'C312615') then 'TRUE'
 else null end) as printable_form_gift,
alt.valuex as eavan_id,
par.originator,
sub.adjustment_type,
sub.adjustment_date,
sub.subledger_universe

FROM (select * from rounddata.subledger where subledger_universe <> 'New Transaction') sub
inner join rounddata.opportunity opp on opp.id=sub.transaction_id 
left join (select * from rounddata.opportunity where recordtypeid='01236000001HtyQAAS') par on opp.rc_giving__parent=par.id
left join rounddata.accountx acc on opp.accountid=acc.id
left join rounddata.contact con on acc.rc_bios__preferred_contact=con.id
left join (select * from (select *,row_number() over (partition by accountx order by createddate desc, lastmodifieddate desc) as rank from rounddata.alternate_id where active='true' and typex='Van') where rank=1) alt on opp.accountid=alt.accountx
where (opp.gau_credit_code in ('C325015', 'C340100UR422ONL') OR sub.gau_1 in ('C325015', 'C340100UR422ONL'))
and ((opp.closedate::date like '2019-04%' AND sub.adjustment_date > '2019-05-28 15:39:01')
or (opp.closedate::date like '2019-05%' AND sub.adjustment_date > '2019-07-16 11:11:34')
or (opp.closedate::date like '2019-06%' AND sub.adjustment_date > '2019-07-26 10:30:09')
or (opp.closedate::date < '2019-04-01' AND sub.adjustment_date::date > '2019-04-26'))
and opp.recordtypeid='01236000001HtyZAAS'
and opp.delete_flag<>'Y'
--and (opp.affiliate_number like '09%' or opp.affiliate_number like '9____')
order by opp.affiliate_number asc, opp.closedate asc