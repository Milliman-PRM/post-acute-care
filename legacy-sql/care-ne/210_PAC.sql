

IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_tmp_PAC_IP', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_tmp_PAC_IP;

SELECT
	[MEMBER_KEY]
	,INCURRED_YEAR_AND_MONTH
	,[ADM_DATE] as from_date
	,[DIS_DATE] as to_date
	,[CLIENT_MS_DRG_CODE]
	,[PRVDR_OSCAR_NUM] as ATT_PROV_ID
	,case
		when [CLIENT_MS_DRG_CODE] in ('945','946') then 'IRF' 
		when '0001' <= substring([PRVDR_OSCAR_NUM],3,4) and substring([PRVDR_OSCAR_NUM],3,4) <= '0879' then 'IP'
		when '1300' <= substring([PRVDR_OSCAR_NUM],3,4) and substring([PRVDR_OSCAR_NUM],3,4) <= '1399' then 'IP'
		when '2000' <= substring([PRVDR_OSCAR_NUM],3,4) and substring([PRVDR_OSCAR_NUM],3,4) <= '2299' then 'IRF'
		when '3025' <= substring([PRVDR_OSCAR_NUM],3,4) and substring([PRVDR_OSCAR_NUM],3,4) <='3099' then 'IRF'
		when substring([PRVDR_OSCAR_NUM],3,1) in ('T','R') then 'Rehab'
		else 'IPOth' end
		as typ
	,ROW_NUMBER() OVER (ORDER BY [MEMBER_KEY],INCURRED_YEAR_AND_MONTH,[ADM_DATE],[DIS_DATE],[CLIENT_MS_DRG_CODE],[PRVDR_OSCAR_NUM]) as rowN
	,sum(cast(MR_ADMITS_CASES_RAW as float)) as admits
	,sum(cast(mr_units_days_raw as float)) as days
	,sum(cast(amt_paid as float)) as paid
into
	MI_CareNE_ACOI.dbo.A_tmp_PAC_IP
from
	MI_CareNE_ACOI.dbo.dat_Claims
where
	HCG_MR_LINE in ('I11a','I11b','I12')
	and SV_STAT != 'D'
GROUP BY
	[MEMBER_KEY]
	,INCURRED_YEAR_AND_MONTH
	,[ADM_DATE]
	,[DIS_DATE]
	,[CLIENT_MS_DRG_CODE]
	,[PRVDR_OSCAR_NUM]
HAVING
	sum(cast(amt_paid as float)) > 0 and sum(cast(MR_ADMITS_CASES_RAW as float)) > 0
GO




IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_tmp_PAC_SNF', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_tmp_PAC_SNF;

SELECT
	'SNF' as typ
	,MEMBER_KEY
	,from_date
	,to_date
	,sum(admits) as admits
	,sum(days) as days
	,sum(paid) as paid
into
	MI_CareNE_ACOI.dbo.A_tmp_PAC_SNF
from
	(
	SELECT
		MEMBER_KEY
		,CS_CLAIM_ID_KEY
		,min(FROM_DATE) as from_date
		,max(TO_DATE) as to_date
		,sum(cast(MR_ADMITS_CASES_RAW as float)) as admits
		,sum(cast(mr_units_days_raw as float)) as days
		,sum(cast(amt_paid as float)) as paid
	from
		MI_CareNE_ACOI.dbo.dat_Claims
	where
		HCG_MR_LINE in ('I31')
	GROUP BY
		MEMBER_KEY
		,CS_CLAIM_ID_KEY
	) as a
group by
	MEMBER_KEY
	,from_date
	,to_date
having
	sum(days) > 0 and sum(paid) > 0



IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_tmp_PAC_HH', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_tmp_PAC_HH;


SELECT
	'HHA' as typ
	,MEMBER_KEY
	,from_date
	,to_date
	, sum(admits) as admits
	, sum(visits) as visits
	, sum(paid) as paid
into
	MI_CareNE_ACOI.dbo.A_tmp_PAC_HH
from
	(
	SELECT
		MEMBER_KEY
		,CLAIM_ID
		,min(from_date) as from_date
		,max(from_date) as to_date
		, case when max(SV_STAT) = 'R' then -1 else 1 end as admits
		, sum(case
			when substring(REV_CODE,1,3) in ('042','043','044','055','056','057') and SV_STAT = 'P' then 1
			when substring(REV_CODE,1,3) in ('042','043','044','055','056','057') and SV_STAT = 'R' then -1
			else 0 end) as visits
		, sum(cast(amt_paid as float)) as paid
	from
		MI_CareNE_ACOI.dbo.dat_Claims
	where
		HCG_MR_LINE in ('P82a')
		and SV_STAT != 'D'
	GROUP BY
		MEMBER_KEY
		,CLAIM_ID
	) as a
GROUP BY
	MEMBER_KEY
	,from_date
	,to_date
HAVING
	sum(paid) > 0 and sum(admits) > 0
GO




IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_tmp_PAC_IP_r', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_tmp_PAC_IP_r;

select
	*
into
	MI_CareNE_ACOI.dbo.A_tmp_PAC_IP_r
from
	MI_CareNE_ACOI.dbo.A_tmp_PAC_IP
where
	typ = 'IP'


declare @dt date
declare @MemKey int
declare @RowNum int
declare @RowMax int

select top 1 @dt=to_date
	from MI_CareNE_ACOI.dbo.A_tmp_PAC_IP_r order by RowN

select top 1 @MemKey=member_key
	from MI_CareNE_ACOI.dbo.A_tmp_PAC_IP_r order by RowN

set @RowNum = 0 

select top 1 @RowMax=max(RowN)
	from MI_CareNE_ACOI.dbo.A_tmp_PAC_IP_r


IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_tmp_PAC0', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_tmp_PAC0;
select top 1 *, 1 as idx
	into MI_CareNE_ACOI.dbo.A_tmp_PAC0
	from MI_CareNE_ACOI.dbo.A_tmp_PAC_IP_r order by RowN


WHILE @RowNum <= @RowMax
BEGIN
	set @RowNum = @RowNum + 1

	--print 'A '+cast(@RowNum as char(1)) + ' ' + cast(@memkey as varchar(255)) + ' ' + cast(@dt as varchar(255))

	insert into MI_CareNE_ACOI.dbo.A_tmp_PAC0
	select *, case when from_date < DATEADD(day,30,@dt) and member_key = @memkey then 0 else 1 end as idx
		from MI_CareNE_ACOI.dbo.A_tmp_PAC_IP_r
		where RowN = @RowNum+1
		order by RowN 
	
	--print 'B '+cast(@RowNum as char(1)) + ' ' + cast(@memkey as varchar(255)) + ' ' + cast(@dt as varchar(255))

	select @dt = case when from_date < DATEADD(day,30,@dt) and member_key = @memkey then @dt else to_date end
		from MI_CareNE_ACOI.dbo.A_tmp_PAC_IP_r
		where RowN = @RowNum+1
		order by RowN

	select @MemKey = member_key
		from MI_CareNE_ACOI.dbo.A_tmp_PAC_IP_r
		where RowN = @RowNum+1
		order by RowN

	--print 'C '+cast(@RowNum as char(1)) + ' ' + cast(@memkey as varchar(255)) + ' ' + cast(@dt as varchar(255))
	--select * from MI_CareNE_ACOI.dbo.A_tmp_PAC0

END




IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_tmp_PAC1', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_tmp_PAC1;

SELECT
	a.[MEMBER_KEY]
	,a.rowN
	,a.INCURRED_YEAR_AND_MONTH
	,a.from_date
	,a.to_date
	,a.[CLIENT_MS_DRG_CODE]
	,c.[DRG_Desc]
	,a.ATT_PROV_ID
	,a.typ
	,a.admits
	,a.days
	,a.paid
	,c.[DRG Family]
	,c.[DRG Family List]
	,y.[freq_wm_readm]
	,y.[freq_wm_IRF]
	,y.[freq_wm_SNF]
	,y.[freq_wm_HHA]
	,y.[alos_wm_readm]*[freq_wm_readm] as [alos_wm_readm]
	,y.[alos_wm_irf]*[freq_wm_irf] as [alos_wm_irf]
	,y.[alos_wm_snf]*[freq_wm_snf] as [alos_wm_snf]
	,y.[freq_lm_readm]
	,y.[freq_lm_IRF]
	,y.[freq_lm_SNF]
	,y.[freq_lm_HHA]
	,y.[alos_lm_readm]*[freq_lm_readm] as [alos_lm_readm]
	,y.[alos_lm_irf]*[freq_lm_irf] as [alos_lm_irf]
	,y.[alos_lm_snf]*[freq_lm_snf] as [alos_lm_snf]
	,max(case when d.typ is not null and z.readm_drg is null then a.admits else 0 end) as freq_readm
	,sum(case when z.readm_drg is null then d.admits else 0 end) as case_readm
	,sum(case when z.readm_drg is null then d.days else 0 end) as days_readm
	,sum(case when z.readm_drg is null then d.paid else 0 end) as paid_readm
into
	MI_CareNE_ACOI.dbo.A_tmp_PAC1
from
	MI_CareNE_ACOI.dbo.A_tmp_PAC0 as a
	left join ref_ACOI.dbo.ref_DRG_Family_Map as c
		on a.[CLIENT_MS_DRG_CODE] = REPLACE(STR(c.DRG,3),' ','0')
	left join ref_ACOI.dbo.ref_PAC_Benchmarks as y
		on a.[CLIENT_MS_DRG_CODE] = y.DRG
	left join MI_CareNE_ACOI.dbo.A_tmp_PAC_IP as d
		on a.RowN != d.RowN
		and a.member_key = d.member_key
		and a.to_date <= d.from_date
		and DATEADD(day,30,a.to_date) > d.from_date
		and d.typ = 'IP'
	left join ref_ACOI.dbo.ref_BPCI_DRG_Excl_Table as z
		on a.[CLIENT_MS_DRG_CODE] = z.anch_drg
		and d.[CLIENT_MS_DRG_CODE] = z.readm_drg
where
	a.idx = 1
group by
	a.[MEMBER_KEY]
	,a.rowN
	,a.INCURRED_YEAR_AND_MONTH
	,a.from_date
	,a.to_date
	,a.[CLIENT_MS_DRG_CODE]
	,c.[DRG_Desc]
	,a.ATT_PROV_ID
	,a.typ
	,a.admits
	,a.days
	,a.paid
	,c.[DRG Family]
	,c.[DRG Family List]
	,y.[freq_wm_readm]
	,y.[freq_wm_IRF]
	,y.[freq_wm_SNF]
	,y.[freq_wm_HHA]
	,y.[alos_wm_readm]
	,y.[alos_wm_irf]
	,y.[alos_wm_snf]
	,y.[freq_lm_readm]
	,y.[freq_lm_IRF]
	,y.[freq_lm_SNF]
	,y.[freq_lm_HHA]
	,y.[alos_lm_readm]
	,y.[alos_lm_irf]
	,y.[alos_lm_snf]
GO


IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_tmp_PAC2', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_tmp_PAC2;

SELECT
	a.*
	,max(case when b.typ is not null then a.admits else 0 end) as freq_irf
	,sum(b.admits) as case_irf
	,sum(b.days) as days_irf
	,sum(b.paid) as paid_irf
into
	MI_CareNE_ACOI.dbo.A_tmp_PAC2
from
	MI_CareNE_ACOI.dbo.A_tmp_PAC1 as a
	left join MI_CareNE_ACOI.dbo.A_tmp_PAC_IP as b
		on a.member_key = b.member_key
		and a.to_date <= b.from_date
		and DATEADD(day,30,a.to_date) > b.from_date
		and b.typ = 'IRF'
group by
	a.[MEMBER_KEY]
	,a.rowN
	,a.INCURRED_YEAR_AND_MONTH
	,a.from_date
	,a.to_date
	,a.[CLIENT_MS_DRG_CODE]
	,a.[DRG_Desc]
	,a.ATT_PROV_ID
	,a.typ
	,a.admits
	,a.days
	,a.paid
	,a.[DRG Family]
	,a.[DRG Family List]
	,a.[freq_wm_readm]
	,a.[freq_wm_IRF]
	,a.[freq_wm_SNF]
	,a.[freq_wm_HHA]
	,a.[alos_wm_readm]
	,a.[alos_wm_irf]
	,a.[alos_wm_snf]
	,a.[freq_lm_readm]
	,a.[freq_lm_IRF]
	,a.[freq_lm_SNF]
	,a.[freq_lm_HHA]
	,a.[alos_lm_readm]
	,a.[alos_lm_irf]
	,a.[alos_lm_snf]
	,freq_readm
	,case_readm
	,days_readm
	,paid_readm
GO


IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_tmp_PAC3', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_tmp_PAC3;

SELECT
	a.*
	,max(case when b.typ is not null then a.admits else 0 end) as freq_snf
	,sum(b.admits) as case_snf
	,sum(b.days) as days_snf
	,sum(b.paid) as paid_snf
into
	MI_CareNE_ACOI.dbo.A_tmp_PAC3
from
	MI_CareNE_ACOI.dbo.A_tmp_PAC2 as a
	left join MI_CareNE_ACOI.dbo.A_tmp_PAC_SNF as b
		on a.member_key = b.member_key
		and a.to_date <= b.from_date
		and DATEADD(day,30,a.to_date) > b.from_date
group by
	a.[MEMBER_KEY]
	,a.rowN
	,a.INCURRED_YEAR_AND_MONTH
	,a.from_date
	,a.to_date
	,a.[CLIENT_MS_DRG_CODE]
	,a.[DRG_Desc]
	,a.ATT_PROV_ID
	,a.typ
	,a.admits
	,a.days
	,a.paid
	,a.[DRG Family]
	,a.[DRG Family List]
	,a.[freq_wm_readm]
	,a.[freq_wm_IRF]
	,a.[freq_wm_SNF]
	,a.[freq_wm_HHA]
	,a.[alos_wm_readm]
	,a.[alos_wm_irf]
	,a.[alos_wm_snf]
	,a.[freq_lm_readm]
	,a.[freq_lm_IRF]
	,a.[freq_lm_SNF]
	,a.[freq_lm_HHA]
	,a.[alos_lm_readm]
	,a.[alos_lm_irf]
	,a.[alos_lm_snf]
	,freq_readm
	,case_readm
	,days_readm
	,paid_readm
	,freq_irf
	,case_irf
	,days_irf
	,paid_irf
GO



IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_tmp_PAC4', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_tmp_PAC4;

SELECT
	a.*
	,max(case when b.typ is not null then a.admits else 0 end) as freq_hha
	,sum(b.admits) as case_hha
	,sum(b.paid) as paid_hha
into
	MI_CareNE_ACOI.dbo.A_tmp_PAC4
from
	MI_CareNE_ACOI.dbo.A_tmp_PAC3 as a
	left join MI_CareNE_ACOI.dbo.A_tmp_PAC_HH as b
		on a.member_key = b.member_key
		and a.to_date <= b.from_date
		and DATEADD(day,30,a.to_date) > b.from_date
group by
	a.[MEMBER_KEY]
	,a.rowN
	,a.INCURRED_YEAR_AND_MONTH
	,a.from_date
	,a.to_date
	,a.[CLIENT_MS_DRG_CODE]
	,a.[DRG_Desc]
	,a.ATT_PROV_ID
	,a.typ
	,a.admits
	,a.days
	,a.paid
	,a.[DRG Family]
	,a.[DRG Family List]
	,a.[freq_wm_readm]
	,a.[freq_wm_IRF]
	,a.[freq_wm_SNF]
	,a.[freq_wm_HHA]
	,a.[alos_wm_readm]
	,a.[alos_wm_irf]
	,a.[alos_wm_snf]
	,a.[freq_lm_readm]
	,a.[freq_lm_IRF]
	,a.[freq_lm_SNF]
	,a.[freq_lm_HHA]
	,a.[alos_lm_readm]
	,a.[alos_lm_irf]
	,a.[alos_lm_snf]
	,freq_readm
	,case_readm
	,days_readm
	,paid_readm
	,freq_irf
	,case_irf
	,days_irf
	,paid_irf
	,freq_snf
	,case_snf
	,days_snf
	,paid_snf
GO



IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_tmp_PAC_sum1', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_tmp_PAC_sum1;

SELECT
	a.INCURRED_YEAR_AND_MONTH
	,ELIG_STATUS
	,ASSN_STATUS
	,PCP_Name ,area
	,CLIENT_MS_DRG_CODE as DRG
	,[DRG_Desc]
	,[DRG Family]
	,[DRG Family List]
	,sum(admits) as anchor_admits
	,sum(days) as anchor_days
	,sum(paid) as anchor_paid
	,sum(freq_wm_readm) as freq_wm_readm
	,sum(freq_wm_IRF) as freq_wm_IRF
	,sum(freq_wm_SNF) as freq_wm_SNF
	,sum(freq_wm_HHA) as freq_wm_HHA
	,sum(alos_wm_readm) as alos_wm_readm
	,sum(alos_wm_irf) as alos_wm_irf
	,sum(alos_wm_snf) as alos_wm_snf
	,sum(freq_lm_readm) as freq_lm_readm
	,sum(freq_lm_IRF) as freq_lm_IRF
	,sum(freq_lm_SNF) as freq_lm_SNF
	,sum(freq_lm_HHA) as freq_lm_HHA
	,sum(alos_lm_readm) as alos_lm_readm
	,sum(alos_lm_irf) as alos_lm_irf
	,sum(alos_lm_snf) as alos_lm_snf
	,sum(freq_readm) as freq_readm
	,sum(case_readm) as case_readm
	,sum(days_readm) as days_readm
	,sum(paid_readm) as paid_readm
	,sum(freq_irf) as freq_irf
	,sum(case_irf) as case_irf
	,sum(days_irf) as days_irf
	,sum(paid_irf) as paid_irf
	,sum(freq_snf) as freq_snf
	,sum(case_snf) as case_snf
	,sum(days_snf) as days_snf
	,sum(paid_snf) as paid_snf
	,sum(freq_hha) as freq_hha
	,sum(case_hha) as case_hha
	,null as days_hha
	,sum(paid_hha) as paid_hha
into
	MI_CareNE_ACOI.dbo.A_tmp_PAC_sum1
from
	MI_CareNE_ACOI.dbo.A_tmp_PAC4 as a
	right join MI_CareNE_ACOI.dbo.A_Enrollment_Key as b
		on b.[MEMBER_KEY] = a.[MEMBER_KEY]
		and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
group by
	a.INCURRED_YEAR_AND_MONTH
	,ELIG_STATUS
	,ASSN_STATUS
	,PCP_Name ,area
	,CLIENT_MS_DRG_CODE
	,[DRG_Desc]
	,[DRG Family]
	,[DRG Family List]
GO



IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_tmp_PAC_sum1_split', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_tmp_PAC_sum1_split;

SELECT
	a.*
INTO
	MI_CareNE_ACOI.dbo.A_tmp_PAC_sum1_split
FROM
	(
	SELECT
		INCURRED_YEAR_AND_MONTH, ELIG_STATUS, ASSN_STATUS,PCP_Name ,area, DRG, [DRG_Desc], [DRG Family], [DRG Family List]
		,anchor_admits, anchor_days, anchor_paid
		,'Exp' as cat1
		,'Inpatient Readmissions' as cat2
		,[freq_readm] as freq
		,[days_readm] as days
		,[paid_readm] as paid
		,[freq_lm_readm] as freq_lm
		,[alos_lm_readm] as days_lm
		,[freq_wm_readm] as freq_wm
		,[alos_wm_readm] as days_wm
	FROM
		MI_CareNE_ACOI.dbo.A_tmp_PAC_sum1

	UNION ALL

	SELECT
		INCURRED_YEAR_AND_MONTH, ELIG_STATUS, ASSN_STATUS,PCP_Name ,area, DRG, [DRG_Desc], [DRG Family], [DRG Family List]
		,anchor_admits, anchor_days, anchor_paid
		,'Exp' as cat1
		,'Acute Inpatient Rehab' as cat2
		,[freq_irf] as freq
		,[days_irf] as days
		,[paid_irf] as paid
		,[freq_lm_irf] as freq_lm
		,[alos_lm_irf] as days_lm
		,[freq_wm_irf] as freq_wm
		,[alos_wm_irf] as days_wm
	FROM
		MI_CareNE_ACOI.dbo.A_tmp_PAC_sum1

	UNION ALL

	SELECT
		INCURRED_YEAR_AND_MONTH, ELIG_STATUS, ASSN_STATUS, PCP_Name, area, DRG, [DRG_Desc], [DRG Family], [DRG Family List]
		,anchor_admits, anchor_days, anchor_paid
		,'Exp' as cat1
		,'Skilled Nursing Facility' as cat2
		,[freq_snf] as freq
		,[days_snf] as days
		,[paid_snf] as paid
		,[freq_lm_snf] as freq_lm
		,[alos_lm_snf] as days_lm
		,[freq_wm_snf] as freq_wm
		,[alos_wm_snf] as days_wm
	FROM
		MI_CareNE_ACOI.dbo.A_tmp_PAC_sum1

	UNION ALL

	SELECT
		INCURRED_YEAR_AND_MONTH, ELIG_STATUS, ASSN_STATUS,PCP_Name ,area, DRG, [DRG_Desc], [DRG Family], [DRG Family List]
		,anchor_admits, anchor_days, anchor_paid
		,'Exp' as cat1
		,'Home Health Care' as cat2
		,[freq_hha] as freq
		,null as days
		,[paid_hha] as paid
		,[freq_lm_hha] as freq_lm
		,null as days_lm
		,[freq_wm_hha] as freq_wm
		,null as days_wm
	FROM
		MI_CareNE_ACOI.dbo.A_tmp_PAC_sum1
	) as a
GO



IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_PAC_1', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_PAC_1;

SELECT
	b.*
	,DRG, [DRG_Desc], [DRG Family], [DRG Family List]
	,anchor_admits, anchor_days, anchor_paid
	,cat1
	,cat2
	,freq
	,days
	,paid
	,freq_lm
	,days_lm
	,freq_wm
	,days_wm
into
	MI_CareNE_ACOI.dbo.A_PAC_1
from
	MI_CareNE_ACOI.dbo.A_tmp_PAC_sum1_split as a
	right join (
		select
			INCURRED_YEAR_AND_MONTH
			,[MEMBER_MONTH_START_DATE]
			,[ELIG_STATUS]
			,[ASSN_STATUS]
			,PCP_Name ,area
			,[MM] as total_pop_mm
			,sum(paid) as total_pop_paid
		from
			MI_CareNE_ACOI.dbo.A_Claim_Summary1
		group by
			INCURRED_YEAR_AND_MONTH
			,[MEMBER_MONTH_START_DATE]
			,[ELIG_STATUS]
			,[ASSN_STATUS]
			,PCP_Name ,area
			,[MM]
			) as b
		on b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
		and b.[ELIG_STATUS] = a.[ELIG_STATUS]
		and b.[ASSN_STATUS] = a.[ASSN_STATUS]	
		and b.[PCP_Name] = a.[PCP_Name]
		and b.[area] = a.[area]	
GO




IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_tmp_PAC_DRG1', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_tmp_PAC_DRG1;

SELECT
	a.INCURRED_YEAR_AND_MONTH
	,b.ELIG_STATUS
	,b.ASSN_STATUS
	,b.PCP_Name ,b.area
	,a.[CLIENT_MS_DRG_CODE] as anchor_DRG
	,c.[DRG_DESC] as anchor_DRG_desc
	,c.[DRG Family]
	,c.[DRG Family List]
	,d.[CLIENT_MS_DRG_CODE] as readm_DRG
	,x.[DRG_DESC] as readm_DRG_desc
	,sum(d.admits) as readm_admits
	,sum(d.days) as readm_days
	,sum(d.paid) as readm_paid
into
	MI_CareNE_ACOI.dbo.A_tmp_PAC_DRG1
from
	MI_CareNE_ACOI.dbo.A_tmp_PAC_IP as a
	right join MI_CareNE_ACOI.dbo.A_Enrollment_Key as b
		on b.[MEMBER_KEY] = a.[MEMBER_KEY]
		and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
	left join ref_ACOI.dbo.ref_DRG_Family_Map as c
		on a.[CLIENT_MS_DRG_CODE] = REPLACE(STR(c.DRG,3),' ','0')
	left join MI_CareNE_ACOI.dbo.A_tmp_PAC_IP as d
		on a.rowN != d.rowN
		and a.member_key = d.member_key
		and a.to_date <= d.from_date
		and DATEADD(day,30,a.to_date) > d.from_date
		and d.typ = 'IP'
	left join ref_ACOI.dbo.ref_BPCI_DRG_Excl_Table as z
		on a.[CLIENT_MS_DRG_CODE] = z.anch_drg
		and d.[CLIENT_MS_DRG_CODE] = z.readm_drg
	left join ref_ACOI.dbo.ref_DRG_Family_Map as x
		on d.[CLIENT_MS_DRG_CODE] = REPLACE(STR(x.DRG,3),' ','0')
where
	b.MM =1
group by
	a.INCURRED_YEAR_AND_MONTH
	,b.ELIG_STATUS
	,b.ASSN_STATUS
	,b.PCP_Name ,b.area
	,a.[CLIENT_MS_DRG_CODE]
	,c.[DRG_DESC]
	,c.[DRG Family]
	,c.[DRG Family List]
	,d.[CLIENT_MS_DRG_CODE]
	,x.[DRG_DESC]
GO


IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_tmp_PAC_DRG2', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_tmp_PAC_DRG2;

SELECT
	a.INCURRED_YEAR_AND_MONTH
	,b.ELIG_STATUS
	,b.ASSN_STATUS
	,b.PCP_Name ,b.area
	,a.[CLIENT_MS_DRG_CODE] as anchor_DRG
	,sum(a.admits) as anchor_admits
	,sum(a.days) as anchor_days
	,sum(a.paid) as anchor_paid
into
	MI_CareNE_ACOI.dbo.A_tmp_PAC_DRG2
from
	MI_CareNE_ACOI.dbo.A_tmp_PAC_IP as a
	right join MI_CareNE_ACOI.dbo.A_Enrollment_Key as b
		on b.[MEMBER_KEY] = a.[MEMBER_KEY]
		and b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
where
	b.MM =1
group by
	a.INCURRED_YEAR_AND_MONTH
	,b.ELIG_STATUS
	,b.ASSN_STATUS
	,b.PCP_Name ,b.area
	,a.[CLIENT_MS_DRG_CODE]
GO


IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_tmp_PAC_DRG', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_tmp_PAC_DRG;

SELECT
	a.INCURRED_YEAR_AND_MONTH
	,a.ELIG_STATUS
	,a.ASSN_STATUS
	,a.PCP_Name ,a.area
	,a.anchor_DRG
	,a.anchor_DRG_desc
	,a.[DRG Family]
	,a.[DRG Family List]
	,a.readm_DRG
	,a.readm_DRG_desc
	,b.anchor_admits
	,b.anchor_days
	,b.anchor_paid
	,a.readm_admits
	,a.readm_days
	,a.readm_paid
into
	MI_CareNE_ACOI.dbo.A_tmp_PAC_DRG
from
	MI_CareNE_ACOI.dbo.A_tmp_PAC_DRG1 as a
	left join MI_CareNE_ACOI.dbo.A_tmp_PAC_DRG2 as b
		on a.INCURRED_YEAR_AND_MONTH = b.INCURRED_YEAR_AND_MONTH
		and a.ELIG_STATUS = b.ELIG_STATUS 
		and a.ASSN_STATUS = b.ASSN_STATUS
		and a.PCP_Name = b.PCP_Name
		and a.area = b.area
		and a.anchor_DRG =  b.anchor_DRG
GO



IF OBJECT_ID('MI_CareNE_ACOI.dbo.A_PAC_2', 'U') IS NOT NULL
DROP TABLE MI_CareNE_ACOI.dbo.A_PAC_2;

SELECT
	b.*
	,anchor_DRG
	,anchor_DRG_desc
	,[DRG Family]
	,[DRG Family List]
	,readm_DRG
	,readm_DRG_desc
	,anchor_admits
	,anchor_days
	,anchor_paid
	,readm_admits
	,readm_days
	,readm_paid
into
	MI_CareNE_ACOI.dbo.A_PAC_2
from
	MI_CareNE_ACOI.dbo.A_tmp_PAC_DRG as a
	right join (
			select
				INCURRED_YEAR_AND_MONTH
				,[MEMBER_MONTH_START_DATE]
				,[ELIG_STATUS]
				,[ASSN_STATUS]
				,PCP_Name ,area
				,[MM] as total_pop_mm
				,sum(paid) as total_pop_paid
			from
				MI_CareNE_ACOI.dbo.A_Claim_Summary1
			group by
				INCURRED_YEAR_AND_MONTH
				,[MEMBER_MONTH_START_DATE]
				,[ELIG_STATUS]
				,[ASSN_STATUS]
				,PCP_Name ,area
				,[MM]
				) as b
			on b.[INCURRED_YEAR_AND_MONTH] = a.[INCURRED_YEAR_AND_MONTH]
			and b.[ELIG_STATUS] = a.[ELIG_STATUS]
			and b.[ASSN_STATUS] = a.[ASSN_STATUS]	
			and b.[PCP_Name] = a.[PCP_Name]
			and b.[area] = a.[area]	
where anchor_DRG is not null	
GO




/*

(14890 row(s) affected)

(3750 row(s) affected)

(12781 row(s) affected)

(14111 row(s) affected)

(1 row(s) affected)

...

...

(0 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(12349 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(12349 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(12349 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(12349 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(15995 row(s) affected)

(63980 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(64076 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(20833 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(18548 row(s) affected)

(20833 row(s) affected)
Warning: Null value is eliminated by an aggregate or other SET operation.

(20821 row(s) affected)

7 min

*/