/* OLD OLD OLD OLD */
/*
select A.*, coalesce(B.[Drug Mapping], 'Not Mapped') [Mapping], coalesce(B.[Drug], 'Not Mapped') [Mapping Type]
into mi_ATM881_ACOI.dbo.A_Svc_Cat_Detail_Mapped
FROM mi_ATM881_ACOI.dbo.A_Svc_Cat_Detail A left join 
(
select [drug mapping], [code], 'Drug' [Drug] from drug_mapping
union 
select [dme mapping], [code], 'DME' from dme_mapping
union
select [surgery name], [hcpcs code], 'Surgery' from surgery_mapping
) B
on CASE WHEN A.CODE_TYPE = 'Revenue Code' THEN left(PROC_CODE_AND_DESC, 4)
		ELSE left(proc_code_and_desc, 5) END = B.[Code]

select A.*, coalesce(B.[Drug Mapping], 'Not Mapped') [Mapping], coalesce(B.[Drug], 'Not Mapped') [Mapping Type]
into mi_ATM881_ACOI.dbo.A_tmp_SvcCatDet1_Mapped
FROM mi_ATM881_ACOI.dbo.A_tmp_SvcCatDet1 A left join 
(
select [drug mapping], [code], 'Drug' [Drug] from drug_mapping
union 
select [dme mapping], [code], 'DME' from dme_mapping
union
select [surgery name], [hcpcs code], 'Surgery' from surgery_mapping
) B
on CASE WHEN A.CODE_TYPE = 'Revenue Code' THEN left(PROC_CODE_AND_DESC, 4)
		ELSE left(proc_code_and_desc, 5) END = B.[Code]
*/

select A.*,
	  CASE WHEN HCG_MR_LINE = 'P84' THEN
		   CASE WHEN B.[Drug Mapping] is NULL THEN 'Other'
				ELSE B.[Drug Mapping] END
		   WHEN HCG_MR_LINE in ('O16a', 'O16b', 'P34a', 'P34b') THEN
		   CASE WHEN B.[Drug Mapping] is NULL THEN 'Other'
				ELSE B.[Drug Mapping] END
		   ELSE coalesce(B.[Drug Mapping], 'Not Mapped') END [Mapping],

	  CASE WHEN HCG_MR_LINE = 'P84' THEN
		   CASE WHEN B.[Drug] is NULL THEN 'DME'
				ELSE B.[Drug] END
		   WHEN HCG_MR_LINE in ('O16a', 'O16b', 'P34a', 'P34b') THEN
		   CASE WHEN B.[Drug] is NULL THEN 'Drug'
				ELSE B.[Drug] END
		   ELSE coalesce(B.[Drug], 'Not Mapped') END [Mapping Type]

into mi_ATM881_ACOI.dbo.A_tmp_SvcCatDet1_Mapped_V4
FROM mi_ATM881_ACOI.dbo.A_tmp_SvcCatDet1 A left join 
(
select [drug mapping], [code], 'Drug' [Drug] from drug_mapping
union 
select [dme mapping], [code], 'DME' from dme_mapping
/*union
select [surgery name], [hcpcs code], 'Surgery' from surgery_mapping*/
) B
on CASE WHEN A.CODE_TYPE = 'Revenue Code' THEN left(PROC_CODE_AND_DESC, 4)
										  ELSE left(proc_code_and_desc, 5) END = B.[Code]


