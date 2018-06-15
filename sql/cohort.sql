with cst as
(
	select
		  *
		, cast(unix_timestamp(hospitaladmissiondate, "dd/MM/yyyy") as timestamp) as hospital_adm
		, cast(unix_timestamp(unitadmissiondate, "dd/MM/yyyy") as timestamp) as unit_adm
	from admission
	where isreadmission = false 
	and ishospitalreadmission = false 
)
, rkd as
(
	select 
		  *
		, RANK() over (partition by medicalrecord order by hospital_adm, unit_admissiontime ASC) as hosp_seq
		, RANK() over (partition by medicalrecord order by unit_adm, unit_admissiontime ASC) as unit_seq
	from cst
) 
, f_adm as (
	select 
		*
	from rkd 
	where unit_seq = 1
	and hosp_seq = 1
) 
, age as 
(
	select
		*
	from f_adm
	where age > 17
) 
, cohort as
(
	select 
		*
	from age
	where hospitaldischargecode is not null
	and hospitaldischargecode != ''
) 
select
	  a.hospitalname
	, a.medicalrecord 
	, a.admissionrecordid
	, a.age
	, a.gender
	, case 
		when gender = 'F' then 0  
		when gender = 'M' then 1 
		else null 
	  end as genders
	, a.weight
	, a.height
	, a.bmi
	, case
		when cast(bmi as float) < 18.5 then  'under'
		when cast(bmi as float) >= 18.5 and cast(bmi as float) < 25 then 'normal'
		when cast(bmi as float) >= 25 and cast(bmi as float) < 30 then 'over'
		when cast(bmi as float) >= 30 then 'obese'
		else null 
	  end as bmi_group
	, a.ishospitalreadmission
	, a.lengthhospitalstaypriorunitadmission
	, a.unitadmissiondate
	, a.isreadmission
	, a.admissionsourcename
	, a.admissionsourcecode
	, a.admissionreasonname
	, a.admissionreasoncode
	, a.admissiontypename
	, a.admissiontypecode
	, a.admissionmaindiagnosisname
	, a.admissionmaindiagnosiscode
	, case 
		when unitdischargecode = 'A' then 0
		when unitdischargecode = 'D' then 1
		else null 
	  end as unitdischargecode
	, a.unitlengthstay
	, a.hospitallengthstay
	, b.ismechanicalventilation
	, b.isnoninvasiveventilation
	, b.isvasopressors
	, b.isrenalreplacementtherapy  
	, c.lowest_systolic_blood_pressure1h
	, c.lowest_diastolic_blood_pressure1h
	, c.lowest_mean_arterial_pressure1h
	, c.highest_heart_rate1h
	, c.highest_respiratory_rate1h
	, c.highest_temperature1h
	, c.lowest_glasgow_coma_scale1h
	, c.highest_leukocyte_count1h
	, c.lowest_platelets_count1h
	, c.highest_creatinine1h
	, c.highest_bilirubin1h
	, c.highest_ph1h
	, c.highest_pa_o21h
	, c.highest_pa_co21h 
	, c.highest_fi_o21h
	, c.highest_pa_o2fi_o21h
	, c.urea
	, e.isneurologicalcomastuporobtundeddelirium
	, e.isneurologicalseizures 
	, e.isneurologicalfocalneurologicdeficit 
	, e.isneurologicalintracranialmasseffect 
	, e.iscardiovascularhypovolemichemorrhagicshock 
	, e.iscardiovascularsepticshock 
	, e.iscardiovascularrhythmdisturbances 
	, e.iscardiovascularanaphylacticmixedundefinedshock 
	, e.isdigestiveacuteabdomen 
	, e.isdigestiveseverepancreatitis 
	, e.isliverfailure 
	, e.istransplantsolidorgan 
	, e.istraumamultipletrauma 
	, e.iscardiacsurgery 
	, e.isneurosurgery 
	, d.saps3points
	, (d.saps3deathprobabilitystandardequation / 100) as saps_prob
	, d.charlsoncomorbidityindex 
	, f.chronic_health_status_name
	, f.ischfnyhaclass4
	, f.ishepaticfailure
	, f.iscirrhosischildab, f.iscirrhosischildc
	, f.issolidtumorlocoregional
	, f.issolidtumormetastatic
	, f.ishematologicalmalignancy
	, f.isimmunossupression
	, f.issteroidsuse
	, f.istobaccoconsumption
	, f.isalcoholism
	, case 
		when a.hospitaldischargecode = 'A' then 0
		when a.hospitaldischargecode = 'D' then 1
		else null 
	  end as hospitalexpireflag
from cohort a
left join acutecomplication b
	on a.admissionrecordid = b.admissionrecordid
left join physiologicallaboratory1h c
	on a.admissionrecordid = c.admissionrecordid
left join score d 
	on a.admissionrecordid = d.admissionrecordid
left join saps3admission e 
	on a.admissionrecordid = e.admissionrecordid
left join comorbidity f 
	on a.admissionrecordid = f.admissionrecordid