USE [Operations]
GO
/****** Object:  StoredProcedure [dbo].[wftask-triggers]    Script Date: 4/11/2024 4:15:41 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


-- =============================================
-- Author:		Nick
-- Create date: 3/5/2024
-- Description:	the procedures for all the workflow task triggers that will go into the
--				[SQL\MyCompany].[Operations].dbo.WorkflowTaskLog Table. 

--NOTES: Completed status should mean that the workflow task sent was completed by end user. 


*/
-- =============================================
ALTER PROCEDURE [dbo].[wftask-triggers]

AS
BEGIN


------------------------ HOLD NOTES-------------------------
	DROP TABLE IF EXISTS #lognotes
    
	SELECT 
		logs.patient_number, 
		logs.admission_number, 
		logs.nurse_ID, 
		logs.user_code, 
		logs.Subject, 
		cast(logs.communication_date_time as date) as communication_date, 
		logs.notes, 
		logs.cb_Delete, 
		logs.communication_ctg_code,
		CONCAT(trim(emp.First_Name), ' ', trim(emp.Last_Name)) AS [Full_Name],
		cde.User_Name,
		Row_Number() OVER (PARTITION BY logs.Patient_Number /*, logs2.Communication_ctg_code*/ ORDER BY Communication_Date_time Desc) AS [Note No] --if they ever decide to go by comm category, can add back in the commented out section
	INTO #lognotes
	FROM [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].communicationlogs logs
	LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].mdnurdat emp on emp.[Nurse_ID] = logs.[Nurse_ID]
	LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].isusrcde cde on cde.[User_code] = logs.User_Code
	LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].signroleusers rle on rle.[User_Code] = logs.[User_Code]
	WHERE communication_ctg_code = 'HOL' and cb_delete = 0

	-------------INSERT STATEMENT------------

	INSERT INTO [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog] (task_code, due, patient_number, admission_number, notes, grp1_type1, grp1_assignee1, status)
	(
		SELECT DISTINCT
			'ADD HOLD NOTE' as WF_Task_Code, 
			(SELECT DATEADD(day, 7, CAST(GETDATE() AS date))) as due_date, 
			adm.patient_number, 
			adm.admission_number, 
			CASE WHEN notes.communication_date is null 
				THEN 'This client has been placed on hold. Please add a note to Patient Log with today''s date, the client admission number, and the communication category ''HOL'' with information on client status'
				END AS 
			WF_Task_notes,
			1, 
			'CaseManager', 
			'Pending'
		FROM [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].mdhomadm adm 
		LEFT JOIN [SQL\MyCompany].[Operations].[dbo].[holdReportVis] hol on hol.admission_number = adm.admission_number
		LEFT JOIN  #lognotes notes on notes.admission_number = adm.admission_number and notes.communication_date >= dateadd(day, -1, hol.hold_report_date) and notes.[Note No] = 1
		LEFT JOIN [SQL\MyCompany].[Operations].[dbo].[WorkflowTasklog] wft on wft.admission_number = adm.admission_number  and wft.task_code = 'ADD HOLD NOTE'
		WHERE adm.discharge_code like 'H%' and hol.hold_end_date is null and notes.communication_date is null 
			and (wft.due is null or wft.due < dateadd(day, -1, hol.hold_report_date) )
	)


-------------------------PAR County Change------------------
--Outstanding Questions: WHO TO SEND THIS TO? A role group? Med recs/PAR? OR Billing? And coordinator? In what order?

	DROP TABLE IF EXISTS #county;

		SELECT 
			pat.patient_number, 
			adm.admission_number, 
			pat.residency_codes, 
			cde.residency_desc, 
			CASE WHEN cde.residency_desc = 'CO-DENVER COUNTY               ' THEN 'DENVER' 
				WHEN cde.residency_desc is not null THEN 'NON-DENVER' 
			END AS county
		INTO #county
		FROM  [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].mdpatdat pat
		LEFT JOIN  [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].mdrescde cde on cde.residency_codes = pat.residency_codes
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].mdhomadm adm on adm.patient_number = pat.patient_number
		WHERE adm.discharge_code not like 'D%' and adm.discharge_code <> 'CL ' and residency_desc is not null
		GROUP BY pat.patient_number, adm.admission_number, pat.residency_codes, cde.residency_desc, case when cde.residency_desc = 'CO-DENVER COUNTY               ' then 'DENVER' when cde.residency_desc is not null then 'NON-DENVER' end

		INSERT INTO [SQL\MyCompany].[Operations].[dbo].[WorkflowTasklog] (
			task_code, due, patient_number, admission_number, notes, grp1_type1, grp1_assignee1, grp2_type1, grp2_assignee1, status
		)
		(
			SELECT
				'DENVER STATUS CHANGE' as WF_Task_Code, 
				(SELECT DATEADD(day, 7, CAST(GETDATE() AS date))) as due_date, 
				cont.patient_number, 
				cont.admission_number, 
				CASE WHEN res.county = 'DENVER' then 'Client county changed to a DENVER COUNTY RESIDENCE from a NON-DENVER COUNTY' 
					ELSE 'Client county changed to a NON-DENVER COUNTY RESIDENCY from a DENVER COUNTY' 
				END AS WF_Task_Note, 
				2, 
				'MRPAR',
				1, 
				'CaseManager', 
				'Pending'
			FROM #county cont 
			LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_Analytical_DB].[dbo].client_county res on res.patient_number = cont.patient_number
			LEFT JOIN [SQL\MyCompany].[Operations].[dbo].[WorkflowTasklog] wft on wft.patient_number = cont.patient_number and wft.admission_number = cont.admission_number
			WHERE cont.county <> res.county 
		)

	DELETE FROM [SQL\EHR1_Linked_Server].[MyCompany_EHR1_Analytical_DB].[dbo].client_county

	INSERT INTO [SQL\EHR1_Linked_Server].[MyCompany_EHR1_Analytical_DB].[dbo].client_county
	SELECT 
		patient_number, 
		residency_codes, 
		residency_desc, 
		county 
	FROM #county 
	WHERE residency_desc is not null
	GROUP BY patient_number, residency_codes, residency_desc, county


------------ LOST ELIGIBILITY-------------------------------
-- JUST UNSKILLED
-- ALERT AFTER LOSE ELIGIBILITY FROM LAST ELIGIBILITY RUN
--SEND TO COORDINATOR
	DROP TABLE IF EXISTS #elg;

	SELECT 
		cast(elg.entry_date as date) as entry_date, 
		elg.patient_number, 
		adm.admission_number, 
		adm.nurse_ID as coordinator,
		elg.plan_code, 
		adm.discharge_code as patient_status
	INTO #elg
	FROM [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].ELGInfoTable elg
	LEFT JOIN (SELECT patient_number, policy_number FROM  [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].mdinsdat dat WHERE dat.billto_status_code = 'MCD' GROUP BY patient_number, policy_number) dat on dat.patient_number = elg.patient_number
	LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].mdpatdat pat on pat.patient_number = elg.patient_number
	LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].mdhomadm adm on adm.patient_number = elg.patient_number and ( adm.primary_plan = elg.plan_code) 
	WHERE elg.entry_date = (SELECT max(entry_date) FROM [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].ELGInfoTable) 
		and adm.discharge_code not like 'D%' and DDL_eligibility_status = '6' and adm.facility_code not in ('LSK', 'CSS')


	INSERT INTO [SQL\MyCompany].[Operations].[dbo].[WorkflowTasklog] (
		task_code, due, patient_number, admission_number, notes, grp1_type1, grp1_assignee1, status
	)
	SELECT 
		'LOST ELIGIBILITY' as WF_Task_Code, 
		(SELECT DATEADD(day, 7, CAST(GETDATE() AS date))) as due_date,  
		elg.patient_number, 
		elg.admission_number,
		concat('Client has lost eligibility as of last eligibility run on ', elg.entry_date, '. Please verify with your manager to check if this client should be placed on hold.') as WF_Task_Notes,
		1, 
		'CaseManager', 
		'Pending'
	FROM #elg elg
	LEFT JOIN [SQL\MyCompany].[Operations].[dbo].[holdReportVis] hol on hol.admission_number = elg.admission_number
	LEFT JOIN [SQL\MyCompany].[Operations].[dbo].[WorkflowTasklog] wft on wft.admission_number = elg.admission_number and wft.task_code = 'LOST ELIGIBILITY'
	WHERE  wft.due is null or wft.due < elg.entry_date

-------------------------------UTILIZATION REPORT-----------------------------------------------------:
Overutilized threshold over > 100% every month (only unskilled)
Underutilized threshold under 80% every quarter (only unskilled) --not sure if they want that - it would be a bigggg note 
DO NOT SEND TO SKILLED COORDINATOR.... Only Unskilled

	
	
--OVER UTILIZATION REPORT
	IF DAY(GETDATE()) = 15
	BEGIN
		
		DROP TABLE IF EXISTS  #util, #overut1, #overut;
		
		SELECT 
			system, 
			systemID, 
			clientID, 
			service, 
			programgroup, 
			themonth, 
			measure_type, 
			sum(numerator) as billed_hrs, 
			sum(denominator) as auth_hrs, 
			sum(numerator)/sum(denominator) as utilization
		into #util
		FROM [SQL\MyCompany].[Operations].[dbo].[formattedutilization] 
		WHERE system = 'EHR1' 
			and active_status = 1 and frq_entered = 1 and no_outliers = 1 and no_duplicate_systems = 1 
			and themonth between left(cast(dateadd(day, -90, getdate()) as date), 7) and  left(cast(dateadd(day, -30, getdate()) as date), 7)
		GROUP BY system, systemID, clientID, service, programgroup, themonth, measure_type
		

		SELECT 
			trim(pat.last_name) as client_last_name, 
			util.*, 
			adm.*
		INTO #overut1
		FROM #util util
		LEFT JOIN (
			SELECT adm.patient_number, adm.facility_code, adm.admission_number, adm.nurse_ID, 
				concat(trim(nur.last_name), ', ', trim(nur.first_name)) as coordinator, adm.primary_plan, 
				count(*) over (partition by adm.patient_number) as count_num 
			FROM [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdhomadm] adm
			LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdnurdat] nur on nur.nurse_ID = adm.nurse_ID
		) adm on adm.patient_number = util.systemID and adm.facility_code not in ('LSK', 'CSS')
		LEFT JOIN  [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdpatdat] pat on pat.patient_number = util.systemID
		WHERE themonth = (select max(themonth) from #util ) and utilization > 1.05
			and programgroup not like '%CNA%' and programgroup not like '%NURSING%' and programgroup not like '%PDN%'


		SELECT 
			nurse_ID,  
			concat('Overutilization Report for Previous Month: ', 
			string_agg(agg, ' --- ')) as notes 
		INTO #overut 
		FROM (
			SELECT patient_number, admission_number, nurse_ID, 
				concat('patient number ', trim(patient_number), ' ', client_last_name, ', ', string_agg(concat(programgroup, ': ', 'billed hours: ', cast(billed_hrs as dec(7,2)), ', auth hours: ', cast(auth_hrs as dec(7,2)), ', overutilized by ' , cast(utilization*100.0 - 100.0 as dec(7,2)), '%'), '; ')) as agg
			FROM #overut1 util
			GROUP BY util.patient_number, util.client_last_name, util.admission_number, nurse_ID, coordinator
		) util GROUP BY nurse_ID


		INSERT INTO [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog] (
			task_code, due, grp1_type1, grp1_assignee1, grp1_type2, grp1_assignee2, notes, status
		)
		SELECT DISTINCT top 1  
			'UTIL REPORT' as WF_TASK_CODE, 
			dateadd(week,2,cast(getdate() as date)) as due_date, 
			3, 
			'[REDACTED]', --cde.user_code, 
			3, 
			'[REDACTED]', --cde.user_code, 
			util.notes, 
			'Pending' as [status]
		FROM #overut util
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].mdnurdat nur on nur.nurse_ID = util.nurse_ID
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].isusrcde cde on trim(cde.USER_Name) = concat(trim(nur.first_NAME), ' ', trim(nur.last_name)) 
		LEFT JOIN [SQL\MyCompany].[Operations].[dbo].[WorkflowTasklog] wft on wft.grp1_assignee1 = cde.user_code and wft.Task_Code = 'UTIL REPORT' 
		WHERE (wft.due is null or left(wft.due, 7) < left(cast(getdate() as date), 7))

	END
	
------UNDER UTILIZATION REPORT (once a quarter unskilled)
	IF DAY(GETDATE()) = 15 AND MONTH(getdate()) in (4,7,10,1)
	BEGIN
	
		DROP TABLE IF EXISTS  #unutil, #underut, #underut1;
    
		SELECT 
			system, 
			systemID, 
			clientID, 
			service, 
			programgroup, 
			--themonth, 
			measure_type, 
			sum(numerator) as billed_hrs, 
			sum(denominator) as auth_hrs, 
			sum(numerator)/sum(denominator) as utilization
		into #unutil
		FROM [SQL\MyCompany].[Operations].[dbo].[formattedutilization] 
		WHERE system = 'EHR1' 
			and active_status = 1 and frq_entered = 1 and no_outliers = 1 and no_duplicate_systems = 1 
			and themonth between left(cast(dateadd(day, -90, getdate()) as date), 7) and  left(cast(dateadd(day, -30, getdate()) as date), 7)
		GROUP BY system, systemID, clientID, service, programgroup, measure_type
    

		SELECT 
			trim(pat.last_name) as client_last_name, 
			util.*, 
			adm.*
		INTO #underut1
		FROM #unutil util
		LEFT JOIN (
			SELECT adm.patient_number, adm.facility_code, adm.admission_number, adm.nurse_ID, concat(trim(nur.last_name), ', ', trim(nur.first_name)) as coordinator, adm.primary_plan, count(*) over (partition by adm.patient_number) as count_num 
			FROM [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdhomadm] adm
			LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdnurdat] nur on nur.nurse_ID = adm.nurse_ID
		) adm on adm.patient_number = util.systemID and adm.facility_code not in ('LSK', 'CSS')
		LEFT JOIN  [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdpatdat] pat on pat.patient_number = util.systemID
		WHERE utilization < .5 and programgroup not like '%CNA%' and programgroup not like '%NURSING%' 
			and programgroup not like '%PDN%'



		SELECT 
			nurse_ID,  
			concat('Underutilization Report for Previous Quarter: ', string_agg(agg, ' --- ')) as notes 
		INTO #underut 
		FROM (
			SELECT patient_number, admission_number, nurse_ID, 
			concat('patient number ', trim(patient_number), ' ', client_last_name, ', ', string_agg(concat(programgroup, ': ', 'billed hours: ', cast(billed_hrs as dec(7,2)), ', auth hours: ', cast(auth_hrs as dec(7,2)), ', underutilized by ' , cast(utilization*100.0 - 100.0 as dec(7,2)), '%'), '; ')) as agg
			FROM #underut1 util
			GROUP BY util.patient_number, util.client_last_name, util.admission_number, nurse_ID, coordinator
		) util GROUP BY nurse_ID


		INSERT INTO [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog] (    -- select top 100 * from [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog] where task_code like 'UTIL%' order by due desc
			task_code, due, grp1_type1, grp1_assignee1, grp1_type2,	grp1_assignee2, notes, status
		)
		SELECT DISTINCT
			'UTIL REPORT' as WF_TASK_CODE, 
			dateadd(week,2,cast(getdate() as date)) as due_date, 
			3, 
			'[REDACTED]', --cde.user_code, adelia is AH2
			3, 
			'[REDACTED]', --cde.user_code, JDY is jessica dottenwhy-- select top 4000 * from [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].isusrcde
			util.notes, 
			'Pending'
		FROM #underut util
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].mdnurdat nur on nur.nurse_ID = util.nurse_ID
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].isusrcde cde on trim(cde.USER_Name) = concat(trim(nur.first_NAME), ' ', trim(nur.last_name)) 
		LEFT JOIN [SQL\MyCompany].[Operations].[dbo].[WorkflowTasklog] wft on wft.grp1_assignee1 = cde.user_code and wft.Task_Code = 'UTIL REPORT' 
		WHERE (wft.due is null or left(wft.due, 7) < left(cast(getdate() as date), 7)) AND User_Code is not null -- this meant patient discharge_code was 'PA' or pending admission

	END


------------------------------------------OT ALERT ---------------------------------
--------SKILLED TO TRY IT OUT, UNSKILLED NOT TO-------------
	IF DATENAME(WEEKDAY, GETDATE()) = 'THURSDAY'
	BEGIN

		-- overtime for the week at 40 hours
		DROP TABLE IF EXISTS #ot, #ot2, #coordinator, #ot3;

		SET DATEFIRST 1;
		SELECT sch.nurse_ID,  
			DATEADD(DAY, 1 - DATEPART(WEEKDAY, CONVERT(datetime, CAST([schedule_date] AS date))), CAST(CONVERT(datetime, CAST([schedule_date] AS date)) AS DATE)) as weekStart, 
			sum(cast((left(trim(sch.confirmed_duration), 2)*60 + right(trim(sch.confirmed_duration), 2)) as int)) as confirmed_duration_min, 
			sum(cast((left(trim(sch.duration), 2)*60 + right(trim(sch.duration), 2)) as int)) as scheduled_duration_min  
		INTO #ot
		FROM  [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdhomsch] sch 
		WHERE cast(schedule_date as date) < cast(getdate() as date) and schedule_status_code in ('P', 'VP', 'V', 'BP', 'C' )
		GROUP BY sch.nurse_ID, DATEADD(DAY, 1 - DATEPART(WEEKDAY, CONVERT(datetime, CAST([schedule_date] AS date))), CAST(CONVERT(datetime, CAST([schedule_date] AS date)) AS DATE))

		SELECT  
			nurse_ID, 
			weekstart, 
			confirmed_duration_min/60.0 as confirmed_duration_hrs, 
			scheduled_duration_min/60.0 as scheduled_duration_hrs 
		INTO #ot2
		FROM #ot 
		WHERE (confirmed_duration_min/60.0 >= 40.0 and confirmed_duration_min/60.0 > scheduled_duration_min/60.0 
			and (confirmed_duration_min - scheduled_duration_min) > 15.0 )
		
		SET DATEFIRST 1;
		SELECT 
			nurse_ID, 
			admission_number, 
			DATEADD(DAY, 1 - DATEPART(WEEKDAY, CONVERT(datetime, CAST([schedule_date] AS date))), CAST(CONVERT(datetime, CAST([schedule_date] AS date)) AS DATE)) as weekStart 
		INTO #coordinator
		FROM  [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdhomsch] sch where cast(schedule_date as date) < cast(getdate() as date) 
		GROUP BY nurse_ID, admission_number, DATEADD(DAY, 1 - DATEPART(WEEKDAY, CONVERT(datetime, CAST([schedule_date] AS date))), CAST(CONVERT(datetime, CAST([schedule_date] AS date)) AS DATE)) 


		SELECT 
			concat(Trim(nur.first_name), ' ', trim(nur.last_name)) as emp_name, 
			nur.employee_profile_code, 
			ot.*, 
			(ot.confirmed_duration_hrs - ot.scheduled_duration_hrs)*60.0 as minutes_over, 
			wft2.count_num, 
			wft.reportdate, 
			adm.nurse_ID as coordinator, 
			cde.user_code
		INTO #ot3
		FROM #ot2 ot 
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_Analytical_DB].[dbo].[overtime_wftask] wft on wft.nurse_ID = ot.nurse_ID and wft.weekstart = ot.weekstart
		LEFT JOIN (
			SELECT wft.*, count(*) over (partition by nurse_ID) as count_num 
			FROM [SQL\EHR1_Linked_Server].[MyCompany_EHR1_Analytical_DB].[dbo].[overtime_wftask] wft 
			WHERE left(weekstart, 4) = left(cast(getdate() as date), 4)
		) wft2 on wft2.nurse_ID = ot.nurse_ID and wft2.weekstart = ot.weekstart
		LEFT JOIN #coordinator cor on cor.weekstart = ot.weekstart and cor.nurse_ID = ot.nurse_ID
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdhomadm] adm on adm.admission_number = cor.admission_number
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdnurdat] nur on nur.nurse_ID = ot.nurse_ID
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdnurdat] nur2 on nur2.nurse_ID = adm.nurse_ID
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].isusrcde cde on trim(cde.USER_Name) = concat(trim(nur2.first_NAME), ' ', trim(nur2.last_name))
		WHERE nur.employee_profile_code <> 'CCT    ' and adm.facility_code in ('LSK', 'CSS')
			and wft.reportdate between  dateadd(day, -10, getdate()) and dateadd(day, -4, getdate())
		GROUP BY ot.nurse_ID, ot.weekstart, ot.confirmed_duration_hrs, ot.scheduled_Duration_hrs, wft.reportdate, adm.nurse_ID, 
			concat(Trim(nur.first_name), ' ', trim(nur.last_name)), nur.employee_profile_code, wft2.count_num, cde.user_code


		INSERT INTO [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog] (
			task_code, due, grp1_type1, grp1_assignee1, notes, status
		)
		(
			SELECT 
				'OT Alert', 
				cast(getdate() as date), 
				3, 
				user_code, 
				concat('Overtime Notice for Following Employee(s): ', string_agg(cast(concat(emp_name, ' for the week of ', weekstart, '. Scheduled hours: ', cast(scheduled_duration_hrs as dec(7,2)), ' Confirmed hours: ', cast(confirmed_duration_hrs as dec(7,2)), ', ', cast(minutes_over as dec(7,2)), ' minutes over. This employee has exceeded scheduled hours this year for ' , count_num, ' week(s). ') as varchar(MAX)), ' -- ') ) as note,
				'Pending' as status
			FROM #ot3 ot
			GROUP BY user_code
		)


	END
--------------------Visit duration not divisible by 15 minutes------------

	
	IF DAY(GETDATE()) in ( 15, 1)
	BEGIN

		INSERT INTO [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog] (
			task_code, due, patient_number, admission_number, notes, grp1_type1, grp1_assignee1, status
		)
		SELECT 
			'NOT DIV BY 15' as task_code, 
			cast(getdate() as date) as due, 
			vis.patient_number, 
			vis.admission_number, 
			concat('There are visit(s) on these dates over the next two weeks that have a duration not divisible by 15 minutes: ',  string_agg(vis.sched, '; ') within group (order by vis.sched asc)) as notes, 
			1, 
			'CaseManager', 
			'Pending' 
		FROM (
			SELECT 
				concat(trim(pat.first_name), ' ', trim(pat.last_name)) as clientname, 
				sch.patient_number, 
				sch.admission_number, 
				adm.nurse_ID, 
				cast(sch.schedule_date as date) as sched
			FROM [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].dbo.mdhomsch sch 
			LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].dbo.mdhomadm adm on adm.admission_number = sch.admission_number 
			LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].dbo.mdpatdat pat on pat.patient_number = sch.patient_number
			WHERE cast(sch.schedule_date as date) between dateadd(day, 1, getdate()) and dateadd(week, 2, getdate()) 
				and right(trim(sch.duration), 2) not in ('00', '15', '30', '45') and adm.admission_number not like 'D%' and sch.patient_number not in ('[REDACTED]')
			GROUP BY sch.patient_number, sch.admission_number, cast(sch.schedule_date as date), adm.nurse_ID,  concat(trim(pat.first_name), ' ', trim(pat.last_name))
		) vis GROUP BY vis.patient_number, vis.admission_number, vis.nurse_ID, vis.clientname


	END
	
-----------------------------No Coordinator---------------------------------------
	INSERT INTO [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog] (
		task_code, due, patient_number, admission_number, grp1_type1, grp1_assignee1, status
	)
	SELECT 
		'ASSIGN COORDINATOR', 
		cast(getdate() as date) as due, 
		adm.patient_number, 
		adm.admission_number, 
		CASE WHEN adm.primary_plan not in ('SKILLT ', 'SKILAC ') then 2 else 3 end as grp1_type1, 
		CASE WHEN adm.primary_plan not in ('SKILLT ', 'SKILAC ') then 'UNSMGR' else '[REDACTED]' end as grp1_assignee1,
		'Pending'
	FROM  [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].dbo.mdhomadm adm 
	LEFT JOIN [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog] wft on wft.task_code = 'ASSIGN COORDINATOR'and wft.admission_number = adm.admission_number
	LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].mdnurdat nur on nur.nurse_ID = adm.nurse_ID
	LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].isusrcde cde on trim(cde.USER_Name) = concat(trim(nur.first_NAME), ' ', trim(nur.last_name))
	WHERE adm.discharge_code not like 'D%' and adm.discharge_code not like '%PA%' and adm.discharge_code not like '%CL%' and  adm.nurse_ID  like '         ' 
		and (wft.admission_number is null or wft.status = 'Completed')

	INSERT INTO [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog] (
		task_code, due, patient_number, admission_number, grp1_type1, grp1_assignee1, status
	)
	SELECT 
		'ASSIGN NURSE', 
		cast(getdate() as date) as due, 
		adm.patient_number, 
		adm.admission_number,  
		3  as grp1_type1, 
		CASE WHEN adm.primary_plan not in ('SKILLT ', 'SKILAC ') then 'HM1' else 'HA6' end as grp1_assignee1, 
		'Pending'
	FROM  [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].dbo.mdhomadm adm
	LEFT JOIN [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog] wft on wft.task_code = 'ASSIGN NURSE'and wft.admission_number = adm.admission_number
	WHERE adm.discharge_code not like 'D%' and adm.discharge_code not like '%PA%'and adm.discharge_code not like '%CL%' and nurse_ID_arr7_2 like '         ' 
		and adm.primary_plan not like 'PPC    ' and (wft.admission_number is null or wft.status = 'Completed')


-----------------------------Schedule Visits over 15 minutes------------------------------------

	IF DATENAME(WEEKDAY, GETDATE()) = 'TUESDAY'
	BEGIN
		DROP TABLE IF EXISTS #sch1

		SELECT 
			sch.patient_number, 
			sch.admission_number, 
			--sch.order_number, 
			cast(sch.update_date as date) as update_date,
			sch.service_category_array2_1,
			sch.schedule_status_code,
			concat(trim(nur.last_name), ', ', trim(nur.first_name)) as caregiver_name,
			adm.nurse_ID as coordinator, 
			nur.phone,
			cde.user_code,
			sch.schedule_date, 
			sch.documentation_date,
			sch.schedule_time, 
			cast((left(trim(sch.duration), 2)*60 + right(trim(sch.duration), 2)) as int) as duration_min, 
			sch.confirmed_time, 
			cast((left(trim(sch.confirmed_duration), 2)*60 + right(trim(sch.confirmed_duration), 2)) as int) as confirmed_duration_min,
			concat(cast(schedule_date as date), ': ', left(cast(sch.schedule_time as time), 5), case when left(cast(sch.schedule_time as time), 2) < 12 then ' AM' else ' PM' end,  ' ', /* trim(service_category_array2_1), */ ' visit went over by ', cast((left(trim(sch.confirmed_duration), 2)*60 + right(trim(sch.confirmed_duration), 2)) as int) - cast((left(trim(duration), 2)*60 + right(trim(duration), 2)) as int) , ' minutes') as note,
			cast((left(trim(sch.confirmed_duration), 2)*60 + right(trim(sch.confirmed_duration), 2)) as int) - cast((left(trim(duration), 2)*60 + right(trim(duration), 2)) as int) as minutes_over,
			count(*) over (Partition by adm.nurse_ID) as count_num 
		INTO #sch1
		FROM [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdhomsch] sch 
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdhomadm] adm on adm.admission_number = sch.admission_number 
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdnurdat] nur on nur.nurse_ID = sch.nurse_ID 
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdnurdat] nur2 on nur2.nurse_ID = adm.nurse_ID 
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].isusrcde cde on trim(cde.USER_Name) = concat(trim(nur2.first_NAME), ' ', trim(nur2.last_name))
		WHERE sch.patient_number not in ('      154', '    15201') 
			and cast((left(trim(sch.confirmed_duration), 2)*60 + right(trim(sch.confirmed_duration), 2)) as int) - cast((left(trim(duration), 2)*60 + right(trim(duration), 2)) as int) > 15
			and schedule_status_code  in ('V  ', 'C  ' ,'VP ') and schedule_status_code not like 'Z%'
			and cast(sch.update_date as date) > dateadd(day, -7, getdate()) and nur.employee_profile_code not like '%CCT%'  
			and nur.employee_profile_code not like '%RN%' and nur.employee_profile_code not like '%LPN%' and adm.facility_code not in ('LSK', 'CSS')
		


		INSERT INTO [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog] (
			task_code, due, patient_number, admission_number, grp1_type1, grp1_assignee1, notes, status
		)
		SELECT 
			'OVER 15 MIN VISITS' as task_code, 
			cast(getdate() as date) as due, 
			patient_number,
			admission_number,
			3, 
			user_code, 
			concat('Employee: ', caregiver_name, ', phone number: ', phone, ' ', string_agg(note, '; ')) as note, 
			'Pending' 
		FROM #sch1 
		GROUP BY coordinator, caregiver_name, user_code, patient_number, admission_number, phone

	END

---------------------------Overtime Alert Part 2------------------------------------

	IF DATENAME(WEEKDAY, GETDATE()) = 'THURSDAY'
	BEGIN
		
		DROP TABLE IF EXISTS #otp2, #otp2_visit;

		SELECT 
			concat(trim(nur.last_name), ', ', trim(nur.first_name)) as employee_name,
			sch.nurse_ID, 
			casT(sch.schedule_date as date) as sched_date, 
			sum(cast((left(trim(duration), 2)*60.0 + right(trim(duration), 2)) as int)) as duration_min,
			sum(cast((left(trim(confirmed_duration), 2)*60.0 + right(trim(confirmed_duration), 2)) as int)) as confirmed_duration_min
		INTO #otp2
		FROM [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdhomsch] sch 
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdhomadm] adm on adm.admission_number = sch.admission_number 
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdnurdat] nur on nur.nurse_ID = sch.nurse_ID 
		WHERE sch.patient_number not in ('      154', '    15201') and sch.schedule_status_code in ('P', 'VP', 'V', 'BP', 'C' ) 
			and nur.employee_profile_code not like '%CCT%' and cast(sch.schedule_date as date) between dateadd(day, -10, getdate()) and dateadd(day, -4, getdate()) 
		GROUP BY sch.nurse_ID, casT(sch.schedule_date as date), concat(trim(nur.last_name), ', ', trim(nur.first_name))

		SELECT 
			concat(trim(nur.last_name), ', ', trim(nur.first_name)) as employee_name,
			/* sch.patient_number,  sch.admission_number,  */
			sch.nurse_ID,
			cast(sch.schedule_date as date) as sched_date,
			cast((left(trim(duration), 2)*60.0 + right(trim(duration), 2)) as int) as duration_min,
			cast((left(trim(confirmed_duration), 2)*60.0 + right(trim(confirmed_duration), 2)) as int) as confirmed_duration_min
		INTO #otp2_visit
		FROM [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdhomsch] sch 
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdhomadm] adm on adm.admission_number = sch.admission_number 
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdnurdat] nur on nur.nurse_ID = sch.nurse_ID 
		WHERE sch.patient_number not in ('      154', '    15201') and sch.schedule_status_code in ('P', 'VP', 'V', 'BP', 'C' ) 
			and nur.employee_profile_code not like '%CCT%' AND adm.facility_Code in ('LSK', 'CSS')
			and cast((left(trim(confirmed_duration), 2)*60.0 + right(trim(confirmed_duration), 2)) as int) > 12*60.0 
			and cast((left(trim(confirmed_duration), 2)*60.0 + right(trim(confirmed_duration), 2)) as int) > cast((left(trim(duration), 2)*60.0 + right(trim(duration), 2)) as int)
			and cast(sch.schedule_date as date) between dateadd(day, -10, getdate()) and dateadd(day, -4, getdate())

	
		INSERT INTO [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog] (
			task_code, due,  grp1_type1, grp1_assignee1, notes, status
		) 
		SELECT 
			'OT Alert' as task_code, 
			cast(getdate() as date) as due, 
			3, 
			cde.user_code, 
			concat('Employee ', employee_name, ': exceeded both twelve hours in a visit/day and scheduled hours on ', otp.sched_date) as notes,
			'Pending' as status
		FROM (
			SELECT 
				employee_name, 
				nurse_ID , 
				sched_date
			FROM #otp2
			WHERE confirmed_duration_min > 12*60.0 and confirmed_duration_min > duration_min

			UNION 

			SELECT 
				employee_name, 
				nurse_ID, 
				sched_date
			FROM #otp2_visit
		) otp 
		LEFT JOIN (
			SELECT nurse_id, 
				cast(schedule_date as date) as sched_date, 
				admission_number 
			FROM  [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdhomsch] 
			GROUP BY nurse_id, cast(schedule_date as date), admission_number
		) sch on sch.nurse_ID = otp.nurse_ID and sch.sched_date = otp.sched_date
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdhomadm] adm on adm.admission_number = sch.admission_number
		LEFT JOIN  [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].mdnurdat nur on nur.nurse_ID = adm.nurse_ID
		LEFT JOIN  [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].isusrcde cde on trim(cde.USER_Name) = concat(trim(nur.first_NAME), ' ', trim(nur.last_name))
		GROUP BY otp.employee_name, otp.nurse_ID, otp.sched_date, adm.nurse_ID, cde.user_code

	END
END
