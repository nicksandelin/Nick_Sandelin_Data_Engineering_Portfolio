/*
Use [Operations]

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO


-------------------------
DATE: 03/25/25 By Nick Sandelin
REQUEST FROM EHR Specialist: 
Using HCA-Task_Check, make a task generator to create HCA visits for any missing. 
Patient's will have 4 a year, and they will start on the 1st of the month, usually the 3MO being same as admit date, 6MO being 3 months after, etc.
------------------------------------------------------------
*/

 DROP TABLE IF EXISTS #temp, #temp2, #temp3;

 CREATE TABLE #temp 
    (
        patient_name varchar(60), 
        patient_number varchar(60), 
        admission_number varchar(60),
        NumberOfMissingTasks varchar(60), 
        theGroup varchar(60), 
        facility_code varchar(60), 
        discharge_code varchar(60), 
        taskItem varchar(60), 
        WF_Task_Number varchar(60), 
        [start_date] varchar(60), 
        DaysFromLast varchar(60), 
        DaysUntilNext varchar(60),
    )
 
 INSERT INTO #temp
    (
        patient_name, 
        patient_number, 
        admission_number,
        NumberOfMissingTasks, 
        theGroup, 
        facility_code, 
        discharge_code, 
        taskItem, 
        WF_Task_Number, 
        [start_date], 
        DaysFromLast, 
        DaysUntilNext
    )
    EXEC [operations].[dbo].[HCA-task-check];

with cte as (
        select patient_number, admission_number, max([start_date]) as latestitemdate, max(WF_Task_Number) as latest_task_id
        from [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].WFTaskEntry
        where [start_date] < GETDATE() and WF_Task_Code like 'HCA %'
        group by patient_number, admission_number
    )
     , cte2 as (
        select
            cer.patient_number, 
            max(cer.certification_from) as certification_from,
            max(cer.certification_thru) as certification_thru, 
            CASE WHEN string_agg(trim(cer.certification_code),';') like '%IHSS%' THEN 'HMA' else 'Non_HMA' end as servicetype
        from #temp tt
        left join [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].[mdpatcer] cer on cer.admission_number = tt.admission_number
AND (
    (
        GETDATE() BETWEEN CAST(cer.certification_from AS DATE) 
        AND CAST(cer.certification_thru AS DATE) 
        AND DATEDIFF(DAY, cer.certification_from, cer.certification_thru) = 365
    )
    OR (
        DATEADD(DAY, 365, GETDATE()) BETWEEN CAST(cer.certification_from AS DATE) 
        AND CAST(cer.certification_thru AS DATE)
    )
    OR (
        GETDATE() BETWEEN CAST(cer.certification_from AS DATE) 
        AND CAST(cer.certification_thru AS DATE)
    )
)
            AND TRIM(cer.certification_code) NOT IN ('SUPERV', 'AUTH', '485POC')
            AND TRIM(cer.certification_code) <> ''
        group by cer.patient_number
    )
        Select
            c2.certification_from,
            c2.certification_thru,
            cast(case 
                when tt.taskitem like '%ANNUAL%' then case when dateadd(month,9,dateadd(year,-1,dateadd(day,1,c2.certification_thru))) < DATEADD(month, DATEDIFF(month, 0, getdate()), 0) then dateadd(month,9+12,dateadd(year,-1,dateadd(day,1,c2.certification_thru))) else dateadd(month,9,dateadd(year,-1,dateadd(day,1,c2.certification_thru))) end 
                when tt.taskitem like '%3MO%' then case when dateadd(month,0,dateadd(year,-1,dateadd(day,1,c2.certification_thru))) < DATEADD(month, DATEDIFF(month, 0, getdate()), 0) then dateadd(month,0+12,dateadd(year,-1,dateadd(day,1,c2.certification_thru))) else dateadd(month,0,dateadd(year,-1,dateadd(day,1,c2.certification_thru))) end 
                when tt.taskitem like '%6MO%' then case when dateadd(month,3,dateadd(year,-1,dateadd(day,1,c2.certification_thru))) < DATEADD(month, DATEDIFF(month, 0, getdate()), 0) then dateadd(month,3+12,dateadd(year,-1,dateadd(day,1,c2.certification_thru))) else dateadd(month,3,dateadd(year,-1,dateadd(day,1,c2.certification_thru))) end 
                when tt.taskitem like '%9MO%' then case when dateadd(month,6,dateadd(year,-1,dateadd(day,1,c2.certification_thru))) < DATEADD(month, DATEDIFF(month, 0, getdate()), 0) then dateadd(month,6+12,dateadd(year,-1,dateadd(day,1,c2.certification_thru))) else dateadd(month,6,dateadd(year,-1,dateadd(day,1,c2.certification_thru))) end 
            end as date) as correctduedate,
            cast(cast(tt.[start_date] as datetime) at time zone 'UTC' at time zone 'mountain standard time' as date) as wft_due_date,
            c2.servicetype,
            case 
                when tt.taskitem like '%3MO%' then 'HCA 3MO SUPER '
                when tt.taskitem like '%6MO%' then 'HCA 6MO SUPER'
                when tt.taskitem like '%9MO%' then 'HCA 9MO SUPER'
                when tt.taskitem like '%ANNUAL%' then case when c2.servicetype = 'Non_HMA' then 'NON-HMA HCA ANNUAL VISIT' else 'HCA ANNUAL VISIT – RN/CC IN PERS' end
            end as task_code,
            cast(case 
                when ((tt.taskitem like '%ANNUAL%' and tt.[start_date] is null) or (((c2.servicetype = 'Non_HMA' and wte.wf_task_code <> 'NON-HMA HCA ANNUAL VISIT') or (c2.servicetype = 'HMA' and wte.wf_task_code <> 'HCA ANNUAL VISIT – RN/CC IN PERS'))))
                    then
                        case 
                            when (select sq.[start_date] from #temp sq where sq.taskitem like '%9MO%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number) is not null 
                                then dateadd(month,3,(select sq.[start_date] from #temp sq where sq.taskitem like '%9MO%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number))
                            when (select sq.[start_date] from #temp sq where sq.taskitem like '%6MO%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number) is not null
                                then dateadd(month,6,(select sq.[start_date] from #temp sq where sq.taskitem like '%6MO%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number))
                            when (select sq.[start_date] from #temp sq where sq.taskitem like '%3MO%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number) is not null
                                then dateadd(month,9,(select sq.[start_date] from #temp sq where sq.taskitem like '%3MO%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number))
                            when wte.wf_task_code like '%9MO%' and cte.latestitemdate > dateadd(month,-2,getdate()) then dateadd(month,3,cte.latestitemdate)
                            when wte.wf_task_code like '%6MO%' and cte.latestitemdate > dateadd(month,-2,getdate()) then dateadd(month,6,cte.latestitemdate)
                            when wte.wf_task_code like '%3MO%' and cte.latestitemdate > dateadd(month,-2,getdate()) then dateadd(month,9,cte.latestitemdate)
                            when wte.wf_task_code like '%ANNUAL%' and cte.latestitemdate > dateadd(month,-2,getdate()) then dateadd(month,12,cte.latestitemdate)
                            else dateadd(month, 0, dateadd(WEEK,cast(right(tt.patient_number,1) as int), dateadd(day,21,getdate()))) 
                        end 
                when tt.taskitem like '%3MO%' and tt.[start_date] is null 
                    then
                        case 
                            when (select sq.[start_date] from #temp sq where sq.taskitem like '%ANNUAL%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number) is not null 
                                then dateadd(month,3,(select sq.[start_date] from #temp sq where sq.taskitem like '%ANNUAL%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number))
                            when (select sq.[start_date] from #temp sq where sq.taskitem like '%9MO%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number) is not null
                                then dateadd(month,6,(select sq.[start_date] from #temp sq where sq.taskitem like '%9MO%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number))
                            when (select sq.[start_date] from #temp sq where sq.taskitem like '%6MO%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number) is not null
                                then dateadd(month,9,(select sq.[start_date] from #temp sq where sq.taskitem like '%6MO%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number))
                            when wte.wf_task_code like '%ANNUAL%' and cte.latestitemdate > dateadd(month,-2,getdate()) then dateadd(month,3,cte.latestitemdate)
                            when wte.wf_task_code like '%9MO%' and cte.latestitemdate > dateadd(month,-2,getdate()) then dateadd(month,6,cte.latestitemdate)
                            when wte.wf_task_code like '%6MO%' and cte.latestitemdate > dateadd(month,-2,getdate()) then dateadd(month,9,cte.latestitemdate)
                            when wte.wf_task_code like '%3MO%' and cte.latestitemdate > dateadd(month,-2,getdate()) then dateadd(month,12,cte.latestitemdate)
                            else dateadd(month, 3, dateadd(WEEK,cast(right(tt.patient_number,1) as int), dateadd(day,21,getdate()))) 
                        end
                when tt.taskitem like '%6MO%' and tt.[start_date] is null 
                    then
                        case 
                            when (select sq.[start_date] from #temp sq where sq.taskitem like '%3MO%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number) is not null 
                                then dateadd(month,3,(select sq.[start_date] from #temp sq where sq.taskitem like '%3MO%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number))
                            when (select sq.[start_date] from #temp sq where sq.taskitem like '%ANNUAL%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number) is not null
                                then dateadd(month,6,(select sq.[start_date] from #temp sq where sq.taskitem like '%ANNUAL%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number))
                            when (select sq.[start_date] from #temp sq where sq.taskitem like '%9MO%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number) is not null
                                then dateadd(month,9,(select sq.[start_date] from #temp sq where sq.taskitem like '%9MO%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number))
                            when wte.wf_task_code like '%3MO%' and cte.latestitemdate > dateadd(month,-2,getdate()) then dateadd(month,3,cte.latestitemdate)
                            when wte.wf_task_code like '%ANNUAL%' and cte.latestitemdate > dateadd(month,-2,getdate()) then dateadd(month,6,cte.latestitemdate)
                            when wte.wf_task_code like '%9MO%' and cte.latestitemdate > dateadd(month,-2,getdate()) then dateadd(month,9,cte.latestitemdate)
                            when wte.wf_task_code like '%6MO%' and cte.latestitemdate > dateadd(month,-2,getdate()) then dateadd(month,12,cte.latestitemdate)
                            else dateadd(month, 6 , dateadd(WEEK,cast(right(tt.patient_number,1) as int), dateadd(day,21,getdate()))) 
                        end
                when tt.taskitem like '%9MO%' and tt.[start_date] is null 
                    then
                        case 
                            when (select sq.[start_date] from #temp sq where sq.taskitem like '%6MO%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number) is not null 
                                then dateadd(month,3,(select sq.[start_date] from #temp sq where sq.taskitem like '%6MO%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number))
                            when (select sq.[start_date] from #temp sq where sq.taskitem like '%3MO%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number) is not null
                                then dateadd(month,6,(select sq.[start_date] from #temp sq where sq.taskitem like '%3MO%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number))
                            when (select sq.[start_date] from #temp sq where sq.taskitem like '%ANNUAL%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number) is not null
                                then dateadd(month,9,(select sq.[start_date] from #temp sq where sq.taskitem like '%ANNUAL%' and sq.patient_number = tt.patient_number and sq.admission_number = tt.admission_number))
                            when wte.wf_task_code like '%6MO%' and cte.latestitemdate > dateadd(month,-2,getdate()) then dateadd(month,3,cte.latestitemdate)
                            when wte.wf_task_code like '%3MO%' and cte.latestitemdate > dateadd(month,-2,getdate()) then dateadd(month,6,cte.latestitemdate)
                            when wte.wf_task_code like '%ANNUAL%' and cte.latestitemdate > dateadd(month,-2,getdate()) then dateadd(month,9,cte.latestitemdate)
                            when wte.wf_task_code like '%9MO%' and cte.latestitemdate > dateadd(month,-2,getdate()) then dateadd(month,12,cte.latestitemdate)
                            else dateadd(month, 9, dateadd(WEEK,cast(right(tt.patient_number,1) as int), dateadd(day,21,getdate()))) 
                        end
                    else tt.[start_date]
            end at time zone 'UTC' at time zone 'mountain standard time' as date) as due, 
            wte.WF_Task_Code as latest_task,
            cast(cte.latestitemdate as date) as latest_task_date,
            tt.*
        into #temp2 
        from #temp tt
        left join cte2 c2 on c2.patient_number = tt.patient_number 
        left join Operations.dbo.WorkflowTasklog wtl on wtl.patient_number = tt.patient_number and wtl.task_code like 'HCA %'
        left join cte on cte.patient_number = tt.patient_number and cte.admission_number = tt.admission_number
        left join [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].WFTaskEntry wte 
            ON wte.patient_number = cte.patient_number 
            and wte.admission_number = cte.admission_number 
            and wte.wf_task_code LIKE 'HCA %'
            and wte.[Start_Date] = cte.latestitemdate 
            and wte.wf_task_number = cte.latest_task_id


        SELECT distinct
            t2.WF_Task_Number
            ,wte.wf_Task_code
            ,t2.servicetype
            ,t2.task_code
            ,case when t2.correctduedate is not null then concat(left(cast(t2.correctduedate as varchar),7),'-01') else null end as due
            ,t2.wft_due_date
            ,cast(t2.certification_from as date) as certification_from
            ,cast(t2.certification_thru as date) as certification_thru
            ,t2.patient_number
            ,t2.admission_number
            ,case when 
                (t2.task_code = wte.wf_task_code or (wte.wf_task_code is not null and wte.wf_task_code not like '%ANNUAL%')) and (concat(left(cast(t2.correctduedate as varchar),7),'-01') = t2.wft_due_date)
                then '' else concat('Client needs an ',case when t2.task_code like '%MO%' then left(trim(t2.task_code),7) else left(trim(t2.task_code),10) end,' visit scheduled for ','the assigned due date.') end as notes
            ,case 
                when  (t2.task_code = wte.wf_task_code or (wte.wf_task_code is not null and wte.wf_task_code not like '%ANNUAL%')) and (concat(left(cast(t2.correctduedate as varchar),7),'-01') = t2.wft_due_date)  then null
                when t2.task_code like '%annual%' and t2.servicetype = 'HMA' then 
                    case when concat(trim(cm3.first_NAME), ' ', trim(cm3.last_name)) = ' ' or cde3.user_code is null then 2 else 3 end
                else
                    case when concat(trim(cm1.first_NAME), ' ', trim(cm1.last_name)) = ' ' or cde1.user_code is null then 2 else 3 end
            end as grp1_type1
            ,case 
                when (t2.task_code = wte.wf_task_code or (wte.wf_task_code is not null and wte.wf_task_code not like '%ANNUAL%')) and (concat(left(cast(t2.correctduedate as varchar),7),'-01') = t2.wft_due_date) then ''
                when t2.task_code like '%annual%' and t2.servicetype = 'HMA' then
                    case when concat(trim(cm3.first_NAME), ' ', trim(cm3.last_name)) = ' ' or cde3.user_code is null then 'UNSMGR' else cde3.user_code end 
                else
                    case when concat(trim(cm1.first_NAME), ' ', trim(cm1.last_name)) = ' ' or cde1.user_code is null then 'UNSMGR' else cde1.user_code end 
            end as grp1_assignee1
            ,case when  (t2.task_code = wte.wf_task_code or (wte.wf_task_code is not null and wte.wf_task_code not like '%ANNUAL%')) and (concat(left(cast(t2.correctduedate as varchar),7),'-01') = t2.wft_due_date)  then '' else 'Pending' end as [status]
            ,case 
                when  (t2.task_code = wte.wf_task_code or (wte.wf_task_code is not null and wte.wf_task_code not like '%ANNUAL%'))  and (concat(left(cast(t2.correctduedate as varchar),7),'-01') = t2.wft_due_date) then ''
                when t2.task_code like '%annual%' and t2.servicetype = 'HMA' then 
                    case when concat(trim(cm3.first_NAME), ' ', trim(cm3.last_name)) = ' ' or cde3.user_code is null then 'UNASSIGNED MGR' else concat(trim(cm3.first_NAME), ' ', trim(cm3.last_name)) end
                else
                    case when concat(trim(cm1.first_NAME), ' ', trim(cm1.last_name)) = ' ' or cde1.user_code is null then 'UNASSIGNED MGR' else concat(trim(cm1.first_NAME), ' ', trim(cm1.last_name)) end
            end as assignee_casemanager_name
        into #temp3 
        from #temp2 t2
        LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].mdhomadm adm on adm.admission_number = t2.admission_number
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].mdnurdat cm1 on cm1.nurse_ID = adm.nurse_id
        LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].mdnurdat cm3 on cm3.nurse_ID = adm.nurse_ID_arr7_2
		LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].isusrcde cde1 on trim(cde1.USER_Name) = concat(trim(cm1.first_NAME), ' ', trim(cm1.last_name)) 
        LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].isusrcde cde3 on trim(cde3.USER_Name) = concat(trim(cm3.first_NAME), ' ', trim(cm3.last_name)) 
		LEFT JOIN [SQL\MyCompany].[Operations].[dbo].[WorkflowTasklog] wft on wft.grp1_assignee1 = case when t2.task_code like '%ANNUAL%' then cde3.user_code else cde1.user_code end and wft.Task_Code = t2.task_code and wft.patient_number = t2.patient_number and t2.due = wft.due
        left join [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].wftaskentry wte on wte.wf_task_number = t2.WF_Task_Number
        where correctduedate is not null
        order by patient_number, due

select
     case 
        when WF_Task_Number is not null and notes = '' then 'Existing task is good, no action'
        when WF_Task_Number is not null and notes <> '' and wf_task_code <> task_code and task_code like '%ANNUAL%' then 'Wrong annual visit HMA/nonHMA type - Delete existing task and create new task'
        when wf_task_number is not null and notes <> '' and due <> wft_due_date then 'Incorrect due date - Delete existing task and create new task'
        when wf_task_number is null then 'No visit scheduled or existing task - Create new task'
        else 'Unknown, please have nick review'
     end as [action]
    ,t3.*
from #temp3 t3 
order by patient_number, due
