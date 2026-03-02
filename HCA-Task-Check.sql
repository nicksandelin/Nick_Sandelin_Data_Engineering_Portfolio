Use [Operations]

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*
-------------------------
DATE: 10/5/24 By Nick Sandelin
REQUEST FROM EHR Specialist: 
 - check HCA tasks looking forward 1 year, unskilled at LUS and CSU facility, see if all 4 present and days between
------------------------------------------------------------
*/
	
ALTER PROCEDURE [dbo].[HCA-task-check]  -- exec [Operations].[dbo].[HCA-task-check]
 	
AS
BEGIN


drop table if exists #certGrouping, #tasks, #main

  --Create groups
    CREATE TABLE #certGrouping (
        certCode VARCHAR(MAX),
        theGroup varchar(MAX)
    );

    insert into #certGrouping (certCode, theGroup)
    VALUES
        ('CNA', 'CLASS A'),
        ('CNAACUTE', 'CLASS A'),
        ('HMK', 'CLASS B - PHS'),
        ('IHSHMK', 'CLASS B - IHSS'),
        ('IHSPCR', 'CLASS B - IHSS'),
        ('IHSPCW', 'CLASS B - IHSS'),
        ('IHSS', 'CLASS B - IHSS'),
        ('MLHMKR', 'CLASS B - PHS'),
        ('MLPECA', 'CLASS B - PHS'),
        ('MLRESP', 'CLASS B - PHS'),
        ('NURSE ACUT', 'CLASS A'),
        ('NURSING', 'CLASS A'),
        ('PCW', 'CLASS B - PHS'),
        ('PDN', 'CLASS A'),
        ('PPCW', 'CLASS B - PPC'),
        ('Respite', 'CLASS B - PHS'),
        ('RPCW', 'CLASS B - PHS');

    CREATE TABLE #tasks (
        thegroup varchar(MAX), 
        taskItem varchar(MAX)
    );

    insert into #tasks (thegroup, taskitem) -- select * from #tasks
    VALUES
        ('CLASS B - IHSS', 'HCA 3MO SUPER'),
        ('CLASS B - IHSS', 'HCA 6MO SUPER'),
        ('CLASS B - IHSS', 'HCA 9MO SUPER'),
        ('CLASS B - IHSS', 'HCA ANNUAL VISIT'),
        ('CLASS B - PHS', 'HCA 3MO SUPER'),
        ('CLASS B - PHS', 'HCA 6MO SUPER'),
        ('CLASS B - PHS', 'HCA 9MO SUPER'),
        ('CLASS B - PHS', 'HCA ANNUAL VISIT');

   -- Temporary table for task tracking
WITH RankedTasks AS (
    SELECT 
        adm.patient_number,
        adm.admission_number,
        cg.theGroup,
        adm.facility_code,
        adm.discharge_code,
        tk.taskItem,
        wft.WF_Task_Number,
        case when wft.Frequency_Type = 2 and wft.start_date < DATEADD(month, DATEDIFF(month, 0, getdate()), 0) then dateadd(day,365,wft.start_date) else wft.start_date end as start_date,
        --wft.start_date,
        ROW_NUMBER() OVER (
            PARTITION BY adm.patient_number, tk.taskItem 
            ORDER BY wft.start_date DESC
        ) AS row_num
       -- ,case when wft.Frequency_Type = 2 and wft.start_date < dateadd(day,-60,getdate()) then dateadd(day,365,wft.start_date) else wft.start_date end as due_date
    FROM [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].dbo.mdhomadm adm
    LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].dbo.mdpatcer cer 
        ON adm.patient_number = cer.patient_number 
        AND adm.admission_number = cer.admission_number
    LEFT JOIN #certGrouping cg 
        ON cg.certCode = cer.Certification_Code
    LEFT JOIN #tasks tk 
        ON tk.theGroup = cg.theGroup
    LEFT JOIN [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].WFTaskEntry wft -- select * from [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].WFTaskEntry where patient_number like '%10809%'
        ON wft.patient_number = adm.patient_number 
        AND wft.admission_number = adm.admission_number
        AND (wft.wf_task_code LIKE CONCAT('%', tk.taskItem, '%') or tk.taskItem is null)
        AND ((wft.Frequency_Type = 1 AND wft.start_date BETWEEN DATEADD(DAY, -60, GETDATE()) AND DATEADD(DAY, 420, GETDATE())) 
            OR (wft.Frequency_Type = 2 AND wft.start_date BETWEEN DATEADD(DAY, -422, GETDATE()) AND DATEADD(DAY, 420, GETDATE()) AND wft.end_date > GETDATE()))
    WHERE 
        GETDATE() BETWEEN CAST(certification_from AS DATE) AND CAST(certification_thru AS DATE)
        AND TRIM(certification_code) NOT IN ('SUPERV', 'AUTH', '485POC')
        AND adm.Discharge_Code NOT LIKE 'D%' 
        AND adm.Discharge_Code not in  ('PA','CL ')
        AND adm.patient_number <> 154 
        AND TRIM(adm.Facility_Code) IN ('LUS', 'CSU')
        AND ( 
            wft.start_date IS NULL 
            OR (
                (wft.Frequency_Type = 1 AND wft.start_date BETWEEN DATEADD(DAY, -60, GETDATE()) AND DATEADD(DAY, 420, GETDATE())) 
                OR (wft.Frequency_Type = 2 and cb_completed = 0 and cb_skipped = 0 and cb_deleted = 0 AND wft.start_date BETWEEN DATEADD(DAY, -422, GETDATE()) AND DATEADD(DAY, 420, GETDATE()) AND wft.end_date > GETDATE()))) 
        AND TRIM(cer.certification_code) <> ''
    )
-- Keep only the first row for each patient and task type
SELECT 
    concat(trim(pat.last_name),', ',trim(pat.first_name)) as patient_name,
    rt.patient_number,
    rt.admission_number,
    (4 - COUNT(rt.WF_Task_Number) OVER (PARTITION BY rt.patient_number)) AS NumberOfMissingTasks,
    rt.theGroup,
    rt.facility_code,
    rt.discharge_code,
    rt.taskItem,
    rt.WF_Task_Number,
    CAST(rt.start_date AT TIME ZONE 'Mountain Standard Time' AT TIME ZONE 'UTC' as datetime) as [start_date],
    DATEDIFF(DAY, LAG(rt.start_date) OVER (PARTITION BY rt.patient_number ORDER BY start_date), rt.start_date) AS DaysFromLast,
    DATEDIFF(DAY, 
        rt.start_date, 
        LEAD(rt.start_date) OVER (PARTITION BY rt.patient_number, rt.admission_number ORDER BY start_date)
    ) AS DaysUntilNext
    --,rt.due_date
FROM RankedTasks rt 
left join [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].dbo.mdpatdat pat on pat.patient_Number = rt.patient_number
WHERE row_num = 1 --and start_date = due_date
ORDER BY pat.last_name, pat.first_name, rt.start_date;


END