
USE [Operations]
GO
/****** Object:  StoredProcedure [dbo].[wftask-manager]    Script Date: 7/1/2024 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO



-- -- =============================================
-- -- Author:		Nick
-- -- Create date: 7/1/2024
-- -- Description:	Does it all! Except for the triggers, it doesnt do that.... 
-- -- =============================================

ALTER PROCEDURE [dbo].[wftask-manager] -- exec [operations].[dbo].[wftask-manager]

AS
BEGIN

SET NOCOUNT ON;
-- due date stays same, start date = due date and end date is 12/31/2030
-- Wait for 6 seconds for API nonsense
WAITFOR DELAY '00:00:05:250'; -- select * from [Operations].[dbo].[WorkflowTaskLog] order by processed_date desc

insert into [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskQueue] ( -- select * from [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskQueue]
	   [taskid]
      ,[task_code]
      ,[due]
      ,[start_date]
      ,[end_date]
      ,[patient_number]
      ,[admission_number]
      ,[clinical_note_number]
      ,[employeeid]
      ,[notes]
	  ,[grp1_type1]
      ,[grp1_assignee1]
	  ,[grp1_type2]
      ,[grp1_assignee2]
	  ,[grp2_type1]
      ,[grp2_assignee1]
	  ,[grp2_type2]
      ,[grp2_assignee2]
      ,[status]
      ,[status_note]
	  ) (
	select logs.taskid, logs.task_code, logs.due, logs.start_date, logs.end_date, logs.patient_number, 
	logs.admission_number, logs.clinical_note_number, 
	logs.employeeid, logs.notes, logs.grp1_type1, logs.grp1_assignee1,logs.grp1_type2, logs.grp1_assignee2, 
	logs.grp2_type1, logs.grp2_assignee1, 
	logs.grp2_type2,
	logs.grp2_assignee2, logs.status, logs.status_note 
	from [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog] logs
	left join [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskQueue] que on que.taskid = logs.taskid
	where logs.status IN ('Pending') and que.taskid is null
	)


----------------Submit WF Tasks in Queue to EHR1------------------------

DECLARE @taskid int;
DECLARE @EHR1_task_number VARCHAR(15);
DECLARE @WF_Task_Code VARCHAR(32); 
DECLARE @Due_Date VARCHAR(64); 
DECLARE @Start_Date VARCHAR(64); 
DECLARE @End_Date VARCHAR(64); 
DECLARE @Patient_Number VARCHAR(64); 
DECLARE @Admission_Number VARCHAR(64); 
DECLARE @CP_Clinical_Note_Number VARCHAR(64); 
DECLARE @Employee_ID VARCHAR(64); 
DECLARE @WF_Task_Notes VARCHAR(MAX);  
DECLARE @G1_Assignee1_Type NVARCHAR(64); 
DECLARE @G1_Assignee1 VARCHAR(64); 
DECLARE @G1_Assignee2_Type NVARCHAR(64); 
DECLARE @G1_Assignee2 VARCHAR(64);
DECLARE @G2_Assignee1_Type VARCHAR(64); 
DECLARE @G2_Assignee1 VARCHAR(64); 
DECLARE @G2_Assignee2_Type NVARCHAR(64); 
DECLARE @G2_Assignee2 VARCHAR(64);
DECLARE @status VARCHAR(32); 
DECLARE @status_note VARCHAR(500); 
--set up to convert group type from int to string later on
DECLARE @G1A1T VARCHAR(64);  
DECLARE @G1A2T VARCHAR(64); 
DECLARE @G2A1T VARCHAR(64); 
DECLARE @G2A2T VARCHAR(64); 


DECLARE @testlimit int



DECLARE wftcursor CURSOR FOR
SELECT
        isnull(taskid,''),
        --isnull(EHR1_task_number,''),
        isnull(task_code,''),
        isnull(due,''),
        isnull(q.start_date,''),
        isnull(q.end_date,''),
        isnull(patient_number,''),
        isnull(admission_number,''),
        isnull(clinical_note_number,''),
        isnull(employeeid,''),
        isnull(notes,''),
        isnull(grp1_type1,''),
        isnull(grp1_assignee1,''),
        isnull(grp1_type2,''),
        isnull(grp1_assignee2,''),
        isnull(grp2_type1,''),
        isnull(grp2_assignee1,''),
        isnull(grp2_type2,''),
        isnull(grp2_assignee2,''),
        isnull(status,''),
        isnull(status_note,'')
    FROM [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskQueue] q
    WHERE status = 'Pending'
    ORDER BY taskid ASC;  -- Define the query for the cursor

SET @testlimit = 0
OPEN wftcursor;

FETCH NEXT FROM wftcursor 
INTO 
        @taskid
        --,@EHR1_task_number
        ,@WF_Task_Code
        ,@Due_Date
        ,@Start_Date
        ,@End_Date
        ,@Patient_Number
        ,@Admission_Number
        ,@CP_Clinical_Note_Number
        ,@Employee_ID
        ,@WF_Task_Notes
        ,@G1_Assignee1_Type
        ,@G1_Assignee1
        ,@G1_Assignee2_Type
        ,@G1_Assignee2
        ,@G2_Assignee1_Type
        ,@G2_Assignee1
        ,@G2_Assignee2_Type
        ,@G2_Assignee2
        ,@status
        ,@status_note;

WHILE @@FETCH_STATUS = 0 --and @testlimit < 2
BEGIN

    --removing old status_note just in case...
    UPDATE [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskQueue]  
    SET status_note = NULL 
    WHERE taskid=@taskid
    
    UPDATE [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog]  
    SET status_note = NULL 
    WHERE taskid=@taskid

    --assigning proper string value for group assignee
    SET @G1A1T = isnull((SELECT [Type] FROM [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskAssignee] WHERE id = @G1_Assignee1_Type),'');
    SET @G1A2T = isnull((SELECT [Type] FROM [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskAssignee] WHERE id = @G1_Assignee2_Type),'');
    SET @G2A1T = isnull((SELECT [Type] FROM [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskAssignee] WHERE id = @G2_Assignee1_Type),'');
    SET @G2A2T = isnull((SELECT [Type] FROM [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskAssignee] WHERE id = @G2_Assignee2_Type),'');

    --preparing for API JSON response capture
    DECLARE @Object INT;
    DECLARE @Response TABLE (txt NVARCHAR(MAX));
    DECLARE @JSON NVARCHAR(MAX);

    -- Construct JSON payload
    SET @JSON = '
    [
        {"Key": "WF_Task_Code", "Value": "{WF_Task_Code}"},
        {"Key": "Due_Date", "Value": "{Due_Date}"},
        {"Key": "Start_Date", "Value": "{Start_Date}"},
        {"Key": "End_Date", "Value": "{End_Date}"},
        {"Key": "Patient_Number", "Value": "{Patient_Number}"},
        {"Key": "Admission_Number", "Value": "{Admission_Number}"},
        {"Key": "CP_Clinical_Note_Number", "Value": "{CP_Clinical_Note_Number}"},
        {"Key": "Employee_ID", "Value": "{Employee_ID}"},
        {"Key": "WF_Task_Notes", "Value": "{WF_Task_Notes}"},
        {"Key": "AssignTaskTo_Group", "Value": 
            [
                [   
                    {"Key": "Assignee1_Type", "Value": "{G1_Assignee1_Type}"},
                    {"Key": "Assignee1", "Value": "{G1_Assignee1}"}
                ]
            ]
        }
    ]
    ';

    -- Replace placeholders with actual values
    SET @JSON = REPLACE(@JSON,'{WF_Task_Code}', @WF_Task_Code);
    SET @JSON = REPLACE(@JSON,'{Due_Date}', @Due_Date);
    SET @JSON = REPLACE(@JSON,'{Start_Date}', @Start_Date);
    SET @JSON = REPLACE(@JSON,'{End_Date}', @End_Date);
    SET @JSON = REPLACE(@JSON,'{Patient_Number}', @Patient_Number);
    SET @JSON = REPLACE(@JSON,'{Admission_Number}', @Admission_Number);
    SET @JSON = REPLACE(@JSON,'{CP_Clinical_Note_Number}', @CP_Clinical_Note_Number);
    SET @JSON = REPLACE(@JSON,'{Employee_ID}', @Employee_ID);
    SET @JSON = REPLACE(@JSON,'{WF_Task_Notes}', @WF_Task_Notes);
    SET @JSON = REPLACE(@JSON,'{G1_Assignee1_Type}', @G1A1T);
    SET @JSON = REPLACE(@JSON,'{G1_Assignee1}', @G1_Assignee1);
    SET @JSON = REPLACE(@JSON,'{G1_Assignee2_Type}', @G1A2T);
    SET @JSON = REPLACE(@JSON,'{G1_Assignee2}', @G1_Assignee2);
    SET @JSON = REPLACE(@JSON,'{G2_Assignee1_Type}', @G2A1T);
    SET @JSON = REPLACE(@JSON,'{G2_Assignee1}', @G2_Assignee1);
    SET @JSON = REPLACE(@JSON,'{G2_Assignee2_Type}', @G2A2T);
    SET @JSON = REPLACE(@JSON,'{G2_Assignee2}', @G2_Assignee2);

    
    -- Wait for 6 seconds because API needy, I know in here twice but seems to work best this way, this one has to stay in the cursor loop for sure
    WAITFOR DELAY '00:00:05:250';

    -- Initialize HTTP request
    EXEC sp_OACreate 'MSXML2.XMLHTTP', @Object OUT;
    EXEC sp_OAMethod @Object, 'open', NULL, 'POST', 'https://MyCompanyhh.EHR1.net/EHR1CompanyService/EHR1/EHR1RestAPI/AddNewWorkflowTask', 'false';
    EXEC sp_OAMethod @Object, 'setRequestHeader', NULL, 'Content-Type', 'application/json';
    EXEC sp_OAMethod @Object, 'setRequestHeader', NULL, 'Authorization', 'Bearer [REDACTED]';
    EXEC sp_OAMethod @Object, 'send', NULL, @JSON;

    -- Capture response
    INSERT INTO @Response (txt)
    EXEC sp_OAMethod @Object, 'responseText';
    EXEC sp_OADestroy @Object;

    --getting values from json array stored in a table and assigning to response, error, and tasknumber values
    DECLARE @JsonResponse NVARCHAR(MAX);
    SELECT @JsonResponse = txt FROM @Response;
    DECLARE @TaskNumber NVARCHAR(50);
    DECLARE @errorMessage NVARCHAR(500);

    
    IF @JsonResponse LIKE '%"statusCode":200%'  --success handling
    BEGIN

        SELECT @TaskNumber = 
            SUBSTRING([value], 
            CHARINDEX('''', [value]) + 1, 
            CHARINDEX('''', [value], CHARINDEX('''', [value]) + 1) - CHARINDEX('''', [value]) - 1)
        FROM OPENJSON(@JsonResponse)
        WHERE [key] = 'data';

        UPDATE [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog] 
        SET status = 'Sent' , EHR1_task_number = @TaskNumber --, status_note = LEFT(@JsonResponse, 499) --add in EHR1 generated tasknumber from JSON response
        WHERE taskid = @taskid;

        DELETE FROM [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskQueue] --delete from queueu if success
        WHERE taskid = @taskid;

        PRINT concat('Task ',@taskid,' updated.');

    END
    

    ELSE
    BEGIN

        SELECT @errorMessage =  [value] FROM OPENJSON(@JsonResponse) WHERE [key] = 'detailedMessage';

        UPDATE [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskQueue] 
        SET status = 'Failed' , status_note = @errorMessage --added addressable error message to note if fail
        WHERE taskid = @taskid;

        UPDATE [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog] 
        SET status = 'Failed' , status_note = @errorMessage
        WHERE taskid = @taskid;

        PRINT concat('Task ',@taskid,' Failed.'); --printing if anyone cares ;(

    END 
    
    --updating process date and time
    UPDATE [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog]  
    SET processed_date = GETDATE() 
    WHERE taskid=@taskid

    --removing processed items from queue
    DELETE FROM [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskQueue] WHERE taskid = @taskid

    set @testLimit = 1 + @testLimit
    --starting next wf task in queue
    FETCH NEXT FROM wftcursor 
    INTO 
        @taskid
        --,@EHR1_task_number
        ,@WF_Task_Code
        ,@Due_Date
        ,@Start_Date
        ,@End_Date
        ,@Patient_Number
        ,@Admission_Number
        ,@CP_Clinical_Note_Number
        ,@Employee_ID
        ,@WF_Task_Notes
        ,@G1_Assignee1_Type
        ,@G1_Assignee1
        ,@G1_Assignee2_Type
        ,@G1_Assignee2
        ,@G2_Assignee1_Type
        ,@G2_Assignee1
        ,@G2_Assignee2_Type
        ,@G2_Assignee2
        ,@status
        ,@status_note;
END

CLOSE wftcursor;
DEALLOCATE wftcursor;


-- status update from EHR1
update logs
set logs.status = 'Completed' 
from [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog] logs
left join [SQL\EHR1_Linked_Server].[MyCompany_EHR1_DB].[dbo].WFTaskEntry ent on ent.wf_task_number = logs.EHR1_task_number
where cb_completed = 1

SET NOCOUNT OFF;

END
