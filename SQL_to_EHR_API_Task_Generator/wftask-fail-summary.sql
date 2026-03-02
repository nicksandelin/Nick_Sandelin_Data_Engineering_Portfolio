USE [Operations]

SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [dbo].[wftask-fail-summary]

AS
BEGIN

-- =============================================
-- Author:		Nick
-- Create date: 7/16/2024
-- Description:	Sends mail to concerned parties for reasons work flow tasks were not able to be created using wftask-manager. 

--NOTES:
-- ============================================

drop table if exists #fail

select distinct
    status_note
into #fail
from [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog] 
where status = 'Failed'
order by status_note


DECLARE @body_content nvarchar(max);
    SET @body_content = N'
    <style type="text/css">
    table.GeneratedTable {
    border-collapse: collapse;
    font-family: Tahoma, Geneva, sans-serif;
    font-size: 11px;
    }

    table.GeneratedTable td{
    padding: 15px;
    }

    table.GeneratedTable thead td {
        background-color: #54585d;
        color: #ffffff;
        font-weight: bold;
        font-size: 11px;
        border: 1px solid #54585d;
    }

    table.GeneratedTable tbody td {
        color: #636363;
        border: 1px solid #dddfe1;
        font-size: 11px;
    }

    table.GeneratedTable tbody tr {
        background-color: #f9fafb;
    }

    table.GeneratedTable tbody tr:nth-child(odd) {
        background-color: #ffffff;
    }

    a{
                mso-line-height-rule:exactly;
            }

    a[href^=tel],a[href^=sms]{
                color:inherit;
                cursor:default;
                text-decoration:none;
            }
    a{
                -ms-text-size-adjust:100%;
                -webkit-text-size-adjust:100%;
            }

    a[x-apple-data-detectors]{
                color:inherit !important;
                text-decoration:none !important;
                font-size:inherit !important;
                font-family:inherit !important;
                font-weight:inherit !important;
                line-height:inherit !important;
            }
    a.mcnButton{
                display:block;
            }



    </style>

    <tr><td valign="top" align="left"><img src="cid:MyCompanyPNG.png" width="175" height="40" border="0" alt=""></td></tr>

    <p>Hello Team, </p>
    <p></p>
    <p>Please see the following error codes that caused work flow task generation to fail:</p>

    <table class="GeneratedTable">
    <thead>
        <tr>
        <td>Status Note</td>
        </tr>
    </thead>
    <tbody>' +
    CAST(
            (select
                td = status_note, ''
            from #fail
            order by status_note
            FOR XML PATH('tr'), TYPE   
            ) AS nvarchar(max)
        ) +
    N'</tbody>
    </table>
    '

    declare @q varchar(max)
    set @q ='
        select * 
        from [SQL\MyCompany].[Operations].[dbo].[WorkflowTaskLog] 
        where status = ''Failed''
        order by taskid
    '
    
DECLARE @tab char(1) = CHAR(9)

EXEC msdb.dbo.sp_send_dbmail
    @profile_name = 'noReply',  
    @recipients = 'REDACTED_EMAIL',
        --@blind_copy_recipients = 'REDACTED_EMAIL',
        @body = @body_content,
        @body_format = 'HTML',
        @subject = 'WFT Fail Summary',
        @file_attachments = 'C:\Program Files\dataMail\MyCompanyPNG.png',
        @query= @q,
        @attach_query_result_as_file=1,
        @query_attachment_filename='wftfailsummary.csv',
        @query_result_separator= @tab, --enforce csv
        @query_result_no_padding=1, --trim
        --@query_no_truncate=1,
        @query_result_width=32767 

/*
--insert into InternalReportLog, finish when manager in prod
insert into [MyCompany_EHR1_Analytical_DB].[dbo].[internalReportLog](
    [runDate]
    ,[reportName]
    ,[casemanagerID]
    ,[totalRows])(
        select 
            cast(getDate() as date),
            'wftFailSummary',
            count(*) 
        FROM #fail
    )
 */

END
GO
