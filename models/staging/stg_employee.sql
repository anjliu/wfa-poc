/*
This file takes the landed table for employee data and cleans it.
*/
{{ config (
    materialized = "incremental",
    unique_key = ["EmpID"]
) }}

select
cast(`EmpID` as INTEGER) as EmpID,
cast(`FirstName` as STRING) as FirstName,
cast(`LastName` as STRING) as LastName,
cast(`StartDate` as DATE) as StartDate,
cast(`ExitDate` as DATE) as ExitDate,
cast(`Title` as STRING) as Title,
cast(`Supervisor` as STRING) as Supervisor,
cast(`ADEmail` as STRING) as ADEmail,
cast(`BusinessUnit` as STRING) as BusinessUnit,
cast(`EmployeeStatus` as STRING) as EmployeeStatus,
cast(`EmployeeType` as STRING) as EmployeeType,
cast(`PayZone` as STRING) as PayZone,
cast(`EmployeeClassificationType` as STRING) as EmployeeClassificationType,
cast(`TerminationType` as STRING) as TerminationType,
cast(`TerminationDescription` as STRING) as TerminationDescription,
cast(`DepartmentType` as STRING) as DepartmentType,
cast(`Division` as STRING) as Division,
cast(`DOB` as DATE) as DOB,
cast(`State` as STRING) as State,
cast(`JobFunctionDescription` as STRING) as JobFunctionDescription,
cast(`GenderCode` as STRING) as GenderCode,
cast(`LocationCode` as INTEGER) as LocationCode,
cast(`RaceDesc` as STRING) as RaceDesc,
cast(`MaritalDesc` as STRING) as MaritalDesc,
cast(`Performance Score` as STRING) as Performance_Score,
cast(`Current Employee Rating` as INTEGER) as Current_Employee_Rating,
table_updated = current_timestamp()
from {{ source('employee') }}

