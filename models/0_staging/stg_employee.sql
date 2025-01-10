/*
This file takes the landed table for employee data and cleans it.
*/
{{ config(
    materialized="incremental", 
    unique_key=["emp_id"], 
    schema = "staging"
    ) }}

select
    cast(`EmpID` as integer) as emp_id,
    cast(`FirstName` as string) as first_name,
    cast(`LastName` as string) as last_name,
    cast(`StartDate` as date) as start_date,
    cast(`ExitDate` as date) as exit_date,
    cast(`Title` as string) as title,
    cast(`Supervisor` as string) as supervisor,
    cast(`ADEmail` as string) as ad_email,
    cast(`BusinessUnit` as string) as business_unit,
    cast(`EmployeeStatus` as string) as employee_status,
    cast(`EmployeeType` as string) as employee_type,
    cast(`PayZone` as string) as pay_zone,
    cast(`EmployeeClassificationType` as string) as employee_classification_type,
    cast(`TerminationType` as string) as termination_type,
    cast(`TerminationDescription` as string) as termination_description,
    cast(`DepartmentType` as string) as department_type,
    cast(`Division` as string) as division,
    cast(`DOB` as date) as dob,
    cast(`State` as string) as state,
    cast(`JobFunctionDescription` as string) as job_function_description,
    cast(`GenderCode` as string) as gender_code,
    cast(`LocationCode` as integer) as location_code,
    cast(`RaceDesc` as string) as race_desc,
    cast(`MaritalDesc` as string) as marital_desc,
    cast(`Performance Score` as string) as performance_score,
    cast(`Current Employee Rating` as integer) as current_employee_rating,
    current_timestamp() as record_updated
from {{ source("big_query_landing", "employee") }}
