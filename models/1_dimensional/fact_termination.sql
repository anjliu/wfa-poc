/*
This model creates a fact table for terminations using the engagement survey and employee termination data.
*/
{{ config(
    materialized="incremental", 
    unique_key=["emp_id"], 
    schema="dimensional"
    ) }}

select
    emp.emp_id,
    emp.exit_date,
    emp.termination_type,
    emp.termination_description,
    emp.performance_score,
    sur.engagement_score,
    sur.satisfaction_score,
    sur.work_life_balance_score,
    current_timestamp() as record_updated
from {{ ref("stg_employee") }} emp
left join {{ ref("stg_engagement_survey")}} sur on emp.emp_id = sur.employee_id
where emp.employee_status in ('Voluntarily Terminated', 'Terminated for Cause')

