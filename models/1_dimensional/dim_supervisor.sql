/*
This file uses the employee data and creates a dim table for supervisors.
*/
{{ config(
    materialized="incremental", 
    unique_key=["emp_id"], 
    schema="dimensional",
    tags=["general_dim"]
    ) }}

select
    supervisor,
    count(emp_id) as employees_supervised,
    count(case when employee_status = 'Active' then 1 else 0 end) as active_employees_supervised,
    current_timestamp() as record_updated
from {{ ref("dim_employee") }}
group by supervisor
order by count(emp_id) desc
