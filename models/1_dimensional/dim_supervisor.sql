/*
This file uses the employee data and creates a dim table for supervisors.
*/
{{ config(materialized="incremental", unique_key=["emp_id"], schema="dimensional") }}

select
    supervisor,
    count(emp_id) as employees_supervised,
    current_timestamp() as record_updated
from {{ ref("dim_employee") }}
group by supervisor
order by count(emp_id) desc
