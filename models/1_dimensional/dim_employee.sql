/*
This file uses the employee data and creates a dim table.
*/
{{ config(
    materialized="incremental", 
    unique_key=["emp_id"], 
    schema = "dimensional",
    tags=["general_dim"]
    ) }}

select
    emp_id,
    first_name,
    last_name,
    title,
    supervisor,
    ad_email,
    business_unit,
    employee_status,
    employee_type,
    pay_zone,
    employee_classification_type,
    department_type,
    division,
    dob,
    state,
    job_function_description,
    gender_code,
    location_code,
    race_desc,
    marital_desc,
    current_timestamp() as record_updated
from {{ ref("stg_employee") }}
