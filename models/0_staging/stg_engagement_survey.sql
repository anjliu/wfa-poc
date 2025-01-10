/*
This file takes the landed table for engagement survey data and cleans it.
*/
{{
    config(
        materialized="incremental",
        unique_key=["employee_id", "survey_date"],
        schema="staging",
    )
}}

select
    cast(`Employee ID` as integer) as employee_id,
    cast(`Survey Date` as date) as survey_date,
    cast(`Engagement Score` as integer) as engagement_score,
    cast(`Satisfaction Score` as integer) as satisfaction_score,
    cast(`Work-Life Balance Score` as integer) as work_life_balance_score,
    current_timestamp() as record_updated
from {{ source('big_query_landing','engagement_survey')}}