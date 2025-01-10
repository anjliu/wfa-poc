/*
This file takes the landed table for employee data and cleans it.
*/
{{ config(materialized="incremental", unique_key=["EmpID"]) }}

select *
from `wfa-poc-dev.landing.employee`

