# Databricks notebook source
# MAGIC %run ./config_to_read_outside_DLT

# COMMAND ----------

host=workspace_url 

if workflow_frequency == 'DV':
  job_name = dv_workflow_name
  dlt_workflow_name = dv_workflow_name
elif workflow_frequency == 'UOM':
  job_name= uom_workflow_name
  dlt_workflow_name = uom_workflow_name
elif workflow_frequency == 'UOM-DEMO':
  job_name= uom_demo_workflow_name
  dlt_workflow_name = uom_demo_workflow_name
else:
  job_name = workflow_name
  dlt_workflow_name = workflow_name

spnvalue = spnvalue

# COMMAND ----------

process_table_name='process_status'
table_name='`'+uc_catalog_name+'`.`'+uc_raw_schema+'`'+'.'+process_table_name

http_header = get_dbx_http_header(spnvalue)
dlt_workflow_job_id=get_job_id(job_name, host, http_header)

# COMMAND ----------

from datta_pipeline_library.core.process_status import update_table
update_table(table_name,dlt_workflow_name,dlt_workflow_job_id)
