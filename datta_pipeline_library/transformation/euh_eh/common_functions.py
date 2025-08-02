# Databricks notebook source
pip install msal

# COMMAND ----------

pip install adal

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp,date_format
from pyspark.sql.functions import col, trim
from datetime import datetime
spark = SparkSession.getActiveSession()
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

from datta_pipeline_library.core.base_config import (
    BaseConfig,
    CollibraConfig,
    CommonConfig,
    EnvConfig
)
from datta_pipeline_library.helpers.adls import configure_spark_to_use_spn_to_write_to_adls_gen2
from datta_pipeline_library.helpers.spn import AzureSPN
# from datta_pipeline_library.edc.collibra import fetch_business_metadata
from datta_pipeline_library.helpers.uc import (
    get_catalog_name,
    get_raw_schema_name,
    get_curated_schema_name,
    get_eh_schema_name,
)

# COMMAND ----------

env = dbutils.widgets.get(name="env")
repos_path = dbutils.widgets.get(name="repos_path")
unique_repo_branch_id = dbutils.widgets.get(name="unique_repo_branch_id")
unique_repo_branch_id_schema = dbutils.widgets.get(name="unique_repo_branch_id_schema")

# env = "dev"
# repos_path = "/Repos/DATTA-Data-Validation/DATTA-TECH-EH"
# unique_repo_branch_id = ""
# unique_repo_branch_id_schema = ""

common_conf = CommonConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/common/common_conf.json")
print("common_conf !!!")
env_conf = EnvConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/{env}/conf.json")
print("env_conf !!!")

base_config = BaseConfig.from_confs(env_conf, common_conf)
if unique_repo_branch_id:
    base_config.set_unique_id(unique_repo_branch_id)

if unique_repo_branch_id_schema:
    base_config.set_unique_id_schema(unique_repo_branch_id_schema)

# COMMAND ----------

kv = env_conf.kv_key

# values from key vault
tenant_id = dbutils.secrets.get(scope=kv, key="AZ-AS-SPN-DATTA-TENANT-ID")
spn_client_id = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_id_key)
spn_client_secret = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_secret_key)
edc_user_id = dbutils.secrets.get(scope=kv, key=env_conf.edc_user_id_key)
edc_user_pwd = dbutils.secrets.get(scope=kv, key=env_conf.edc_user_pwd_key)

spn = AzureSPN(tenant_id, spn_client_id, spn_client_secret)
configure_spark_to_use_spn_to_write_to_adls_gen2(env_conf.storage_account, spn)

# COMMAND ----------

uc_catalog_name = base_config.get_uc_catalog_name()
print("uc_catalog_name : ",uc_catalog_name)

uc_raw_schema = base_config.get_uc_raw_schema()
print("uc_raw_schema : ",uc_raw_schema)
uc_euh_schema = base_config.get_uc_euh_schema()
print("uc_euh_schema : ",uc_euh_schema)
uc_eh_schema = base_config.get_uc_eh_schema()
print("uc_eh_schema : ",uc_eh_schema)

raw_folder_path = base_config.get_raw_folder_path()
print("raw_folder_path : ",raw_folder_path)
euh_folder_path = base_config.get_euh_folder_path()
print("euh_folder_path : ",euh_folder_path)
eh_folder_path = base_config.get_eh_folder_path()
print("eh_folder_path : ",eh_folder_path)

tbl_owner_grp = base_config.get_tbl_owner_grp()
print("tbl_owner_grp : ",tbl_owner_grp)
tbl_read_grp = base_config.get_tbl_read_grp()
print("tbl_read_grp : ",tbl_owner_grp)

uc_eh_md_schema = uc_eh_schema.replace("-capability", "-masterdata")
print("uc_eh_md_schema : ",uc_eh_md_schema)

eh_md_folder_path=eh_folder_path.replace("/capability_", "/masterdata_")
print("eh_md_folder_path: ", eh_md_folder_path)

# COMMAND ----------

''' assignPermission This function assigns Permission to all the tables created '''
def assignPermission(catalog,schema,table_name,tbl_owner,tbl_read):
    spark.sql(f"ALTER table `{catalog}`.`{schema}`.{table_name} owner to `{tbl_owner}`")
    print("Table Owner is assigned")
    spark.sql(f"GRANT ALL PRIVILEGES ON TABLE `{catalog}`.`{schema}`.{table_name} TO `{tbl_owner}`")
    print("All privileges access given to tbl owner")
    spark.sql(f"GRANT SELECT ON TABLE `{catalog}`.`{schema}`.{table_name} TO `{tbl_read}`")
    print("Reader access granted")

# COMMAND ----------

def get_latest_delta(dlt_workflow_name, catalog, schema):
    df_max_run_start_time = spark.sql(f"""select date_format(run_start_date, "yyyMMddHHmmss") as date from `{catalog}`.`{schema}`.process_status where run_id in (select max(run_id) as max_run_id from `{catalog}`.`{schema}`.process_status where status = 'completed' and dlt_workflow_name = '{dlt_workflow_name}') and dlt_workflow_name = '{dlt_workflow_name}'""").first()[0]
                            
    return df_max_run_start_time

# COMMAND ----------

# DBTITLE 1,Common_tables
"""
currency Conversion
:return: Dataframe
"""
df_TCURR = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.tcurr")
df_TCURR_BW = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.tcurr_bw")
df_TCURV = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.tcurv")
df_TCURV_BW = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.tcurv_bw")
df_TCURT = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.tcurt")
df_TCURW = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.tcurw")
df_TCURW_BW = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.tcurw_bw")
df_TCURN = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.tcurn")
df_TCURF = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.tcurf")
df_TCURF_BW = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.tcurf_bw")
df_TCURC = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.tcurc")
df_TCURR_PATTERN = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.tcurr_pattern")
df_DATE = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.date")

# df_TIME_PATTERN = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.time_pattern")
# df_TCURX = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.tcurx")

"""
Asset T006,T006A 
:return: Dataframe
"""
df_T006 = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.t006")
df_T006A = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.t006a")

"""
Asset MARA,MARM 
:param transform_dict: Transform dict of transformation function
:return: Dataframe
"""
df_MARA = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.mara")
df_MARM = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.marm")

# COMMAND ----------

""" Function to modify DateField(GDATU) to format yyyyMMdd in Currency related tables"""
def date_conversion(df):
    filtered_df = df.filter(trim(col("GDATU")) != "")
    filtered_df = df.withColumn("GDATU_DATS", 99999999 - df["GDATU"].cast("int")) \
                    .withColumn("GDATU_DATS", to_date("GDATU_DATS" , "yyyyMMdd"))
    return filtered_df

""" Function to apply language filter to E(English)"""
def language_filter(df):
    filtered_df = df.filter(df.SPRAS=="E") 
    return filtered_df

""" Function to apply client filter as 110 """
def client_filter(df):
    filtered_df = df.filter(df.MANDT==110) 
    return filtered_df
