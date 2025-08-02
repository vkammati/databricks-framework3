# Databricks notebook source
pip install msal

# COMMAND ----------

spark.conf.set("spark.sql.adaptive.maxShuffledHashJoinLocalMapThreshold", -1)

# COMMAND ----------

pip install adal

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from datta_pipeline_library.core.spark_init import spark
from datta_pipeline_library.core.base_config import (
    BaseConfig,
    CommonConfig,
    EnvConfig
)
from datta_pipeline_library.helpers.adls import configure_spark_to_use_spn_to_write_to_adls_gen2
from datta_pipeline_library.helpers.spn import AzureSPN
from datta_pipeline_library.helpers.uc import (
    get_catalog_name
)

# COMMAND ----------

env = dbutils.widgets.get(name="env")
repos_path = dbutils.widgets.get(name="repos_path")
unique_repo_branch_id = dbutils.widgets.get(name="unique_repo_branch_id")
unique_repo_branch_id_schema = dbutils.widgets.get(name="unique_repo_branch_id_schema")

# env = "dev"
# repos_path = "/Repos/DATTA-RETAIL_SITE_PNL/DATTA-MOBILITY-EH"
# unique_repo_branch_id = ""
# unique_repo_branch_id_schema = "gsap"

common_conf = CommonConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/common/common_conf.json")
env_conf = EnvConfig.from_file(f"/Workspace/{repos_path.strip('/')}/conf/{env}/conf.json")

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

spn = AzureSPN(tenant_id, spn_client_id, spn_client_secret)

configure_spark_to_use_spn_to_write_to_adls_gen2(env_conf.storage_account, spn)

# COMMAND ----------

uc_catalog_name = base_config.get_uc_catalog_name()
print("uc_catalog_name : ",uc_catalog_name)

uc_euh_schema = base_config.get_uc_euh_schema()
print("uc_euh_schema : ", uc_euh_schema)

uc_eh_schema = base_config.get_uc_eh_schema()
print("uc_eh_schema : ", uc_eh_schema)

uc_eh_md_schema = uc_eh_schema.replace("-capability", "-masterdata")
print("uc_eh_md_schema : ",uc_eh_md_schema)

tbl_owner_grp = base_config.get_tbl_owner_grp()
print("tbl_owner_grp : ",tbl_owner_grp)

tbl_read_grp = base_config.get_tbl_read_grp()
print("tbl_read_grp : ",tbl_owner_grp)

# COMMAND ----------

spark.sql(f"""GRANT USE SCHEMA ON SCHEMA `{uc_catalog_name}`.`{uc_eh_schema}` TO `{tbl_owner_grp}`""")
print("USE SCHEMA granted to ", tbl_owner_grp)

# COMMAND ----------

# assignViewPermission: This function assigns Permission to the view created
def assignViewPermission(catalog,schema,view_name,tbl_owner):
    spark.sql(f"""GRANT ALL PRIVILEGES ON VIEW `{catalog}`.`{schema}`.{view_name} TO `{tbl_owner}`""")
    print("All privileges access given to tbl owner", tbl_owner)

# COMMAND ----------

# DBTITLE 1,CC View
#View creation
cc_view_name = "vw_dim_currency_conversion"
use_catalog = "USE CATALOG `" + uc_catalog_name + "`"
spark.sql(use_catalog)
spark.sql(f"""DROP VIEW IF EXISTS `{uc_catalog_name}`.`{uc_eh_schema}`.{cc_view_name}""")
spark.sql(f"""CREATE OR REPLACE VIEW `{uc_catalog_name}`.`{uc_eh_schema}`.{cc_view_name} 
AS SELECT * 
FROM 
      `{uc_catalog_name}`.`{uc_eh_schema}`.dim_currency_conversion""")

assignViewPermission(uc_catalog_name, uc_eh_schema, cc_view_name, tbl_owner_grp)          
print("Retail view dropped and recreated in the schema")

# COMMAND ----------

# DBTITLE 1,MM UOM View
mat_uom_view_name = "vw_md_mm_material_uom"
use_catalog = "USE CATALOG `" + uc_catalog_name + "`"
spark.sql(use_catalog)
spark.sql(f"""DROP VIEW IF EXISTS `{uc_catalog_name}`.`{uc_eh_schema}`.{mat_uom_view_name}""")
spark.sql(f"""CREATE OR REPLACE VIEW `{uc_catalog_name}`.`{uc_eh_schema}`.{mat_uom_view_name} 
AS SELECT * 
FROM 
      `{uc_catalog_name}`.`{uc_eh_schema}`.md_mm_material_uom """)

assignViewPermission(uc_catalog_name, uc_eh_schema, mat_uom_view_name, tbl_owner_grp)          
print("Retail view dropped and recreated in the schema")

# COMMAND ----------

# DBTITLE 1,SI UOM View
si_uom_view_name = "vw_md_mm_si_uom"
use_catalog = "USE CATALOG `" + uc_catalog_name + "`"
spark.sql(use_catalog)
spark.sql(f"""DROP VIEW IF EXISTS `{uc_catalog_name}`.`{uc_eh_schema}`.{si_uom_view_name}""")
spark.sql(f"""CREATE OR REPLACE VIEW `{uc_catalog_name}`.`{uc_eh_schema}`.{si_uom_view_name} 
AS SELECT * 
FROM 
      `{uc_catalog_name}`.`{uc_eh_schema}`.md_mm_si_uom """)

assignViewPermission(uc_catalog_name, uc_eh_schema, si_uom_view_name, tbl_owner_grp)          
print("Retail view dropped and recreated in the schema")

# COMMAND ----------

# DBTITLE 1,Time Dimension View
time_dim_view_name = "vw_m_time_dimension"
use_catalog = "USE CATALOG `" + uc_catalog_name + "`"
spark.sql(use_catalog)
spark.sql(f"""DROP VIEW IF EXISTS `{uc_catalog_name}`.`{uc_eh_md_schema}`.{time_dim_view_name}""")
spark.sql(f"""CREATE OR REPLACE VIEW `{uc_catalog_name}`.`{uc_eh_md_schema}`.{time_dim_view_name} 
AS SELECT * 
FROM 
      `{uc_catalog_name}`.`{uc_eh_md_schema}`.m_time_dimension """)

assignViewPermission(uc_catalog_name, uc_eh_md_schema, time_dim_view_name, tbl_owner_grp)          
print("Retail view dropped and recreated in the schema")
