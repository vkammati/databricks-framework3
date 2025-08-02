# Databricks notebook source
pip install msal

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
# repos_path = "/Repos/DATTA-WS-MIGRATION/DATTA-CAPABILITIES-EH"
# unique_repo_branch_id = ""
# unique_repo_branch_id_schema = ""


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

uc_eh_schema = base_config.get_uc_eh_schema()
print("uc_eh_schema : ", uc_eh_schema)

uc_eh_schema = uc_eh_schema.replace("-gsap", "")
print("uc_eh_schema : ", uc_eh_schema)

tbl_owner_grp = base_config.get_tbl_owner_grp()
print("tbl_owner_grp : ",tbl_owner_grp)

tbl_read_grp = base_config.get_tbl_read_grp()
print("tbl_read_grp : ",tbl_owner_grp)

security_object_aad_group_name = env_conf.security_object_aad_group
print("security object aad group name: ", security_object_aad_group_name)

security_end_user_aad_group_name = env_conf.security_end_user_aad_group
print("security end user aad group name: ", security_end_user_aad_group_name)

security_functional_readers_aad_group = env_conf.security_functional_readers_aad_group
print("security functional readers aad group name: ", security_functional_readers_aad_group)

security_security_readers_aad_group = env_conf.security_security_readers_aad_group
print("security security readers aad group name: ", security_security_readers_aad_group)


# COMMAND ----------

# assignViewPermission: This function assigns Permission to the view created
def assignViewPermission(catalog,schema,view_name,tbl_owner, security_end_user_aad_group, security_object_aad_group, security_functional_readers_aad_group):
    spark.sql(f"""GRANT ALL PRIVILEGES ON VIEW `{catalog}`.`{schema}`.{view_name} TO `{tbl_owner}`""")
    print("All privileges access given to tbl owner", tbl_owner)
    spark.sql(f"""GRANT SELECT ON VIEW `{catalog}`.`{schema}`.{view_name} TO `{security_end_user_aad_group}`""")
    print("Reader access granted to ", security_end_user_aad_group)
    spark.sql(f"""GRANT SELECT ON VIEW `{catalog}`.`{schema}`.{view_name} TO `{security_functional_readers_aad_group}`""")
    print("Reader access granted to ", security_functional_readers_aad_group)
    # spark.sql(f"""ALTER VIEW `{catalog}`.`{schema}`.{view_name} owner to `{security_object_aad_group}`""")
    # print("Table Owner is assigned to ", security_object_aad_group)

# COMMAND ----------

uom_view_name = "vw_vbrk_vbrp_mara_too6_base_uom"
use_catalog = "USE CATALOG `" + uc_catalog_name + "`"
spark.sql(use_catalog)
spark.sql(f"""CREATE OR REPLACE VIEW `{uc_catalog_name}`.`{uc_eh_schema}`.{uom_view_name} 
AS
select BILLING_DOCUMENT,COMPANY_CODE,BILLING_ITEM,ACT_BL_QTY,UOM_UNIT,MATERIAL_NO,CREATED_DATE,CLIENT_NO,SCALE_QTY,BILLING_QTY,REQ_QTY_SKU,NETWT,GROSS_WT,BASE_UOM,YEAR_FIELD,MARM_BASE_ACT_BL_QTY,MARM_CONVERSION_FACTOR,T006_BASE_ACT_BL_QTY,T006_CONVERSION_FACTOR from (
select tt.BILLING_DOCUMENT,tt.COMPANY_CODE,tt.BILLING_ITEM,tt.ACT_BL_QTY,tt.UOM_UNIT,tt.MATERIAL_NO,tt.CREATED_DATE,tt.CLIENT_NO,tt.SCALE_QTY,tt.BILLING_QTY,tt.REQ_QTY_SKU,tt.NETWT,tt.GROSS_WT,tt.BASE_UOM,tt.YEAR_FIELD,tt.ACT_BL_QTY*marm.CONVERSION_FACTOR as MARM_BASE_ACT_BL_QTY,tt.ACT_BL_QTY*too6.CONVERSION_FACTOR as T006_BASE_ACT_BL_QTY, marm.CONVERSION_FACTOR as MARM_CONVERSION_FACTOR,too6.CONVERSION_FACTOR as T006_CONVERSION_FACTOR
from `{uc_catalog_name}`.`{uc_eh_schema}`.FACT_SD_SALES_HEADER_LINE_JOIN tt
left JOIN (select * from
    `{uc_catalog_name}`.`{uc_eh_schema}`.MD_MM_MATERIAL_UOM_JOIN ) marm
ON  tt.MATERIAL_NO = marm.MATERIAL_NO AND tt.UOM_UNIT = marm.SOURCE_UOM AND tt.BASE_UOM=marm.TARGET_UOM
left JOIN (select * from
    `{uc_catalog_name}`.`{uc_eh_schema}`.MD_MM_UOM_TEXT_JOIN ) too6
ON  tt.UOM_UNIT = too6.SOURCE_UOM AND tt.BASE_UOM=too6.TARGET_UOM)""")
      
assignViewPermission(uc_catalog_name,uc_eh_schema,uom_view_name,tbl_owner_grp, security_end_user_aad_group_name, security_object_aad_group_name, security_functional_readers_aad_group)
    
print("uom view recreated in the schema")

# COMMAND ----------

target_uom_view_name = "target_uom"
use_catalog = "USE CATALOG `" + uc_catalog_name + "`"
spark.sql(use_catalog)
spark.sql(f"""CREATE OR REPLACE VIEW `{uc_catalog_name}`.`{uc_eh_schema}`.{target_uom_view_name} as 
select TARGET_UOM from `{uc_catalog_name}`.`{uc_eh_schema}`.MD_MM_MATERIAL_UOM_JOIN
union
select TARGET_UOM from `{uc_catalog_name}`.`{uc_eh_schema}`.MD_MM_UOM_TEXT_JOIN""")
      
assignViewPermission(uc_catalog_name,uc_eh_schema,target_uom_view_name,tbl_owner_grp, security_end_user_aad_group_name, security_object_aad_group_name, security_functional_readers_aad_group)
    
print("uom view recreated in the schema")

# COMMAND ----------

uom_t006_dynamic_view_name = "vw_uom_t006_dynamic"
use_catalog = "USE CATALOG `" + uc_catalog_name + "`"
spark.sql(use_catalog)
spark.sql(f"""CREATE OR REPLACE VIEW `{uc_catalog_name}`.`{uc_eh_schema}`.{uom_t006_dynamic_view_name} as 
                select CLIENT_NO, 
                MATERIAL_NO as MATERIAL_NUMBER,
                ACT_BL_QTY as QUANTITY, 
                BASE_UOM,
                UOM_UNIT as SOURCE_UOM,
                UOM_UNIT_TO as QUANTITY_TON, 
                UOM_UNIT_LB as QUANTITY_LB  
            from `{uc_catalog_name}`.`{uc_eh_schema}`.uom_T006_dynamic"""
            )
      
assignViewPermission(uc_catalog_name,uc_eh_schema,uom_t006_dynamic_view_name,tbl_owner_grp, security_end_user_aad_group_name, security_object_aad_group_name, security_functional_readers_aad_group)
    
print("uom t00b dynamic view recreated in the schema")
