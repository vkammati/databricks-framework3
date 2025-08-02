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
def assignViewPermission(catalog,schema,function_name,tbl_owner, security_end_user_aad_group, security_functional_readers_aad_group):
    spark.sql(f"""GRANT ALL PRIVILEGES ON FUNCTION `{catalog}`.`{schema}`.{function_name} TO `{tbl_owner}`""")
    print("All privileges access given to tbl owner", tbl_owner)
    spark.sql(f"""GRANT EXECUTE ON FUNCTION `{catalog}`.`{schema}`.{function_name} TO `{security_end_user_aad_group}`""")
    print("Execute access granted to ", security_end_user_aad_group)
    spark.sql(f"""GRANT EXECUTE ON FUNCTION `{catalog}`.`{schema}`.{function_name} TO `{security_functional_readers_aad_group}`""")
    print("Execute access granted to ", security_functional_readers_aad_group)

# COMMAND ----------

uom_function_name = "uom_dynamic_otf_mvp2"
use_catalog = "USE CATALOG `" + uc_catalog_name + "`"
spark.sql(use_catalog)
# spark.sql(f"""DROP FUNCTION IF EXISTS `{uc_catalog_name}`.`{uc_eh_schema}`.{uom_function_name}""")
spark.sql(f"""CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_eh_schema}`.{uom_function_name}(
    CompanyCode STRING,
    UomMode STRING,
    BaseUOM STRING,
    SourceUOM STRING,
    TargetUOM STRING, 
    MaterialNumber STRING, 
    YearField STRING, 
    ConversionMode STRING
)
RETURNS TABLE (
    CLIENT_NO STRING, 
    MATERIAL_NO STRING, 
    ACT_BL_QTY DECIMAL(13,3), 
    SCALE_QTY DECIMAL(13,3), 
    BILLING_QTY DECIMAL(13,3),
    REQ_QTY_SKU DECIMAL(13,3), 
    NETWT DECIMAL(15,3), 
    GROSS_WT DECIMAL(15,3),
    BASE_UOM STRING,
    UOM_UNIT STRING, 
    BILLING_DOCUMENT STRING,
    BILLING_ITEM STRING, 
    COMPANY_CODE STRING, 
    YEAR_FIELD STRING,
    MARM_BASE_ACT_BL_QTY DECIMAL(13,3),
    T006_BASE_ACT_BL_QTY DECIMAL(13,3),
    MARM_CONVERSION_FACTOR DECIMAL(30,4),
    T006_CONVERSION_FACTOR DECIMAL(30,4),
    ADJ_ACT_BL_QTY DECIMAL(13,3),
    BASE_CONVERSION_FACTOR DECIMAL(30,4),
    BASE_QUANTITY DECIMAL(13,3),
    SOURCE_UOM STRING,
    TARGET_UOM STRING,
    CONVERSION_FACTOR DECIMAL(30,4),
    ACT_BL_QTY_TARGETUOM DECIMAL(13,3),
    CONVERSION_STATUS STRING
)
RETURN 
    SELECT 
    CLIENT_NO,MATERIAL_NO,ACT_BL_QTY,SCALE_QTY,BILLING_QTY,REQ_QTY_SKU,NETWT,GROSS_WT,BASE_UOM,UOM_UNIT,BILLING_DOCUMENT,BILLING_ITEM,COMPANY_CODE,YEAR_FIELD,MARM_BASE_ACT_BL_QTY,T006_BASE_ACT_BL_QTY, MARM_CONVERSION_FACTOR,T006_CONVERSION_FACTOR,
    case when isnull(ADJ_ACT_BL_QTY) then ACT_BL_QTY else ADJ_ACT_BL_QTY end as ADJ_ACT_BL_QTY,
    BASE_CONVERSION_FACTOR,BASE_QUANTITY,SOURCE_UOM,TARGET_UOM, CONVERSION_FACTOR,
    case when ISNULL(ACT_BL_QTY_TARGETUOM) then ADJ_ACT_BL_QTY else ACT_BL_QTY_TARGETUOM end as ACT_BL_QTY_TARGETUOM,
    CONVERSION_STATUS from (
    SELECT 
    CLIENT_NO,MATERIAL_NO,ACT_BL_QTY,SCALE_QTY,BILLING_QTY,REQ_QTY_SKU,NETWT,GROSS_WT,BASE_UOM,UOM_UNIT,BILLING_DOCUMENT,BILLING_ITEM,COMPANY_CODE,YEAR_FIELD,MARM_BASE_ACT_BL_QTY,T006_BASE_ACT_BL_QTY, MARM_CONVERSION_FACTOR,T006_CONVERSION_FACTOR,ADJ_ACT_BL_QTY,BASE_CONVERSION_FACTOR,BASE_QUANTITY,SOURCE_UOM,
    CASE
        WHEN uom_dynamic_otf_mvp2.UomMode = "Source" THEN case when ISNULL(TARGET_UOM) THEN  uom_dynamic_otf_mvp2.TargetUOM ELSE TARGET_UOM END
        WHEN uom_dynamic_otf_mvp2.UomMode = "Base" THEN case when ISNULL(TARGET_UOM) THEN  uom_dynamic_otf_mvp2.TargetUOM ELSE TARGET_UOM END
      END AS TARGET_UOM, 
    CONVERSION_FACTOR,ADJ_ACT_BL_QTY * CONVERSION_FACTOR as ACT_BL_QTY_TARGETUOM,
    CASE
        WHEN uom_dynamic_otf_mvp2.UomMode = "Source" THEN case when ISNULL(CONVERSION_FACTOR) THEN CONCAT("No conversion factor from ", UOM_UNIT , " to ", uom_dynamic_otf_mvp2.TargetUOM, " found for Material " , MATERIAL_NO) ELSE "Conversion successful" END
        WHEN  uom_dynamic_otf_mvp2.UomMode = "Base" THEN case when ISNULL(CONVERSION_FACTOR) THEN CONCAT("No conversion factor from ", BASE_UOM , " to ", uom_dynamic_otf_mvp2.TargetUOM, " found for Material " , MATERIAL_NO) ELSE "Conversion successful" END
      END AS CONVERSION_STATUS from (
    SELECT
    tt.CLIENT_NO, tt.MATERIAL_NO, tt.ACT_BL_QTY, tt.SCALE_QTY, tt.BILLING_QTY, tt.REQ_QTY_SKU, tt.NETWT, tt.GROSS_WT,tt.BASE_UOM, tt.UOM_UNIT, tt.BILLING_DOCUMENT, tt.BILLING_ITEM, tt.COMPANY_CODE, tt.YEAR_FIELD,tt.MARM_BASE_ACT_BL_QTY,tt.T006_BASE_ACT_BL_QTY,tt.MARM_CONVERSION_FACTOR,tt.T006_CONVERSION_FACTOR,
    CASE
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARM" and uom_dynamic_otf_mvp2.UomMode = "Source" THEN tt.ACT_BL_QTY
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARM" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN case when ISNULL(tt.MARM_BASE_ACT_BL_QTY) THEN tt.ACT_BL_QTY ELSE tt.MARM_BASE_ACT_BL_QTY END
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006" and uom_dynamic_otf_mvp2.UomMode = "Source" THEN tt.ACT_BL_QTY
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN case when ISNULL(tt.T006_BASE_ACT_BL_QTY) THEN tt.ACT_BL_QTY ELSE tt.T006_BASE_ACT_BL_QTY END
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARMT006" and uom_dynamic_otf_mvp2.UomMode = "Source" THEN tt.ACT_BL_QTY
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARMT006" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN case when ISNULL(tt.MARM_BASE_ACT_BL_QTY) THEN  tt.T006_BASE_ACT_BL_QTY ELSE tt.MARM_BASE_ACT_BL_QTY END
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006MARM" and uom_dynamic_otf_mvp2.UomMode = "Source" THEN tt.ACT_BL_QTY
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006MARM" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN case when ISNULL(tt.T006_BASE_ACT_BL_QTY) THEN  tt.MARM_BASE_ACT_BL_QTY ELSE tt.T006_BASE_ACT_BL_QTY END
      END AS ADJ_ACT_BL_QTY,
    CASE
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARM" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN MARM_CONVERSION_FACTOR
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN T006_CONVERSION_FACTOR
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARMT006" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN case when ISNULL(MARM_CONVERSION_FACTOR) THEN  T006_CONVERSION_FACTOR ELSE MARM_CONVERSION_FACTOR END
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006MARM" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN case when ISNULL(T006_CONVERSION_FACTOR) THEN  MARM_CONVERSION_FACTOR ELSE T006_CONVERSION_FACTOR END
      END AS BASE_CONVERSION_FACTOR,
    CASE
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARM" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN MARM_BASE_ACT_BL_QTY
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN T006_BASE_ACT_BL_QTY
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARMT006" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN case when ISNULL(MARM_BASE_ACT_BL_QTY) THEN  T006_BASE_ACT_BL_QTY ELSE MARM_BASE_ACT_BL_QTY END
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006MARM" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN case when ISNULL(T006_BASE_ACT_BL_QTY) THEN  MARM_BASE_ACT_BL_QTY ELSE T006_BASE_ACT_BL_QTY END
      END AS BASE_QUANTITY,
    CASE
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARM" and uom_dynamic_otf_mvp2.UomMode = "Source" THEN marm.SOURCE_UOM
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARM" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN marm_base.SOURCE_UOM
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006" and uom_dynamic_otf_mvp2.UomMode = "Source" THEN too6.SOURCE_UOM
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN too6_base.SOURCE_UOM
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARMT006" and uom_dynamic_otf_mvp2.UomMode = "Source" THEN case when ISNULL(marm.SOURCE_UOM) THEN  too6.SOURCE_UOM ELSE marm.SOURCE_UOM END
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARMT006" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN case when ISNULL(marm_base.SOURCE_UOM) THEN  too6_base.SOURCE_UOM ELSE marm_base.SOURCE_UOM END
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006MARM" and uom_dynamic_otf_mvp2.UomMode = "Source" THEN case when ISNULL(too6.SOURCE_UOM) THEN  marm.SOURCE_UOM ELSE too6.SOURCE_UOM END
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006MARM" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN case when ISNULL(too6_base.SOURCE_UOM) THEN  marm_base.SOURCE_UOM ELSE too6_base.SOURCE_UOM END
      END AS SOURCE_UOM,
    CASE
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARM" and uom_dynamic_otf_mvp2.UomMode = "Source" THEN marm.TARGET_UOM
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARM" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN marm_base.TARGET_UOM
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006" and uom_dynamic_otf_mvp2.UomMode = "Source" THEN too6.TARGET_UOM
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN too6_base.TARGET_UOM
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARMT006" and uom_dynamic_otf_mvp2.UomMode = "Source" THEN case when ISNULL(marm.TARGET_UOM) THEN  too6.TARGET_UOM ELSE marm.TARGET_UOM END
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARMT006" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN case when ISNULL(marm_base.TARGET_UOM) THEN  too6_base.TARGET_UOM ELSE marm_base.TARGET_UOM END
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006MARM" and uom_dynamic_otf_mvp2.UomMode = "Source" THEN case when ISNULL(too6.TARGET_UOM) THEN  marm.TARGET_UOM ELSE too6.TARGET_UOM END
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006MARM" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN case when ISNULL(too6_base.TARGET_UOM) THEN  marm_base.TARGET_UOM ELSE too6_base.TARGET_UOM END
      END AS TARGET_UOM,
    CASE
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARM" and uom_dynamic_otf_mvp2.UomMode = "Source" THEN marm.CONVERSION_FACTOR
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARM" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN marm_base.CONVERSION_FACTOR
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006" and uom_dynamic_otf_mvp2.UomMode = "Source" THEN too6.CONVERSION_FACTOR
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN too6_base.CONVERSION_FACTOR
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARMT006" and uom_dynamic_otf_mvp2.UomMode = "Source" THEN case when ISNULL(marm.CONVERSION_FACTOR) THEN  too6.CONVERSION_FACTOR ELSE marm.CONVERSION_FACTOR END
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "MARMT006" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN case when ISNULL(marm_base.CONVERSION_FACTOR) THEN  too6_base.CONVERSION_FACTOR ELSE marm_base.CONVERSION_FACTOR END
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006MARM" and uom_dynamic_otf_mvp2.UomMode = "Source" THEN case when ISNULL(too6.CONVERSION_FACTOR) THEN  marm.CONVERSION_FACTOR ELSE too6.CONVERSION_FACTOR END
        WHEN uom_dynamic_otf_mvp2.ConversionMode = "T006MARM" and uom_dynamic_otf_mvp2.UomMode = "Base" THEN case when ISNULL(too6_base.CONVERSION_FACTOR) THEN  marm_base.CONVERSION_FACTOR ELSE too6_base.CONVERSION_FACTOR END
      END AS CONVERSION_FACTOR
FROM
    (select * from `{uc_catalog_name}`.`{uc_eh_schema}`.vw_vbrk_vbrp_mara_too6_base_uom where (array_contains(SPLIT(uom_dynamic_otf_mvp2.SourceUOM,',') ,UOM_UNIT) OR (uom_dynamic_otf_mvp2.SourceUOM='ALL')) or (array_contains(SPLIT(uom_dynamic_otf_mvp2.BaseUOM,',') ,BASE_UOM) OR (uom_dynamic_otf_mvp2.BaseUOM='ALL'))) tt
left JOIN (select * from
    `{uc_catalog_name}`.`{uc_eh_schema}`.MD_MM_MATERIAL_UOM_JOIN where TARGET_UOM = uom_dynamic_otf_mvp2.TargetUOM ) marm
ON  tt.MATERIAL_NO = marm.MATERIAL_NO AND  tt.UOM_UNIT = marm.SOURCE_UOM
left JOIN (select * from
    `{uc_catalog_name}`.`{uc_eh_schema}`.MD_MM_MATERIAL_UOM_JOIN where TARGET_UOM = uom_dynamic_otf_mvp2.TargetUOM ) marm_base
ON  tt.MATERIAL_NO = marm_base.MATERIAL_NO AND tt.BASE_UOM = marm_base.SOURCE_UOM
left JOIN (select * from
    `{uc_catalog_name}`.`{uc_eh_schema}`.MD_MM_UOM_TEXT_JOIN where TARGET_UOM = uom_dynamic_otf_mvp2.TargetUOM ) too6
ON  tt.UOM_UNIT = too6.SOURCE_UOM
left JOIN (select * from
    `{uc_catalog_name}`.`{uc_eh_schema}`.MD_MM_UOM_TEXT_JOIN where TARGET_UOM = uom_dynamic_otf_mvp2.TargetUOM ) too6_base
ON  tt.BASE_UOM = too6_base.SOURCE_UOM
WHERE
    (array_contains(SPLIT(uom_dynamic_otf_mvp2.CompanyCode,',') ,tt.COMPANY_CODE) OR (uom_dynamic_otf_mvp2.CompanyCode='ALL')) AND
    (array_contains(SPLIT(uom_dynamic_otf_mvp2.MaterialNumber,',') ,tt.MATERIAL_NO) OR (uom_dynamic_otf_mvp2.MaterialNumber='ALL')) AND
    (array_contains(SPLIT(uom_dynamic_otf_mvp2.YearField,',') ,tt.YEAR_FIELD) OR (uom_dynamic_otf_mvp2.YearField='ALL'))))""")
      
assignViewPermission(uc_catalog_name,uc_eh_schema,uom_function_name,tbl_owner_grp, security_end_user_aad_group_name, security_functional_readers_aad_group)
    
print("uom function uom_dynamic_otf_mvp2 dropped and recreated in the schema")

# COMMAND ----------

fixed_uom_to_mvp2_function_name = "fixed_uom_to_mvp2"
use_catalog = "USE CATALOG `" + uc_catalog_name + "`"
spark.sql(use_catalog)
# spark.sql(f"""DROP FUNCTION IF EXISTS `{uc_catalog_name}`.`{uc_eh_schema}`.{fixed_uom_to_mvp2_function_name}""")
spark.sql(f"""CREATE OR REPLACE FUNCTION `{uc_catalog_name}`.`{uc_eh_schema}`.{fixed_uom_to_mvp2_function_name}(
    CompanyCode STRING,
    UomMode STRING,
    BaseUOM STRING,
    SourceUOM STRING, 
    MaterialNumber STRING, 
    YearField STRING, 
    ConversionMode STRING
)
RETURNS TABLE (
    CLIENT_NO STRING, 
    MATERIAL_NO STRING, 
    ACT_BL_QTY DECIMAL(13,3), 
    SCALE_QTY DECIMAL(13,3), 
    BILLING_QTY DECIMAL(13,3),
    REQ_QTY_SKU DECIMAL(13,3), 
    NETWT DECIMAL(15,3), 
    GROSS_WT DECIMAL(15,3),
    BASE_UOM STRING,
    UOM_UNIT STRING, 
    BILLING_DOCUMENT STRING,
    BILLING_ITEM STRING, 
    COMPANY_CODE STRING, 
    YEAR_FIELD STRING,
    MARM_BASE_ACT_BL_QTY DECIMAL(13,3),
    T006_BASE_ACT_BL_QTY DECIMAL(13,3),
    MARM_CONVERSION_FACTOR DECIMAL(30,4),
    T006_CONVERSION_FACTOR DECIMAL(30,4),
    ADJ_ACT_BL_QTY DECIMAL(13,3),
    BASE_CONVERSION_FACTOR DECIMAL(30,4),
    BASE_QUANTITY DECIMAL(13,3),
    SOURCE_UOM STRING,
    TARGET_UOM STRING,
    CONVERSION_FACTOR DECIMAL(30,4),
    ACT_BL_QTY_TARGETUOM DECIMAL(13,3),
    CONVERSION_STATUS STRING
)
RETURN 
    SELECT 
    CLIENT_NO,MATERIAL_NO,ACT_BL_QTY,SCALE_QTY,BILLING_QTY,REQ_QTY_SKU,NETWT,GROSS_WT,BASE_UOM,UOM_UNIT,BILLING_DOCUMENT,BILLING_ITEM,COMPANY_CODE,YEAR_FIELD,MARM_BASE_ACT_BL_QTY,T006_BASE_ACT_BL_QTY, MARM_CONVERSION_FACTOR,T006_CONVERSION_FACTOR,
    case when isnull(ADJ_ACT_BL_QTY) then ACT_BL_QTY else ADJ_ACT_BL_QTY end as ADJ_ACT_BL_QTY,
    BASE_CONVERSION_FACTOR,BASE_QUANTITY,SOURCE_UOM,TARGET_UOM, CONVERSION_FACTOR,
    case when ISNULL(ACT_BL_QTY_TARGETUOM) then ADJ_ACT_BL_QTY else ACT_BL_QTY_TARGETUOM end as ACT_BL_QTY_TARGETUOM,
    CONVERSION_STATUS from (
    SELECT 
    CLIENT_NO,MATERIAL_NO,ACT_BL_QTY,SCALE_QTY,BILLING_QTY,REQ_QTY_SKU,NETWT,GROSS_WT,BASE_UOM,UOM_UNIT,BILLING_DOCUMENT,BILLING_ITEM,COMPANY_CODE,YEAR_FIELD,MARM_BASE_ACT_BL_QTY,T006_BASE_ACT_BL_QTY, MARM_CONVERSION_FACTOR,T006_CONVERSION_FACTOR,ADJ_ACT_BL_QTY,BASE_CONVERSION_FACTOR,BASE_QUANTITY,SOURCE_UOM,TARGET_UOM, 
    CONVERSION_FACTOR,ADJ_ACT_BL_QTY * CONVERSION_FACTOR as ACT_BL_QTY_TARGETUOM,
    CASE
        WHEN fixed_uom_to_mvp2.UomMode = "Source" THEN case when ISNULL(CONVERSION_FACTOR) THEN CONCAT("No conversion factor from ", UOM_UNIT , " to TO found for Material " , MATERIAL_NO) ELSE "Conversion successful" END
        WHEN  fixed_uom_to_mvp2.UomMode = "Base" THEN case when ISNULL(CONVERSION_FACTOR) THEN CONCAT("No conversion factor from ", BASE_UOM , " to TO found for Material " , MATERIAL_NO) ELSE "Conversion successful" END
      END AS CONVERSION_STATUS from (
    SELECT
    tt.CLIENT_NO, tt.MATERIAL_NO, tt.ACT_BL_QTY, tt.SCALE_QTY, tt.BILLING_QTY, tt.REQ_QTY_SKU, tt.NETWT, tt.GROSS_WT,tt.BASE_UOM, tt.UOM_UNIT, tt.BILLING_DOCUMENT, tt.BILLING_ITEM, tt.COMPANY_CODE, tt.YEAR_FIELD,tt.MARM_BASE_ACT_BL_QTY,tt.T006_BASE_ACT_BL_QTY,tt.MARM_CONVERSION_FACTOR,tt.T006_CONVERSION_FACTOR,
    CASE
        WHEN fixed_uom_to_mvp2.ConversionMode = "MARM" and fixed_uom_to_mvp2.UomMode = "Source" THEN tt.ACT_BL_QTY
        WHEN fixed_uom_to_mvp2.ConversionMode = "MARM" and fixed_uom_to_mvp2.UomMode = "Base" THEN case when ISNULL(tt.MARM_BASE_ACT_BL_QTY) THEN tt.ACT_BL_QTY ELSE tt.MARM_BASE_ACT_BL_QTY END
        WHEN fixed_uom_to_mvp2.ConversionMode = "T006" and fixed_uom_to_mvp2.UomMode = "Source" THEN tt.ACT_BL_QTY
        WHEN fixed_uom_to_mvp2.ConversionMode = "T006" and fixed_uom_to_mvp2.UomMode = "Base" THEN case when ISNULL(tt.T006_BASE_ACT_BL_QTY) THEN tt.ACT_BL_QTY ELSE tt.T006_BASE_ACT_BL_QTY END
        WHEN fixed_uom_to_mvp2.ConversionMode = "MARMT006" and fixed_uom_to_mvp2.UomMode = "Source" THEN tt.ACT_BL_QTY
        WHEN fixed_uom_to_mvp2.ConversionMode = "MARMT006" and fixed_uom_to_mvp2.UomMode = "Base" THEN case when ISNULL(tt.MARM_BASE_ACT_BL_QTY) THEN  tt.T006_BASE_ACT_BL_QTY ELSE tt.MARM_BASE_ACT_BL_QTY END
        WHEN fixed_uom_to_mvp2.ConversionMode = "T006MARM" and fixed_uom_to_mvp2.UomMode = "Source" THEN tt.ACT_BL_QTY
        WHEN fixed_uom_to_mvp2.ConversionMode = "T006MARM" and fixed_uom_to_mvp2.UomMode = "Base" THEN case when ISNULL(tt.T006_BASE_ACT_BL_QTY) THEN  tt.MARM_BASE_ACT_BL_QTY ELSE tt.T006_BASE_ACT_BL_QTY END
      END AS ADJ_ACT_BL_QTY,
    CASE
        WHEN fixed_uom_to_mvp2.ConversionMode = "MARM" and fixed_uom_to_mvp2.UomMode = "Base" THEN MARM_CONVERSION_FACTOR
        WHEN fixed_uom_to_mvp2.ConversionMode = "T006" and fixed_uom_to_mvp2.UomMode = "Base" THEN T006_CONVERSION_FACTOR
        WHEN fixed_uom_to_mvp2.ConversionMode = "MARMT006" and fixed_uom_to_mvp2.UomMode = "Base" THEN case when ISNULL(MARM_CONVERSION_FACTOR) THEN  T006_CONVERSION_FACTOR ELSE MARM_CONVERSION_FACTOR END
        WHEN fixed_uom_to_mvp2.ConversionMode = "T006MARM" and fixed_uom_to_mvp2.UomMode = "Base" THEN case when ISNULL(T006_CONVERSION_FACTOR) THEN  MARM_CONVERSION_FACTOR ELSE T006_CONVERSION_FACTOR END
      END AS BASE_CONVERSION_FACTOR,
    CASE
        WHEN fixed_uom_to_mvp2.ConversionMode = "MARM" and fixed_uom_to_mvp2.UomMode = "Base" THEN MARM_BASE_ACT_BL_QTY
        WHEN fixed_uom_to_mvp2.ConversionMode = "T006" and fixed_uom_to_mvp2.UomMode = "Base" THEN T006_BASE_ACT_BL_QTY
        WHEN fixed_uom_to_mvp2.ConversionMode = "MARMT006" and fixed_uom_to_mvp2.UomMode = "Base" THEN case when ISNULL(MARM_BASE_ACT_BL_QTY) THEN  T006_BASE_ACT_BL_QTY ELSE MARM_BASE_ACT_BL_QTY END
        WHEN fixed_uom_to_mvp2.ConversionMode = "T006MARM" and fixed_uom_to_mvp2.UomMode = "Base" THEN case when ISNULL(T006_BASE_ACT_BL_QTY) THEN  MARM_BASE_ACT_BL_QTY ELSE T006_BASE_ACT_BL_QTY END
      END AS BASE_QUANTITY,
    CASE
        WHEN fixed_uom_to_mvp2.ConversionMode = "MARM" and fixed_uom_to_mvp2.UomMode = "Source" THEN marm.SOURCE_UOM
        WHEN fixed_uom_to_mvp2.ConversionMode = "MARM" and fixed_uom_to_mvp2.UomMode = "Base" THEN marm_base.SOURCE_UOM
        WHEN fixed_uom_to_mvp2.ConversionMode = "T006" and fixed_uom_to_mvp2.UomMode = "Source" THEN too6.SOURCE_UOM
        WHEN fixed_uom_to_mvp2.ConversionMode = "T006" and fixed_uom_to_mvp2.UomMode = "Base" THEN too6_base.SOURCE_UOM
        WHEN fixed_uom_to_mvp2.ConversionMode = "MARMT006" and fixed_uom_to_mvp2.UomMode = "Source" THEN case when ISNULL(marm.SOURCE_UOM) THEN  too6.SOURCE_UOM ELSE marm.SOURCE_UOM END
        WHEN fixed_uom_to_mvp2.ConversionMode = "MARMT006" and fixed_uom_to_mvp2.UomMode = "Base" THEN case when ISNULL(marm_base.SOURCE_UOM) THEN  too6_base.SOURCE_UOM ELSE marm_base.SOURCE_UOM END
        WHEN fixed_uom_to_mvp2.ConversionMode = "T006MARM" and fixed_uom_to_mvp2.UomMode = "Source" THEN case when ISNULL(too6.SOURCE_UOM) THEN  marm.SOURCE_UOM ELSE too6.SOURCE_UOM END
        WHEN fixed_uom_to_mvp2.ConversionMode = "T006MARM" and fixed_uom_to_mvp2.UomMode = "Base" THEN case when ISNULL(too6_base.SOURCE_UOM) THEN  marm_base.SOURCE_UOM ELSE too6_base.SOURCE_UOM END
      END AS SOURCE_UOM,"TO" as TARGET_UOM,
    CASE
        WHEN fixed_uom_to_mvp2.ConversionMode = "MARM" and fixed_uom_to_mvp2.UomMode = "Source" THEN marm.CONVERSION_FACTOR
        WHEN fixed_uom_to_mvp2.ConversionMode = "MARM" and fixed_uom_to_mvp2.UomMode = "Base" THEN marm_base.CONVERSION_FACTOR
        WHEN fixed_uom_to_mvp2.ConversionMode = "T006" and fixed_uom_to_mvp2.UomMode = "Source" THEN too6.CONVERSION_FACTOR
        WHEN fixed_uom_to_mvp2.ConversionMode = "T006" and fixed_uom_to_mvp2.UomMode = "Base" THEN too6_base.CONVERSION_FACTOR
        WHEN fixed_uom_to_mvp2.ConversionMode = "MARMT006" and fixed_uom_to_mvp2.UomMode = "Source" THEN case when ISNULL(marm.CONVERSION_FACTOR) THEN  too6.CONVERSION_FACTOR ELSE marm.CONVERSION_FACTOR END
        WHEN fixed_uom_to_mvp2.ConversionMode = "MARMT006" and fixed_uom_to_mvp2.UomMode = "Base" THEN case when ISNULL(marm_base.CONVERSION_FACTOR) THEN  too6_base.CONVERSION_FACTOR ELSE marm_base.CONVERSION_FACTOR END
        WHEN fixed_uom_to_mvp2.ConversionMode = "T006MARM" and fixed_uom_to_mvp2.UomMode = "Source" THEN case when ISNULL(too6.CONVERSION_FACTOR) THEN  marm.CONVERSION_FACTOR ELSE too6.CONVERSION_FACTOR END
        WHEN fixed_uom_to_mvp2.ConversionMode = "T006MARM" and fixed_uom_to_mvp2.UomMode = "Base" THEN case when ISNULL(too6_base.CONVERSION_FACTOR) THEN  marm_base.CONVERSION_FACTOR ELSE too6_base.CONVERSION_FACTOR END
      END AS CONVERSION_FACTOR
FROM
    (select * from `{uc_catalog_name}`.`{uc_eh_schema}`.vw_vbrk_vbrp_mara_too6_base_uom where (array_contains(SPLIT(fixed_uom_to_mvp2.SourceUOM,',') ,UOM_UNIT) OR (fixed_uom_to_mvp2.SourceUOM='ALL')) or (array_contains(SPLIT(fixed_uom_to_mvp2.BaseUOM,',') ,BASE_UOM) OR (fixed_uom_to_mvp2.BaseUOM='ALL'))) tt
left JOIN (select * from
    `{uc_catalog_name}`.`{uc_eh_schema}`.MD_MM_MATERIAL_UOM_JOIN where TARGET_UOM = "TO" ) marm
ON  tt.MATERIAL_NO = marm.MATERIAL_NO AND  tt.UOM_UNIT = marm.SOURCE_UOM
left JOIN (select * from
    `{uc_catalog_name}`.`{uc_eh_schema}`.MD_MM_MATERIAL_UOM_JOIN where TARGET_UOM = "TO" ) marm_base
ON  tt.MATERIAL_NO = marm_base.MATERIAL_NO AND tt.BASE_UOM = marm_base.SOURCE_UOM
left JOIN (select * from
    `{uc_catalog_name}`.`{uc_eh_schema}`.MD_MM_UOM_TEXT_JOIN where TARGET_UOM = "TO" ) too6
ON  tt.UOM_UNIT = too6.SOURCE_UOM
left JOIN (select * from
    `{uc_catalog_name}`.`{uc_eh_schema}`.MD_MM_UOM_TEXT_JOIN where TARGET_UOM = "TO" ) too6_base
ON  tt.BASE_UOM = too6_base.SOURCE_UOM
WHERE
    (array_contains(SPLIT(fixed_uom_to_mvp2.CompanyCode,',') ,tt.COMPANY_CODE) OR (fixed_uom_to_mvp2.CompanyCode='ALL')) AND
    (array_contains(SPLIT(fixed_uom_to_mvp2.MaterialNumber,',') ,tt.MATERIAL_NO) OR (fixed_uom_to_mvp2.MaterialNumber='ALL')) AND
    (array_contains(SPLIT(fixed_uom_to_mvp2.YearField,',') ,tt.YEAR_FIELD) OR (fixed_uom_to_mvp2.YearField='ALL'))))""")
      
assignViewPermission(uc_catalog_name,uc_eh_schema,fixed_uom_to_mvp2_function_name,tbl_owner_grp, security_end_user_aad_group_name, security_functional_readers_aad_group)
    
print("uom function fixed_uom_to_mvp2 dropped and recreated in the schema")
