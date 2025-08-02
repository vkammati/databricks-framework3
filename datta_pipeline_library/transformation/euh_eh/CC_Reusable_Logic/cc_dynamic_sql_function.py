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

spark.sql(f"""GRANT USE SCHEMA ON SCHEMA `{uc_catalog_name}`.`{uc_eh_schema}` TO `{security_end_user_aad_group_name}`""")
print("USE SCHEMA granted to ", security_end_user_aad_group_name)

# COMMAND ----------

# assignViewPermission: This function assigns Permission to the view created
def assignViewPermission(catalog,schema,view_name,tbl_owner, security_end_user_aad_group, security_object_aad_group, security_functional_readers_aad_group):
    spark.sql(f"""GRANT ALL PRIVILEGES ON VIEW `{catalog}`.`{schema}`.{view_name} TO `{tbl_owner}`""")
    print("All privileges access given to tbl owner", tbl_owner)
    spark.sql(f"""GRANT SELECT ON VIEW `{catalog}`.`{schema}`.{view_name} TO `{security_end_user_aad_group}`""")
    print("Reader access granted to ", security_end_user_aad_group)
    spark.sql(f"""GRANT SELECT ON VIEW `{catalog}`.`{schema}`.{view_name} TO `{security_functional_readers_aad_group}`""")
    print("Reader access granted to ", security_functional_readers_aad_group)
    spark.sql(f"""ALTER VIEW `{catalog}`.`{schema}`.{view_name} owner to `{security_object_aad_group}`""")
    print("Table Owner is assigned to ", security_object_aad_group)

# COMMAND ----------

# DBTITLE 1,cc_dynamic_sql_function
cc_dynamic_sql_function_name = 'cc_dynamic_sql_function'
security_function_name = "sf_gen_rls_security"
security_schema = "eh-ds-security"
use_catalog = "USE CATALOG `"+uc_catalog_name+"`"
spark.sql(use_catalog)
spark.sql(f"""DROP VIEW IF EXISTS `{uc_catalog_name}`.`{uc_eh_schema}`.cc_dynamic_sql_function""")
spark.sql(f"""CREATE OR REPLACE FUNCTION  `{uc_catalog_name}`.`{uc_eh_schema}`.cc_dynamic_sql_function
            (EXCHANGERATETYPE STRING, TARGETCURRENCY STRING, COMPANYCODE STRING, TRANSACTIONALSTARTDATE  STRING, TRANSACTIONALENDDATE STRING, EXCHANGERATEDAILY  STRING) 
            RETURNS TABLE 
            (
                FISCAL_YEAR STRING,	COMPANY_CODE STRING,NETVALUE_BILLINGITEM DECIMAL(30, 6),MATERIAL_NUMBER STRING,PLANT STRING,BILLING_DOCUMENT_DATE STRING,DOCUMENT_CURRENCY STRING,PROFIT_CENTER STRING,CLIENT STRING,DATEID STRING, EXCHANGERATE_TYPE STRING,EXCHANGERATE_DESCRIPTION STRING,FROMCURRENCY STRING,FROMCURRENCY_DESCRIPTION STRING,TOCURRENCY STRING,TOCURRENCY_DESCRIPTION STRING,EXCHANGERATE_EFFECTIVEDATE STRING,CURRENCY_VALIDDATE STRING,CURRENCY_VALID_DATEFORMAT DATE, EXCHANGERATE DECIMAL(30, 5),FROMCURRENCY_RATIO DECIMAL(15, 0),TOCURRENCY_RATIO DECIMAL(15, 0), NETVALUE_BILLINGITEM_TARGETCURRENCY DECIMAL(38, 6),CONVERSION_STATUS STRING
            ) 
            RETURN   
            WITH CC_CTE(
            SELECT 
                CT.TCURR_CLIENT
                ,CT.DATEID 
                ,CT.EXCHANGERATE_TYPE
                ,CT.EXCHANGERATE_DESCRIPTION
                ,CT.FROMCURRENCY
                ,CT.FROMCURRENCY_DESCRIPTION
                ,CT.TOCURRENCY
                ,CT.TOCURRENCY_DESCRIPTION
                ,CT.EXCHANGERATE_EFFECTIVEDATE
                ,CT.CURRENCY_VALIDDATE
                ,CT.CURRENCY_VALID_DATEFORMAT
                ,CT.EXCHANGERATE
                ,CT.FROMCURRENCY_RATIO
                ,CT.TOCURRENCY_RATIO
            FROM  `{uc_catalog_name}`.`{uc_eh_schema}`.vw_tcurr_cc_global_curated CT
            LEFT JOIN  `{uc_catalog_name}`.`{uc_euh_schema}`.DATE DCC 
            ON CT.DATEID=DCC.FULL_DATE 
            WHERE CT.EXCHANGERATE_TYPE =cc_dynamic_sql_function.EXCHANGERATETYPE
            AND  CT.TOCURRENCY= cc_dynamic_sql_function.TARGETCURRENCY
            AND  (CASE WHEN  cc_dynamic_sql_function.EXCHANGERATEDAILY = '9999-01-01'
            THEN (DCC.FULL_DATE BETWEEN cc_dynamic_sql_function.TRANSACTIONALSTARTDATE AND cc_dynamic_sql_function.TRANSACTIONALENDDATE)
            ELSE DCC.FULL_DATE = cc_dynamic_sql_function.EXCHANGERATEDAILY END)
            )
            ,
            CC_CTE1 AS (
            SELECT 
                CT.TCURR_CLIENT AS  S_TCURR_CLIENT
                ,CT.DATEID  AS S_DATEID
                ,CT.EXCHANGERATE_TYPE AS S_EXCHANGERATE_TYPE
                ,CT.EXCHANGERATE_DESCRIPTION AS S_EXCHANGERATE_DESCRIPTION
                ,CT.FROMCURRENCY AS S_FROMCURRENCY
                ,CT.FROMCURRENCY_DESCRIPTION AS S_FROMCURRENCY_DESCRIPTION
                ,CT.TOCURRENCY AS S_TOCURRENCY
                ,CT.TOCURRENCY_DESCRIPTION AS S_TOCURRENCY_DESCRIPTION
                ,CT.EXCHANGERATE_EFFECTIVEDATE AS S_EXCHANGERATE_EFFECTIVEDATE
                ,CT.CURRENCY_VALIDDATE AS S_CURRENCY_VALIDDATE
                ,CT.CURRENCY_VALID_DATEFORMAT AS S_CURRENCY_VALID_DATEFORMAT
                ,CT.EXCHANGERATE AS S_EXCHANGERATE
                ,CT.FROMCURRENCY_RATIO AS S_FROMCURRENCY_RATIO
                ,CT.TOCURRENCY_RATIO AS S_TOCURRENCY_RATIO
            FROM  `{uc_catalog_name}`.`{uc_eh_schema}`.vw_tcurr_cc_global_curated CT
            LEFT JOIN  `{uc_catalog_name}`.`{uc_euh_schema}`.DATE DCC 
            ON CT.DATEID=DCC.FULL_DATE
            WHERE CT.EXCHANGERATE_TYPE =cc_dynamic_sql_function.EXCHANGERATETYPE
            AND  CT.TOCURRENCY= cc_dynamic_sql_function.TARGETCURRENCY
            AND  (DCC.FULL_DATE BETWEEN cc_dynamic_sql_function.TRANSACTIONALSTARTDATE AND cc_dynamic_sql_function.TRANSACTIONALENDDATE)
            )
            ,FINAL_CC_CTE(SELECT 
                DISTINCT
                COALESCE(S_TCURR_CLIENT,TCURR_CLIENT) AS CLIENT
                ,CASE WHEN  cc_dynamic_sql_function.EXCHANGERATEDAILY = '9999-01-01'
                    THEN DATEID ELSE S_DATEID 
                    END AS DATEID
                ,COALESCE(S_EXCHANGERATE_TYPE,EXCHANGERATE_TYPE) AS EXCHANGERATE_TYPE
                ,COALESCE(S_EXCHANGERATE_DESCRIPTION,EXCHANGERATE_DESCRIPTION) AS EXCHANGERATE_DESCRIPTION
            ,FROMCURRENCY AS FROM_CURRENCY
                ,FROMCURRENCY_DESCRIPTION AS FROM_CURRENCY_DESCRIPTION
                ,TOCURRENCY AS TO_CURRENCY
                ,TOCURRENCY_DESCRIPTION AS TO_CURRENCY_DESCRIPTION
                ,EXCHANGERATE_EFFECTIVEDATE AS EXCHANGERATE_EFFECTIVEDATE
                ,CURRENCY_VALIDDATE AS CURRENCY_VALIDDATE
                ,CURRENCY_VALID_DATEFORMAT AS CURRENCY_VALID_DATEFORMAT
                ,EXCHANGERATE AS EXCHANGERATE
                ,FROMCURRENCY_RATIO AS FROM_CURRENCY_RATIO
                ,TOCURRENCY_RATIO AS TO_CURRENCY_RATIO
            FROM CC_CTE 
            FULL JOIN CC_CTE1 
            ON CC_CTE.TCURR_CLIENT= CC_CTE1.S_TCURR_CLIENT
            ),

            TRANSACTIONAL_CTE AS(
            SELECT 
                T.FISCAL_YEAR
                ,T.COMPANY_CODE
                ,SUM(T.NETVALUE_BILLINGITEM) AS NETVALUE_BILLINGITEM
                ,T.MATERIAL_NUMBER
                ,T.PLANT
                ,T.BILLING_DOCUMENT_DATE
                ,T.DOCUMENT_CURRENCY
                ,T.PROFIT_CENTER
            FROM  `{uc_catalog_name}`.`{uc_eh_schema}`.vw_vbrp_cc_cob_curated T
            LEFT JOIN  `{uc_catalog_name}`.`{uc_euh_schema}`.DATE DTT 
            ON T.BILLING_DOCUMENT_DATE=DTT.FULL_DATE 
            WHERE (ARRAY_CONTAINS(SPLIT(cc_dynamic_sql_function.COMPANYCODE,',') ,T.COMPANY_CODE) OR (cc_dynamic_sql_function.COMPANYCODE='ALL')) 
            AND (DTT.FULL_DATE between TRANSACTIONALSTARTDATE and TRANSACTIONALENDDATE)
            GROUP BY T.FISCAL_YEAR
                ,T.COMPANY_CODE
                ,T.MATERIAL_NUMBER
                ,T.PLANT
                ,T.RECORD_CREATEDDATE
                ,T.BILLING_DOCUMENT_DATE
                ,T.DOCUMENT_CURRENCY
                ,T.PROFIT_CENTER
            )

            SELECT 
            TT.FISCAL_YEAR
            ,TT.COMPANY_CODE
            ,TT.NETVALUE_BILLINGITEM
            ,TT.MATERIAL_NUMBER
            ,TT.PLANT
            ,TT.BILLING_DOCUMENT_DATE
            ,TT.DOCUMENT_CURRENCY
            ,TT.PROFIT_CENTER
            ,CC.CLIENT
            ,CC.DATEID
            ,CC.EXCHANGERATE_TYPE
            ,CC.EXCHANGERATE_DESCRIPTION
            ,CC.FROM_CURRENCY
            ,CC.FROM_CURRENCY_DESCRIPTION
            ,CC.TO_CURRENCY
            ,CC.TO_CURRENCY_DESCRIPTION
            ,CC.EXCHANGERATE_EFFECTIVEDATE
            ,CC.CURRENCY_VALIDDATE
            ,CC.CURRENCY_VALID_DATEFORMAT
            ,CC.EXCHANGERATE
            ,CC.FROM_CURRENCY_RATIO
            ,CC.TO_CURRENCY_RATIO
            ,TT.NETVALUE_BILLINGITEM * CC.EXCHANGERATE AS NETVALUE_BILLINGITEM_TARGETCURRENCY,
            CASE
                WHEN ISNULL(CC.EXCHANGERATE) THEN CONCAT("No conversion from ",TT.DOCUMENT_CURRENCY," to ",cc_dynamic_sql_function.TARGETCURRENCY," for the Exchange Rate type ",cc_dynamic_sql_function.EXCHANGERATETYPE," on ",TT.BILLING_DOCUMENT_DATE) ELSE "Conversion successful"
            END AS CONVERSION_STATUS
            FROM TRANSACTIONAL_CTE TT
            LEFT OUTER JOIN FINAL_CC_CTE CC 
            ON TT.DOCUMENT_CURRENCY =CC.FROM_CURRENCY 
            AND  TT.BILLING_DOCUMENT_DATE = CC.DATEID""")

assignViewPermission(uc_catalog_name,uc_eh_schema,cc_dynamic_sql_function_name,tbl_owner_grp, security_end_user_aad_group_name, security_object_aad_group_name, security_functional_readers_aad_group)          
print("uom_dynamic_sql_function dropped and recreated in the schema")
