# Databricks notebook source
# MAGIC %pip install --upgrade pip

# COMMAND ----------

# MAGIC %pip install -r ../../requirements.txt

# COMMAND ----------

import os
os.environ["pipeline"] = "databricks"

# COMMAND ----------

from datta_pipeline_library.core.base_config import (
    BaseConfig,
    CommonConfig,
    EnvConfig,
    GreatExpectationsConfig,
)
from datta_pipeline_library.helpers.adls import configure_spark_to_use_spn_to_write_to_adls_gen2
from datta_pipeline_library.helpers.spn import AzureSPN

# COMMAND ----------

# DBTITLE 1,Parameters
unique_repo_branch_id = dbutils.widgets.get(name="unique_repo_branch_id")
unique_repo_branch_id_schema = dbutils.widgets.get(name="unique_repo_branch_id_schema")
repos_path = dbutils.widgets.get(name="repos_path")
env = dbutils.widgets.get(name="env")


common_conf = CommonConfig.from_file("../../conf/common/common_conf.json")
env_conf = EnvConfig.from_file(f"../../conf/{env}/conf.json")

kv = env_conf.kv_key

# values from key vault
tenant_id = dbutils.secrets.get(scope=kv, key="AZ-AS-SPN-DATTA-TENANT-ID")
spn_client_id = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_id_key)
spn_client_secret = dbutils.secrets.get(scope=kv, key=env_conf.spn_client_secret_key)

spn = AzureSPN(tenant_id, spn_client_id, spn_client_secret)

#gx_config = GreatExpectationsConfig(azure_conn_str)

#base_config = BaseConfig.from_confs(env_conf, common_conf, gx_config)
base_config = BaseConfig.from_confs(env_conf, common_conf)
base_config.set_unique_id(unique_repo_branch_id)
base_config.set_unique_id_schema(unique_repo_branch_id_schema)

# COMMAND ----------

configure_spark_to_use_spn_to_write_to_adls_gen2(env_conf.storage_account, spn)

# COMMAND ----------

# DBTITLE 1,Configuration
uc_catalog = base_config.get_uc_catalog_name()
euh_schema = base_config.get_uc_euh_schema()
eh_schema = base_config.get_uc_eh_schema()

euh_folder_path = base_config.get_euh_folder_path()
eh_folder_path = base_config.get_eh_folder_path()

eh_md_schema = eh_schema.replace("-capability", "-masterdata")
print("eh_md_schema : ",eh_md_schema)

eh_md_folder_path=eh_folder_path.replace("/capability_dev", "/masterdata_dev")
print("eh_md_folder_path: ", eh_md_folder_path)

print("unity catalog: ", uc_catalog)
print("euh schema: ", euh_schema)
print("eh schema: ", eh_schema)
print("euh folder path: ", euh_folder_path)
print("eh folder path: ", eh_folder_path)
print("eh md schema: ", eh_md_schema)
print("eh md folder path: ", eh_md_folder_path)

# COMMAND ----------

# MAGIC %md ## Delete UC schemas and tables

# COMMAND ----------

euh_table_list = ["tcurr_bw","tcurv_bw","tcurw_bw","tcurf_bw",'tcurr','tcurv','tcurt','tcurw','tcurn','tcurf','tcurc','tcurr_pattern','date','t006','t006a','mara','marm']
if env == "dev":
    for table_name in euh_table_list:
        spark.sql(f"DROP TABLE IF EXISTS `{uc_catalog}`.`{euh_schema}`.table_name")

# COMMAND ----------

eh_md_table_list = ['m_time_dimension']
if env == "dev":
    for table_name in eh_md_table_list:
        spark.sql(f"DROP TABLE IF EXISTS `{uc_catalog}`.`{eh_md_schema}`.table_name")

# COMMAND ----------

eh_table_list = ["dim_currency_conversion"]
if env == "dev":
    for table_name in eh_table_list:
        spark.sql(f"DROP TABLE IF EXISTS `{uc_catalog}`.`{eh_schema}`.table_name")

# COMMAND ----------

if env == "dev":
    spark.sql(f"DROP SCHEMA IF EXISTS `{uc_catalog}`.`{euh_schema}` CASCADE")

# COMMAND ----------

if env == "dev":
    spark.sql(f"DROP SCHEMA IF EXISTS `{uc_catalog}`.`{eh_schema}` CASCADE")

# COMMAND ----------

if env == "dev":
    spark.sql(f"DROP SCHEMA IF EXISTS `{uc_catalog}`.`{eh_md_schema}` CASCADE")

# COMMAND ----------

# MAGIC %md ## Delete ADLS folders

# COMMAND ----------

# DBTITLE 1,Delete EUH folder
if env == "dev":
    dbutils.fs.rm(euh_folder_path, recurse=True)

# COMMAND ----------

# DBTITLE 1,Delete MD EH folder
if env == "dev":
    dbutils.fs.rm(eh_md_folder_path, recurse=True)

# COMMAND ----------

# DBTITLE 1,Delete EH folder
if env == "dev":
    dbutils.fs.rm(eh_folder_path, recurse=True)

# COMMAND ----------

# MAGIC %md ## Insert data into euh layer tables

# COMMAND ----------

# DBTITLE 1,Create UC schemas
if env == "dev":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{uc_catalog}`.`{euh_schema}`")

# COMMAND ----------

if env == "dev":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{uc_catalog}`.`{eh_md_schema}`")

# COMMAND ----------

if env == "dev":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{uc_catalog}`.`{eh_schema}`")

# COMMAND ----------

# DBTITLE 1,TCURR_BW create statement

if env == "dev": 
    tcurr_bw_table_path = euh_folder_path + "/tcurr_bw"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.tcurr_bw
    (MANDT string,
        KURST string,
        FCURR string,
        TCURR string,
        GDATU string,
        UKURS decimal(38,18),
        FFACT decimal(38,18),
        TFACT decimal(38,18),
        ingested_at timestamp,
        AEDATTM timestamp,
        OPFLAG string)
    USING delta
    LOCATION '{tcurr_bw_table_path}' """)
    

if env == "dev":
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.tcurr_bw
    VALUES('110','M','USD','PAB','79869170',1.000000000000000000,0E-18,0E-18,current_timestamp(),null,null),('110','M','USD','PAB','79869171',1.000000000000000000,0E-18,0E-18,current_timestamp(),null,null),('110','M','USD','PAB','79869172',1.000000000000000000,0E-18,0E-18,current_timestamp(),null,null),('110','M','USD','PAB','79869169',1.000000000000000000,0E-18,0E-18,current_timestamp(),null,null),('110','M','USD','PAB','79869097',1.000000000000000000,0E-18,0E-18,current_timestamp(),null,null),('110','M','USD','PAB','79869096',1.000000000000000000,0E-18,0E-18,current_timestamp(),null,null),('110','M','USD','PAB','79869095',1.000000000000000000,0E-18,0E-18,current_timestamp(),null,null),('110','M','USD','PAB','79869176',1.000000000000000000,0E-18,0E-18,current_timestamp(),null,null),('110','M','USD','PAB','79869177',1.000000000000000000,0E-18,0E-18,current_timestamp(),null,null),('110','M','USD','PAB','79869178',1.000000000000000000,0E-18,0E-18,current_timestamp(),null,null)
""")
     

# COMMAND ----------

# DBTITLE 1,TCURV_BW create statement

if env == "dev": 
    tcurv_bw_table_path = euh_folder_path + "/tcurv_bw"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.tcurv_bw
    (MANDT string,
        KURST string,
        XINVR string,
        BWAER string,
        XBWRL string,
        GKUZU string,
        BKUZU string,
        XFIXD string,
        XEURO string,
        ingested_at timestamp,
        AEDATTM timestamp,
        OPFLAG string)
    USING delta
    LOCATION '{tcurv_bw_table_path}' """)
    

if env == "dev":
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.tcurv_bw
    VALUES('110','ZLE1','','','','','','','',current_timestamp(),null,null),('110','ZM1','','','','','','','',current_timestamp(),null,null),('110','ZM2','','','','','','','',current_timestamp(),null,null),('110','ZQ1','','','','','','','',current_timestamp(),null,null),('110','ZY1','','','','','','','',current_timestamp(),null,null),('110','ZP1','','','','','','','',current_timestamp(),null,null)
""")
     

# COMMAND ----------

# DBTITLE 1,TCURW_BW create statement

if env == "dev": 
    tcurw_bw_table_path = euh_folder_path + "/tcurw_bw"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.tcurw_bw
    (MANDT string,
        SPRAS string,
        KURST string,
        CURVW string,
        ingested_at timestamp,
        AEDATTM timestamp,
        OPFLAG string)
    USING delta
    LOCATION '{tcurw_bw_table_path}' """)
    

if env == "dev":
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.tcurw_bw
    VALUES('110','E','ZLE1','LE Exchange Rate',current_timestamp(),null,null),('110','E','ZM1','Month End Exchange Rate (Treasury)',current_timestamp(),null,null),('110','E','ZM2','Month Average Exchange Rate (Treasury)',current_timestamp(),null,null),('110','E','ZQ1','Quarter Average Exchange Rate (Treasury)',current_timestamp(),null,null),('110','E','ZY1','Year Average Exchange Rate (Treasury)',current_timestamp(),null,null),('110','E','ZP1','Plan Exchange Rate : for BPC',current_timestamp(),null,null)
""")
     

# COMMAND ----------

# DBTITLE 1,TCURF_BW create statement

if env == "dev": 
    tcurf_bw_table_path = euh_folder_path + "/tcurf_bw"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.tcurf_bw
    (MANDT string,
        KURST string,
        FCURR string,
        TCURR string,
        GDATU string,
        FFACT decimal(38,18),
        TFACT decimal(38,18),
        ABWCT string,
        ABWGA string,
        ingested_at timestamp,
        AEDATTM timestamp,
        OPFLAG string)
    USING delta
    LOCATION '{tcurf_bw_table_path}' """)
    

if env == "dev":
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.tcurf_bw
    VALUES('110','ZLE1','USD','CZK','81999898',1.000000000000000000,1.000000000000000000,'','00000000',current_timestamp(),null,null),('110','ZM1','USD','CZK','81999898',1.000000000000000000,1.000000000000000000,'','00000000',current_timestamp(),null,null),('110','ZM2','USD','CZK','81999898',1.000000000000000000,1.000000000000000000,'','00000000',current_timestamp(),null,null),('110','ZP1','USD','CZK','81999898',1.000000000000000000,1.000000000000000000,'','00000000',current_timestamp(),null,null),('110','ZQ1','USD','CZK','81999898',1.000000000000000000,1.000000000000000000,'','00000000',current_timestamp(),null,null),('110','ZY1','USD','CZK','81999898',1.000000000000000000,1.000000000000000000,'','00000000',current_timestamp(),null,null),('110','ZLE1','MOP','HKD','81999898',1.000000000000000000,1.000000000000000000,'','00000000',current_timestamp(),null,null),('110','ZLE1','USD','BND','81999898',1.000000000000000000,1.000000000000000000,'','00000000',current_timestamp(),null,null),('110','ZLE1','USD','HKD','81999898',1.000000000000000000,1.000000000000000000,'','00000000',current_timestamp(),null,null),('110','ZM1','MOP','HKD','81999898',1.000000000000000000,1.000000000000000000,'','00000000',current_timestamp(),null,null)
""")
     

# COMMAND ----------

# DBTITLE 1,TCURR create statement
if env == "dev":
    tcurr_table_path = euh_folder_path + "/tcurr"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.tcurr(        
    MANDT string,
    KURST string,
    FCURR string,
    TCURR string,
    GDATU string,
    UKURS decimal(9,5),
    FFACT decimal(9,0),
    TFACT decimal(9,0),
    yrmono string,
    ingested_at	timestamp,
    AEDATTM	timestamp,
    OPFLAG string)
    USING delta
    LOCATION '{tcurr_table_path}'"""
    )

# COMMAND ----------

# DBTITLE 1,TCURV create statement
if env == "dev":
    tcurv_table_path = euh_folder_path + "/tcurv"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.tcurv(        
    MANDT string,
    KURST string,
    XINVR string,
    BWAER string,
    XBWRL string,
    GKUZU string,
    BKUZU string,
    XFIXD string,
    XEURO string,
    yrmono string,
    ingested_at	timestamp,
    AEDATTM	timestamp,
    OPFLAG string)
    USING delta
    LOCATION '{tcurv_table_path}'"""
    )

# COMMAND ----------

# DBTITLE 1,TCURT create statement
if env == "dev":
    tcurt_table_path = euh_folder_path + "/tcurt"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.tcurt(        
    MANDT string,
    SPRAS string,
    WAERS string,
    LTEXT string,
    KTEXT string,
    LAST_ACTION_CD string,
    LAST_DTM timestamp,
    SYSTEM_ID string,
    yrmono string,
    ingested_at	timestamp,
    AEDATTM	timestamp,
    OPFLAG string)
    USING delta
    LOCATION '{tcurt_table_path}'"""
    )

# COMMAND ----------

# DBTITLE 1,TCURW create statement
if env == "dev":
    tcurw_table_path = euh_folder_path + "/tcurw"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.tcurw(        
    MANDT string,
    SPRAS string,
    KURST string,
    CURVW string,
    yrmono string,
    ingested_at	timestamp,
    AEDATTM	timestamp,
    OPFLAG string)
    USING delta
    LOCATION '{tcurw_table_path}'"""
    )

# COMMAND ----------

# DBTITLE 1,TCURN create statement
if env == "dev":
    tcurn_table_path = euh_folder_path + "/tcurn"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.tcurn(        
    MANDT string,
    FCURR string,
    TCURR string,
    GDATU string,
    NOTATION string,
    yrmono string,
    ingested_at	timestamp,
    AEDATTM	timestamp,
    OPFLAG string)
    USING delta
    LOCATION '{tcurn_table_path}'"""
    )

# COMMAND ----------

# DBTITLE 1,TCURF create statement
if env == "dev":
    tcurf_table_path = euh_folder_path + "/tcurf"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.tcurf(        
    MANDT string,
    KURST string,
    FCURR string,
    TCURR string,
    GDATU string,
    FFACT decimal(9,0),
    TFACT decimal(9,0),
    ABWCT string,
    ABWGA date,
    yrmono string,
    ingested_at	timestamp,
    AEDATTM	timestamp,
    OPFLAG string)
    USING delta
    LOCATION '{tcurf_table_path}'"""
    )

# COMMAND ----------

# DBTITLE 1,TCURC create statement
if env == "dev":
    tcurc_table_path = euh_folder_path + "/tcurc"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.tcurc(        
    MANDT string,
    WAERS string,
    ISOCD string,
    ALTWR string,
    GDATU date,
    XPRIMARY string,
    yrmono string,
    ingested_at	timestamp,
    AEDATTM	timestamp,
    OPFLAG string)
    USING delta
    LOCATION '{tcurc_table_path}'"""
    )

# COMMAND ----------

# DBTITLE 1,TCURR_PATTERN create statement
if env == "dev":
    tcurr_pattern_table_path = euh_folder_path + "/tcurr_pattern"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.tcurr_pattern(        
    MANDT string,
    KURST string,
    PATTERN	string,
    ingested_at	timestamp,
    AEDATTM	timestamp,
    OPFLAG string)
    USING delta
    LOCATION '{tcurr_pattern_table_path}'"""
    )

# COMMAND ----------

# DBTITLE 1,DATE create statement
if env == "dev":
    date_table_path = euh_folder_path + "/date"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.date(        
    DateId string,
    Date string,
    Full_Date string,
    Year string,
    FirstDateofYear string,
    LastDateofYear string,
    Quarter	string,
    FirstDateofCurrentQuarter string,
    LastDateofCurrentQuarter string,
    RecentlyClosedDateofQuarter	string,
    MonthofQuarter string,
    MonthYear string,
    MonthNumber	string,
    MonthName string,
    FirstDateofMonth string,
    LastDateofMonth	string,
    WeekofYear string,
    WeekofQuarter string,
    WeekofMonth	string,
    DayName	string,
    DayofWeek string,
    DayofMonth string,
    DayofYear string,
    ingested_at	timestamp,
    AEDATTM	timestamp,
    OPFLAG string)
    USING delta
    LOCATION '{date_table_path}'"""
    )

# COMMAND ----------

# DBTITLE 1,T006 create statement
if env == "dev":
    t006_table_path = euh_folder_path + "/t006"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.t006(        
    MANDT string,
    MSEHI string,
    KZEX3 string,
    KZEX6 string,
    ANDEC int,
    KZKEH string,
    KZWOB string,
    KZ1EH string,
    KZ2EH string,
    DIMID string,
    ZAEHL int,
    NENNR int,
    EXP10 int,
    ADDKO decimal(9,6),
    EXPON int,
    DECAN int,
    ISOCODE	string,
    PRIMARY	string,
    TEMP_VALUE double,
    TEMP_UNIT string,
    FAMUNIT	string,
    PRESS_VAL double,
    PRESS_UNIT string,
    ZMSEHI_ID string,
    yrmono string,
    ingested_at	timestamp,
    AEDATTM	timestamp,
    OPFLAG string)
    USING delta
    LOCATION '{t006_table_path}'"""
    )

# COMMAND ----------

# DBTITLE 1,T006A create statement
if env == "dev":
    t006a_table_path = euh_folder_path + "/t006a"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.t006a(        
    MANDT string,
    SPRAS string,
    MSEHI string,
    MSEH3 string,
    MSEH6 string,
    MSEHT string,
    MSEHL string,
    SYSTEM_ID string,
    LAST_ACTION_CD string,
    LAST_DTM timestamp,
    yrmono string,
    ingested_at	timestamp,
    AEDATTM	timestamp,
    OPFLAG string)
    USING delta
    LOCATION '{t006a_table_path}'"""
    )

# COMMAND ----------

# DBTITLE 1,MARA create statement
if env == "dev":
    mara_table_path = euh_folder_path + "/mara"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.mara(        
    MANDT string,
    MATNR string,
    ERSDA date,
    ERNAM string,
    LAEDA date,
    AENAM string,
    VPSTA string,
    PSTAT string,
    LVORM string,
    MTART string,
    MBRSH string,
    MATKL string,
    BISMT string,
    MEINS string,
    BSTME string,
    ZEINR string,
    ZEIAR string,
    ZEIVR string,
    ZEIFO string,
    AESZN string,
    BLATT string,
    BLANZ string,
    FERTH string,
    FORMT string,
    GROES string,
    WRKST string,
    NORMT string,
    LABOR string,
    EKWSL string,
    BRGEW decimal(13,3),
    NTGEW decimal(13,3),
    GEWEI string,
    VOLUM decimal(13,3),
    VOLEH string,
    BEHVO string,
    RAUBE string,
    TEMPB string,
    DISST string,
    TRAGR string,
    STOFF string,
    SPART string,
    KUNNR string,
    EANNR string,
    WESCH decimal(13,3),
    BWVOR string,
    BWSCL string,
    SAISO string,
    ETIAR string,
    ETIFO string,
    ENTAR string,
    EAN11 string,
    NUMTP string,
    LAENG decimal(13,3),
    BREIT decimal(13,3),
    HOEHE decimal(13,3),
    MEABM string,
    PRDHA string,
    AEKLK string,
    CADKZ string,
    QMPUR string,
    ERGEW decimal(13,3),
    ERGEI string,
    ERVOL decimal(13,3),
    ERVOE string,
    GEWTO decimal(3,1),
    VOLTO decimal(3,1),
    VABME string,
    KZREV string,
    KZKFG string,
    XCHPF string,
    VHART string,
    FUELG decimal(3,0),
    STFAK int,
    MAGRV string,
    BEGRU string,
    DATAB date,
    LIQDT date,
    SAISJ string,
    PLGTP string,
    MLGUT string,
    EXTWG string,
    SATNR string,
    ATTYP string,
    KZKUP string,
    KZNFM string,
    PMATA string,
    MSTAE string,
    MSTAV string,
    MSTDE date,
    MSTDV date,
    TAKLV string,
    RBNRM string,
    MHDRZ decimal(4,0),
    MHDHB decimal(4,0),
    MHDLP decimal(3,0),
    INHME string,
    INHAL decimal(13,3),
    VPREH decimal(5,0),
    ETIAG string,
    INHBR decimal(13,3),
    CMETH string,
    CUOBF string,
    KZUMW string,
    KOSCH string,
    SPROF string,
    NRFHG string,
    MFRPN string,
    MFRNR string,
    BMATN string,
    MPROF string,
    KZWSM string,
    SAITY string,
    PROFL string,
    IHIVI string,
    ILOOS string,
    SERLV string,
    KZGVH string,
    XGCHP string,
    KZEFF string,
    COMPL string,
    IPRKZ string,
    RDMHD string,
    PRZUS string,
    MTPOS_MARA string,
    BFLME string,
    MATFI string,
    CMREL string,
    BBTYP string,
    SLED_BBD string,
    GTIN_VARIANT string,
    GENNR string,
    RMATP string,
    GDS_RELEVANT string,
    WEORA string,
    HUTYP_DFLT string,
    PILFERABLE string,
    WHSTC string,
    WHMATGR string,
    HNDLCODE string,
    HAZMAT string,
    HUTYP string,
    TARE_VAR string,
    MAXC decimal(15,3),
    MAXC_TOL decimal(3,1),
    MAXL decimal(15,3),
    MAXB decimal(15,3),
    MAXH decimal(15,3),
    MAXDIM_UOM string,
    HERKL string,
    MFRGR string,
    QQTIME decimal(3,0),
    QQTIMEUOM string,
    QGRP string,
    SERIAL string,
    PS_SMARTFORM string,
    LOGUNIT string,
    CWQREL string,
    CWQPROC string,
    CWQTOLGR string,
    ADPROF string,
    IPMIPPRODUCT string,
    ALLOW_PMAT_IGNO string,
    MEDIUM string,
    COMMODITY string,
    ANIMAL_ORIGIN string,
    TEXTILE_COMP_IND string,
    SGT_CSGR string,
    SGT_COVSA string,
    SGT_STAT string,
    SGT_SCOPE string,
    SGT_REL string,
    FSH_MG_AT1 string,
    FSH_MG_AT2 string,
    FSH_MG_AT3 string,
    FSH_SEALV string,
    FSH_SEAIM string,
    FSH_SC_MID string,
    ANP string,
    PSM_CODE string,
    `/BEV1/LULEINH` string,
    `/BEV1/LULDEGRP` string,
    `/BEV1/NESTRUCCAT` string,
    `/CWM/XCWMAT` string,
    `/CWM/VALUM` string,
    `/CWM/TOLGR` string,
    `/CWM/TARA` string,
    `/CWM/TARUM` string,
    `/DSD/SL_TOLTYP` string,
    `/DSD/SV_CNT_GRP` string,
    `/DSD/VC_GROUP` string,
    `/VSO/R_TILT_IND` string,
    `/VSO/R_STACK_IND` string,
    `/VSO/R_BOT_IND` string,
    `/VSO/R_TOP_IND` string,
    `/VSO/R_STACK_NO` string,
    `/VSO/R_PAL_IND` string,
    `/VSO/R_PAL_OVR_D` decimal(13,3),
    `/VSO/R_PAL_OVR_W` decimal(13,3),
    `/VSO/R_PAL_B_HT` decimal(13,3),
    `/VSO/R_PAL_MIN_H` decimal(13,3),
    `/VSO/R_TOL_B_HT` decimal(13,3),
    `/VSO/R_NO_P_GVH` string,
    `/VSO/R_QUAN_UNIT` string,
    `/VSO/R_KZGVH_IND` string,
    PACKCODE string,
    DG_PACK_STATUS string,
    MCOND string,
    RETDELC string,
    LOGLEV_RETO string,
    NSNID string,
    ADSPC_SPC string,
    IMATN string,
    PICNUM string,
    BSTAT string,
    COLOR_ATINN string,
    SIZE1_ATINN string,
    SIZE2_ATINN string,
    COLOR string,
    SIZE1 string,
    SIZE2 string,
    FREE_CHAR string,
    CARE_CODE string,
    BRAND_ID string,
    FIBER_CODE1 string,
    FIBER_PART1 string,
    FIBER_CODE2 string,
    FIBER_PART2 string,
    FIBER_CODE3 string,
    FIBER_PART3 string,
    FIBER_CODE4 string,
    FIBER_PART4 string,
    FIBER_CODE5 string,
    FIBER_PART5 string,
    FASHGRD string,
    OIGROUPNAM string,
    OITRIND string,
    OIHMTXGR string,
    yrmono string,
    ingested_at	timestamp,
    AEDATTM	timestamp,
    OPFLAG string)
    USING delta
    LOCATION '{mara_table_path}'"""
    )

# COMMAND ----------

if env == "dev":
    marm_table_path = euh_folder_path + "/marm"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.marm(        
    MANDT string,
    MATNR string,
    MEINH string,
    UMREZ decimal(5,0),
    UMREN decimal(5,0),
    EANNR string,
    EAN11 string,
    NUMTP string,
    LAENG decimal(13,3),
    BREIT decimal(13,3),
    HOEHE decimal(13,3),
    MEABM string,
    VOLUM decimal(13,3),
    VOLEH string,
    BRGEW decimal(13,3),
    GEWEI string,
    MESUB string,
    ATINN string,
    MESRT string,
    XFHDW string,
    XBEWW string,
    KZWSO string,
    MSEHI string,
    BFLME_MARM string,
    GTIN_VARIANT string,
    NEST_FTR decimal(3,0),
    MAX_STACK int,
    CAPAUSE decimal(15,3),
    TY2TQ string,
    `/CWM/TY2TQ` string,
    yrmono string,
    ingested_at	timestamp,
    AEDATTM	timestamp,
    OPFLAG string)
    USING delta
    LOCATION '{marm_table_path}'"""
    )

# COMMAND ----------

# DBTITLE 1,Load sample data into EUH tables
if env == "dev":

    #TCURR
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.tcurr
    VALUES
    ('110','001M','PKR','CLP','79769069',279.26800,0,0,'TCURR_20231018_124041_1.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','001M','PKR','MUR','79769069',0.14907,0,0,'TCURR_20231018_124041_1.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','CH01','EUR','CHF','79769098',0.96732,0,0,'TCURR_20231019_164041_1.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','GL02','EUR','USD','79768896',1.07020,0,0,'TCURR_20231108_161440_1.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','GL02','EUR','USD','79768969',1.06050,0,0,'TCURR_20231108_161440_1.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','GL02','EUR','USD','79768974',1.05760,0,0,'TCURR_20231108_161440_1.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','GL02','EUR','USD','79768981',1.05650,0,0,'TCURR_20231024_124041_1.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','GL02','EUR','USD','79768994',1.05260,0,0,'TCURR_20231024_124041_1.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','GL02','EUR','USD','79768996',1.04690,0,0,'TCURR_20231024_124041_1.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','GL02','USD','EUR','79759890',0.91408,0,0,'TCURR_20240110_163340_1.parquet',current_timestamp(),current_timestamp(),'I')""")

      #TCURV
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.tcurv
    VALUES('110','ZLE1',null,null,null,null,null,null,null,'TCURV_20240223_125002_001.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','ZM1',null,null,null,null,null,null,null,'TCURV_20240223_125002_001.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','ZM2',null,null,null,null,null,null,null,'TCURV_20240223_125002_001.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','ZP1',null,null,null,null,null,null,null,'TCURV_20240223_125002_001.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','ZQ1',null,null,null,null,null,null,null,'TCURV_20240223_125002_001.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','ZY1',null,null,null,null,null,null,null,'TCURV_20240223_125002_001.parquet',current_timestamp(),current_timestamp(),'I')""")

    #TCURT
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.tcurt
    VALUES
    ('000','1','AED','United Arab Emirates Dirham','Dirham','C',current_timestamp(),'1001','TCURT_20231031_151053_001.parquet',current_timestamp(),null,null),
    ('000','1','AFA','Afghani (Old)','Afghani','C',current_timestamp(),'1001','TCURT_20231031_151053_001.parquet',current_timestamp(),null,null),
    ('000','1','AFN','Afghani','Afghani','C',current_timestamp(),'1001','TCURT_20231031_151053_001.parquet',current_timestamp(),null,null),
    ('000','1','ALL','Albanian Lek','Lek','C',current_timestamp(),'1001','TCURT_20231031_151053_001.parquet',current_timestamp(),null,null),
    ('000','1','AMD','Armenian Dram','Dram','C',current_timestamp(),'1001','TCURT_20231031_151053_001.parquet',current_timestamp(),null,null),
    ('000','1','ANG','West Indian Guilder','W.Ind.Guilder','C',current_timestamp(),'1001','TCURT_20231031_151053_001.parquet',current_timestamp(),null,null),
    ('000','1','AOA','Angolanische Kwanza','Kwansa','C',current_timestamp(),'1001','TCURT_20231031_151053_001.parquet',current_timestamp(),null,null),
    ('000','1','AON','Angolan New Kwanza (Old)','New Kwanza','C',current_timestamp(),'1001','TCURT_20231031_151053_001.parquet',current_timestamp(),null,null),
    ('000','1','AOR','Angolan Kwanza Reajustado (Old)','Kwanza Reajust.','C',current_timestamp(),'1001','TCURT_20231031_151053_001.parquet',current_timestamp(),null,null)""")

    #TCURW

    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.tcurw
    VALUES
    ('110','1','001M','Shell Group Month end rate','TCURW_20231214_132917_1.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','1','001Q','Shell Group Qtr end average rate','TCURW_20231214_132917_1.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','1','002M','Shell Group Month end average rate','TCURW_20231214_132917_1.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','1','100*','Reference value = group value','TCURW_20231214_132917_1.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','1','1001','Current exchange rate','TCURW_20231214_132917_1.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','1','1002','Average exchange rate','TCURW_20231214_132917_1.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','1','1003','Historical exchange rate','TCURW_20231214_132917_1.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','1','1004','Current exch. rate prior year','TCURW_20231214_132917_1.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','1','200*','Reference value = group value','TCURW_20231214_132917_1.parquet',current_timestamp(),current_timestamp(),'I'),
    ('110','1','2001','Current exchange rate','TCURW_20231214_132917_1.parquet',current_timestamp(),current_timestamp(),'I')""")

    #tcurn

    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.tcurn
    VALUES
    ('000','','','81999898','1','TCURN_20231031_152858_001.parquet',current_timestamp(),null,null),
    ('000','','AUD','81999898','2','TCURN_20231031_152858_001.parquet',current_timestamp(),null,null),
    ('000','','EUR','81999898','2','TCURN_20231031_152858_001.parquet',current_timestamp(),null,null),
    ('000','','GBP','81999898','2','TCURN_20231031_152858_001.parquet',current_timestamp(),null,null),
    ('000','','HUF','81999898','2','TCURN_20231031_152858_001.parquet',current_timestamp(),null,null),
    ('000','','IEP','81999898','2','TCURN_20231031_152858_001.parquet',current_timestamp(),null,null),
    ('000','','NZD','81999898','2','TCURN_20231031_152858_001.parquet',current_timestamp(),null,null),
    ('000','','USD','81999898','2','TCURN_20231031_152858_001.parquet',current_timestamp(),null,null),
    ('000','AUD','NZD','81999898','1','TCURN_20231031_152858_001.parquet',current_timestamp(),null,null),
    ('000','AUD','USD','81999898','1','TCURN_20231031_152858_001.parquet',current_timestamp(),null,null)""")
    
    #TCURF

    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.tcurf
    VALUES
    ('000','100*','EUR','USD','81999898',1,1,'',null,'TCURF_20231031_154822_001.parquet',current_timestamp(),null,null),
    ('000','100*','USD','EUR','81999898',1,1,'',null,'TCURF_20231031_154822_001.parquet',current_timestamp(),null,null),
    ('000','1001','EUR','USD','81999898',1,1,'',null,'TCURF_20231031_154822_001.parquet',current_timestamp(),null,null),
    ('000','1001','USD','EUR','81999898',1,1,'',null,'TCURF_20231031_154822_001.parquet',current_timestamp(),null,null),
    ('000','1002','EUR','USD','81999898',1,1,'',null,'TCURF_20231031_154822_001.parquet',current_timestamp(),null,null),
    ('000','1002','USD','EUR','81999898',1,1,'',null,'TCURF_20231031_154822_001.parquet',current_timestamp(),null,null),
    ('000','1003','EUR','USD','81999898',1,1,'',null,'TCURF_20231031_154822_001.parquet',current_timestamp(),null,null),
    ('000','1003','USD','EUR','81999898',1,1,'',null,'TCURF_20231031_154822_001.parquet',current_timestamp(),null,null),
    ('000','1004','EUR','USD','81999898',1,1,'',null,'TCURF_20231031_154822_001.parquet',current_timestamp(),null,null),
    ('000','1004','USD','EUR','81999898',1,1,'',null,'TCURF_20231031_154822_001.parquet',current_timestamp(),null,null)""")

    #TCURC

    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.tcurc
    VALUES
    ('000','ADP','ADP','020',null,'','TCURC_20231031_153607_001.parquet',current_timestamp(),null,null),
    ('000','AED','AED','784',null,'','TCURC_20231031_153607_001.parquet',current_timestamp(),null,null),
    ('000','AFA','AFA','004',null,'','TCURC_20231031_153607_001.parquet',current_timestamp(),null,null),
    ('000','AFN','AFN','971',null,'','TCURC_20231031_153607_001.parquet',current_timestamp(),null,null),
    ('000','ALL','ALL','008',null,'','TCURC_20231031_153607_001.parquet',current_timestamp(),null,null),
    ('000','AMD','AMD','051',null,'','TCURC_20231031_153607_001.parquet',current_timestamp(),null,null),
    ('000','ANG','ANG','532',null,'','TCURC_20231031_153607_001.parquet',current_timestamp(),null,null),
    ('000','AOA','AOA','973',null,'','TCURC_20231031_153607_001.parquet',current_timestamp(),null,null),
    ('000','AON','AON','024',null,'','TCURC_20231031_153607_001.parquet',current_timestamp(),null,null),
    ('000','AOR','AOR','982',null,'','TCURC_20231031_153607_001.parquet',current_timestamp(),null,null)""")

    #TCURR_PATTERN

    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.tcurr_pattern
    VALUES
    ('110','001M','M',current_timestamp(),null,null),
    ('110','001Q','Q',current_timestamp(),null,null),
    ('110','002M','M',current_timestamp(),null,null),
    ('110','002Y','Y',current_timestamp(),null,null),
    ('110','003M','M',current_timestamp(),null,null),
    ('110','100*',null,current_timestamp(),null,null),
    ('110','1001',null,current_timestamp(),null,null),
    ('110','1002',null,current_timestamp(),null,null),
    ('110','1003',null,current_timestamp(),null,null),
    ('110','1004',null,current_timestamp(),null,null)""")
    
     #DATE

    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.date
    VALUES('19610906','1961-09-06','1961-09-06','1961','1961-01-01','1961-12-31','1961-Q3','1961-07-01','1961-09-30','1961-06-30','3','1961-09','9','September','1961-09-01','1961-09-30','36','10','2','Wednesday','4','6','249',current_timestamp(),null,null),
    ('19610907','1961-09-07','1961-09-07','1961','1961-01-01','1961-12-31','1961-Q3','1961-07-01','1961-09-30','1961-06-30','3','1961-09','9','September','1961-09-01','1961-09-30','36','10','2','Thursday','5','7','250',current_timestamp(),null,null),
    ('19610908','1961-09-08','1961-09-08','1961','1961-01-01','1961-12-31','1961-Q3','1961-07-01','1961-09-30','1961-06-30','3','1961-09','9','September','1961-09-01','1961-09-30','36','10','2','Friday','6','8','251',current_timestamp(),null,null),
    ('19610909','1961-09-09','1961-09-09','1961','1961-01-01','1961-12-31','1961-Q3','1961-07-01','1961-09-30','1961-06-30','3','1961-09','9','September','1961-09-01','1961-09-30','36','10','2','Saturday','7','9','252',current_timestamp(),null,null),
    ('19610910','1961-09-10','1961-09-10','1961','1961-01-01','1961-12-31','1961-Q3','1961-07-01','1961-09-30','1961-06-30','3','1961-09','9','September','1961-09-01','1961-09-30','36','10','3','Sunday','1','10','253',current_timestamp(),null,null),
    ('19610911','1961-09-11','1961-09-11','1961','1961-01-01','1961-12-31','1961-Q3','1961-07-01','1961-09-30','1961-06-30','3','1961-09','9','September','1961-09-01','1961-09-30','37','11','3','Monday','2','11','254',current_timestamp(),null,null),
    ('19610912','1961-09-12','1961-09-12','1961','1961-01-01','1961-12-31','1961-Q3','1961-07-01','1961-09-30','1961-06-30','3','1961-09','9','September','1961-09-01','1961-09-30','37','11','3','Tuesday','3','12','255',current_timestamp(),null,null),
    ('19610913','1961-09-13','1961-09-13','1961','1961-01-01','1961-12-31','1961-Q3','1961-07-01','1961-09-30','1961-06-30','3','1961-09','9','September','1961-09-01','1961-09-30','37','11','3','Wednesday','4','13','256',current_timestamp(),null,null),
    ('19610914','1961-09-14','1961-09-14','1961','1961-01-01','1961-12-31','1961-Q3','1961-07-01','1961-09-30','1961-06-30','3','1961-09','9','September','1961-09-01','1961-09-30','37','11','3','Thursday','5','14','257',current_timestamp(),null,null),
    ('19610915','1961-09-15','1961-09-15','1961','1961-01-01','1961-12-31','1961-Q3','1961-07-01','1961-09-30','1961-06-30','3','1961-09','9','September','1961-09-01','1961-09-30','37','11','3','Friday','6','15','258',current_timestamp(),null,null)""")

    #T006A

    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.t006a
    VALUES('000','1','10','D','d','Days','Days','1001','C',current_timestamp(),'T006A_20231031_150346_001.parquet',current_timestamp(),null,null),
    ('000','1','2M','CMS','cm/s','cm/s','Centimeter/second','1001','C',current_timestamp(),'T006A_20231031_150346_001.parquet',current_timestamp(),null,null),
    ('000','1','4G','µL','µl','µl','Microliter','1001','C',current_timestamp(),'T006A_20231031_150346_001.parquet',current_timestamp(),null,null),
    ('000','1','4O','µF','µF','µF','Microfarad','1001','C',current_timestamp(),'T006A_20231031_150346_001.parquet',current_timestamp(),null,null),
    ('000','1','4T','IB','pF','pF','Pikofarad','1001','C',current_timestamp(),'T006A_20231031_150346_001.parquet',current_timestamp(),null,null),
    ('000','1','A','A','A','A','Ampere','1001','C',current_timestamp(),'T006A_20231031_150346_001.parquet',current_timestamp(),null,null),
    ('000','1','A87','GOH','GOhm','GOhm','Gigaohm','1001','C',current_timestamp(),'T006A_20231031_150346_001.parquet',current_timestamp(),null,null),
    ('000','1','API','API','°API','°API','API Gravity','1001','C',current_timestamp(),'T006A_20231031_150346_001.parquet',current_timestamp(),null,null),
    ('000','1','B73','MN','MN','MN','Meganewton','1001','C',current_timestamp(),'T006A_20231031_150346_001.parquet',current_timestamp(),null,null),
    ('000','1','B78','MHV','MV','MV','Megavolt','1001','C',current_timestamp(),'T006A_20231031_150346_001.parquet',current_timestamp(),null,null)""")
    
    #T006

    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.t006
    VALUES('000','%','X','X',0,'X','','','','PROPOR',1,100,0,0.000000,0,0,'P1','',0.0,'','',0.0,'','1001%','T006_20231031_153250_001.parquet',current_timestamp(),null,null),
    ('000','%O','X','X',0,'X','','','','PROPOR',1,1000,0,0.000000,0,0,'','',0.0,'','',0.0,'','1001%O','T006_20231031_153250_001.parquet',current_timestamp(),null,null),
    ('000','1','X','X',0,'','','','','PROPOR',1,1,0,0.000000,0,0,'','',0.0,'','',0.0,'','10011','T006_20231031_153250_001.parquet',current_timestamp(),null,null),
    ('000','10','X','X',0,'X','','','','TIME',86400,1,0,0.000000,0,0,'DAY','',0.0,'','',0.0,'','100110','T006_20231031_153250_001.parquet',current_timestamp(),null,null),
    ('000','22S','X','X',0,'','','','','VISKIN',1,1000000,0,0.000000,0,0,'','',0.0,'','',0.0,'','100122S','T006_20231031_153250_001.parquet',current_timestamp(),null,null),
    ('000','2M','X','X',0,'X','','','','SPEED',1,1,-2,0.000000,0,0,'2M','',0.0,'','',0.0,'','10012M','T006_20231031_153250_001.parquet',current_timestamp(),null,null),
    ('000','2X','X','X',0,'X','','','','SPEED',1,60,0,0.000000,0,0,'2X','',0.0,'','',0.0,'','10012X','T006_20231031_153250_001.parquet',current_timestamp(),null,null),
    ('000','4G','X','X',0,'X','','','','VOLUME',1,1,-9,0.000000,0,0,'4G','',0.0,'','',0.0,'','10014G','T006_20231031_153250_001.parquet',current_timestamp(),null,null),
    ('000','4O','X','X',0,'X','','','','CAPACI',1,1,-6,0.000000,0,0,'4O','',0.0,'','',0.0,'','10014O','T006_20231031_153250_001.parquet',current_timestamp(),null,null),
    ('000','4T','X','X',0,'X','','','','CAPACI',1,1,-12,0.000000,0,0,'4T','',0.0,'','',0.0,'','10014T','T006_20231031_153250_001.parquet',current_timestamp(),null,null)""")

    #MARA
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.mara
    VALUES('110','000000000000000252',null,'UPGSN017SH',null,'','K','K','','CH00','1','31XXXX','','EA','','','','','','','','000','','','','','','','','0','0','','0','','','','','','','','','','','0','','','','','','','','','0','0','0','','','','','','0','','0','','0','0','','','','','','0',0,'','',null,null,'','','','','','','','','','','',null,null,'','','0','0','0','','0','0','','0','','000000000000000000','','','','','','','','','','','','','','','','','','00','','','','','','','','','','','','','','','','','','','','','','','0','0','0','0','0','','','','0','','','','','','','','','','','','','','','','','','','','','','','','','','','000000000','','00000000','','','','','','','','','','','','','','','000','','0','0','0','0','0','00','','','','','','','','','0','','','','0000000000','0000000000','0000000000','','','','','','','','000','','000','','000','','000','','000','','','','','MARA_20231109_103040_1.parquet','2024-07-31T14:28:19.416+0000',null,null),
    ('110','000000000000003501',null,'INAMA7',null,'PHMVJM','KELVCXQDBAGSZ','KELVCXQDBAGS','','YPAC','O','ZPDC201P','','KAR','','','','','','','','000','','','','','','','','11.385','10.886','KG','11.356','L15','','01','','000','0001','','03','','','0','','','','','','','71611835015','UC','37','29','31','CM','001B0853002V7MA0II','','','','0','','0','','0','0','1','','','','','0',0,'','SH01',null,null,'','','','15121503','','','','','','','',null,null,'','','3','48','0','','0','0','','0','2','000000000000000000','X','','','','','','','','','','GPP','','','','','','','00','2','','','','','','','','B','','','','','','','','','','','','','','0','0','0','0','0','','','','0','','','','','','','','','','','','','','','','','','','','','','','','','','','000000000','','00000000','','','','KAR','','','','','','','','','','','000','','0','0','0','0','0','00','','','','','','','','','0','','','','0000000000','0000000000','0000000000','','','','','','','','000','','000','','000','','000','','000','','','2','','MARA_20240215_133942_1.parquet','2024-07-31T14:28:19.416+0000',null,null),
    ('110','000000000000012414',null,'INAMA7',null,'PHMVJM','KELVCXQDBAGSZ','KELVCXQDBAGS','','YPAC','O','ZPDC201P','','KAR','','','','','','','','000','','','','','','','','2.174','1.976','KG','2.271','L15','','01','','000','0001','','03','','','0','','','','','','','73102124158','UC','31','17','14','CM','001B2048002WN0A0II','','','','0','','0','','0','0','1','','','','','0',0,'','SH01',null,null,'','','','15121501','','','','','','','',null,null,'','','3','48','0','','0','0','','0','2','000000000000000000','X','','','','','','','','','','000','','','','','','','00','2','','','','','','','','B','','','','','','','','','','','','','','0','0','0','0','0','','','','0','','','','','','','','','','','','','','','','','','','','','','','','','','','000000000','','00000000','','','','KAR','','','','','','','','','','','000','','0','0','0','0','0','00','','','','','','','','','0','','','','0000000000','0000000000','0000000000','','','','','','','','000','','000','','000','','000','','000','','','2','','MARA_20240215_133942_1.parquet','2024-07-31T14:28:19.416+0000',null,null),
    ('110','000000000000030710',null,'INAMA7',null,'PHMVJM','KELVCXQDBAGSZ','KELVCXQDBAGS','','YPAC','O','ZPDC201P','','EA','','','','','','','','000','','','','','','','','198.118','183.442','KG','208.197','L15','','01','','000','0001','','03','','','0','','','','','','','73102307100','UC','87.8','58.5','58.5','CM','001B0956002VJ1A0II','','','','0','','0','','0','0','1','','','','','0',0,'','SH01',null,null,'','','','15121501','','','','','','','',null,null,'','','0','0','0','','0','0','','0','2','000000000000000000','X','','','','','','','','','','000','','','','','','','00','','','','','','','','','B','','','','','','','','','','','','','','0','0','0','0','0','','','','0','','','','','','','','','','','','','','','','','','','','','','','','','','','000000000','','00000000','','','','EA','','','','','','','','','','','000','','0','0','0','0','0','00','','','','','','','','','0','','','','0000000000','0000000000','0000000000','','','','','','','','000','','000','','000','','000','','000','','','2','','MARA_20240215_133942_1.parquet','2024-07-31T14:28:19.416+0000',null,null),
    ('110','000000000005066325',null,'INAMA7',null,'PHNMAB','KELVCXQDBAGSZ','KELVCXQDBAGS','','YPAC','O','ZPDC221F','','KAR','','','','','','','','000','','','','','','','','26.483','24.075','KG','22.712','L15','','01','','000','0001','','03','','','0','','','','','','','10021400574318','IC','41','33','32','CM','001C1189002V2RA0II','','','','0','','0','','0','0','1','','','','','0',0,'','SH01',null,null,'','','','25174004','','','','','','','',null,null,'','','3','24','0','','0','0','','0','2','000000000000000000','X','','','','','','','','','','000','','','','','','','00','2','','','','','','','','B','','','','','','','','','','','','','','0','0','0','0','0','','','','0','','','','','','','','','','','','','','','','','','','','','','','','','','','000000000','','00000000','','','','KAR','','','','','','','','','','','000','','0','0','0','0','0','00','','','','','','','','','0','','','','0000000000','0000000000','0000000000','','','','','','','','000','','000','','000','','000','','000','','','2','','MARA_20231021_164038_1.parquet','2024-07-31T14:28:19.416+0000',null,null)""")

    #MARM
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.marm
    VALUES('110','000000000000000252','EA','1','1','','','','0','0','0','','0','','0','','','0000000000','00','','','','','','','0','0','0','','','MARM_20231109_142157_1.parquet','2024-07-31T14:28:18.863+0000','2023-11-09T09:47:49.293+0000','I'),
    ('110','000000000000003897','APU','4017','76030','','','','0','0','0','','0','','0','','','0000000000','00','','','','','','','0','0','0','','','MARM_20231021_124051_1.parquet','2024-07-31T14:28:18.863+0000','2023-10-21T12:29:17.328+0000','I'),
    ('110','000000000000003897','EA','1','2','','71611938976','UC','22','18','37','CM','9.464','L15','8.864','KG','','0000000000','00','','','','','','','0','0','0','','','MARM_20231021_124051_1.parquet','2024-07-31T14:28:18.863+0000','2023-10-21T12:29:17.328+0000','I'),
    ('110','000000000000003897','KAR','1','1','','10071611938973','IC','41','25','38','CM','18.927','L15','18.321','KG','','0000000000','00','','','','','','','0','0','0','','','MARM_20231021_124051_1.parquet','2024-07-31T14:28:18.863+0000','2023-10-21T12:29:17.328+0000','I'),
    ('110','000000000000003897','KG','100','1701','','','','0','0','0','','0','','0','','','0000000000','00','','','','','','','0','0','0','','','MARM_20231021_124051_1.parquet','2024-07-31T14:28:18.863+0000','2023-10-21T12:29:17.328+0000','I')""")
