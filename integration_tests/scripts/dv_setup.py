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
from pyspark.sql.functions import cast  

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
raw_schema = base_config.get_uc_raw_schema()
euh_schema = base_config.get_uc_euh_schema()
eh_schema = base_config.get_uc_eh_schema()

raw_folder_path = base_config.get_raw_folder_path()
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
# print("eh md schema: ", eh_md_schema)
# print("eh md folder path: ", eh_md_folder_path)

# COMMAND ----------

# MAGIC %md ## Delete UC schemas and tables

# COMMAND ----------

raw_table_list = ['datavalidation_control_table']
if env == "dev":
    for table_name in raw_table_list:
        spark.sql(f"DROP TABLE IF EXISTS `{uc_catalog}`.`{raw_schema}`.table_name")

# COMMAND ----------

euh_table_list = ['mseg','glpca','vbrp',"tcurr_bw","tcurv_bw","tcurw_bw","tcurf_bw",'tcurr','tcurv','tcurt','tcurw','tcurn','tcurf','tcurc','tcurr_pattern','date','t006','t006a','mara','marm']
if env == "dev":
    for table_name in euh_table_list:
        spark.sql(f"DROP TABLE IF EXISTS `{uc_catalog}`.`{euh_schema}`.table_name")

# COMMAND ----------

eh_table_list = ['datavalidation_output_table']
if env == "dev":
    for table_name in eh_table_list:
        spark.sql(f"DROP TABLE IF EXISTS `{uc_catalog}`.`{eh_schema}`.table_name")

# COMMAND ----------

if env == "dev":
    spark.sql(f"DROP SCHEMA IF EXISTS `{uc_catalog}`.`{raw_schema}` CASCADE")

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

# DBTITLE 1,Delete Raw folder
if env == "dev":
    dbutils.fs.rm(raw_folder_path, recurse=True)

# COMMAND ----------

# DBTITLE 1,Delete EUH folder
if env == "dev":
    dbutils.fs.rm(euh_folder_path, recurse=True)

# COMMAND ----------

# DBTITLE 1,Delete EH folder
if env == "dev":
    dbutils.fs.rm(eh_folder_path, recurse=True)

# COMMAND ----------

if env == "dev":
    dbutils.fs.rm(eh_md_folder_path, recurse=True)

# COMMAND ----------

# MAGIC %md ## Insert data into euh layer tables

# COMMAND ----------

# DBTITLE 1,Create UC schema
if env == "dev":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{uc_catalog}`.`{raw_schema}`")

# COMMAND ----------

# DBTITLE 1,Create UC schemas
if env == "dev":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{uc_catalog}`.`{euh_schema}`")

# COMMAND ----------

if env == "dev":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{uc_catalog}`.`{eh_schema}`")

# COMMAND ----------

if env == "dev":
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{uc_catalog}`.`{eh_md_schema}`")

# COMMAND ----------

# DBTITLE 1,dataValidation_Control_table create statement
if env == "dev":    
    datavalidation_control_table = raw_folder_path + "/dataValidation_Control_table"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{raw_schema}`.dataValidation_Control_table(
    Table_name string,
    Measure String,
    Date_field String,
    From_Ind Integer,
    To_Ind Integer)
    USING delta
    LOCATION '{datavalidation_control_table}'""")
        
        
              

# COMMAND ----------

# DBTITLE 1,dataValidation_Output_table create statement
if env == "dev":    
    datavalidation_output_table  = eh_folder_path + "/dataValidation_Output_table"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{eh_schema}`.dataValidation_Output_table (
    Table_Name string,
    Record_Count string,
    Measure String,
    Measure_Sum decimal(35,2),
    From_Ind Integer,
    To_Ind Integer,
    Ingested_at string)
    USING delta
    LOCATION '{datavalidation_output_table}'""")

# COMMAND ----------

# DBTITLE 1,GLPCA create statement
if env == "dev":     
    glpca_table_path = euh_folder_path + "/GLPCA"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.glpca (
    RCLNT STRING,
    GL_SIRID STRING,
    RLDNR STRING,
    RRCTY STRING,
    RVERS STRING,
    RYEAR STRING,
    RTCUR STRING,
    RUNIT STRING,
    DRCRK STRING,
    POPER STRING,
    DOCCT STRING,
    DOCNR STRING,
    DOCLN STRING,
    RBUKRS STRING,
    RPRCTR STRING,
    RHOART STRING,
    RFAREA STRING,
    KOKRS STRING,
    RACCT STRING,
    HRKFT STRING,
    RASSC STRING,
    EPRCTR STRING,
    ACTIV STRING,
    AFABE STRING,
    OCLNT STRING,
    SBUKRS STRING,
    SPRCTR STRING,
    SHOART STRING,
    SFAREA STRING,
    TSL DECIMAL(15,2),
    HSL DECIMAL(15,2),
    KSL DECIMAL(15,2),
    MSL DECIMAL(15,3),
    CPUDT DATE,
    CPUTM STRING,
    USNAM STRING,
    SGTXT STRING,
    AUTOM STRING,
    DOCTY STRING,
    BLDAT DATE,
    BUDAT DATE,
    WSDAT DATE,
    REFDOCNR STRING,
    REFRYEAR STRING,
    REFDOCLN STRING,
    REFDOCCT STRING,
    REFACTIV STRING,
    AWTYP STRING,
    AWORG STRING,
    WERKS STRING,
    GSBER STRING,
    KOSTL STRING,
    LSTAR STRING,
    AUFNR STRING,
    AUFPL STRING,
    ANLN1 STRING,
    ANLN2 STRING,
    MATNR STRING,
    BWKEY STRING,
    BWTAR STRING,
    ANBWA STRING,
    KUNNR STRING,
    LIFNR STRING,
    RMVCT STRING,
    EBELN STRING,
    EBELP STRING,
    KSTRG STRING,
    ERKRS STRING,
    PAOBJNR STRING,
    PASUBNR STRING,
    PS_PSP_PNR STRING,
    KDAUF STRING,
    KDPOS STRING,
    FKART STRING,
    VKORG STRING,
    VTWEG STRING,
    AUBEL STRING,
    AUPOS STRING,
    SPART STRING,
    VBELN STRING,
    POSNR STRING,
    VKGRP STRING,
    VKBUR STRING,
    VBUND STRING,
    LOGSYS STRING,
    ALEBN STRING,
    AWSYS STRING,
    VERSA STRING,
    STFLG STRING,
    STOKZ STRING,
    STAGR STRING,
    GRTYP STRING,
    REP_MATNR STRING,
    CO_PRZNR STRING,
    IMKEY STRING,
    DABRZ DATE,
    VALUT DATE,
    RSCOPE STRING,
    AWREF_REV STRING,
    AWORG_REV STRING,
    BWART STRING,
    BLART STRING,
    ZZRBLGP STRING,
    SYSTEM_ID STRING,
    LAST_ACTION_CD STRING,
    LAST_DTM TIMESTAMP,
    yrmono STRING,
    ingested_at TIMESTAMP,
    AEDATTM TIMESTAMP,
    OPFLAG STRING)
    USING delta
    LOCATION '{glpca_table_path}'""")

# COMMAND ----------

# DBTITLE 1,MSEG create statement
if env == "dev":    
    mseg_table_path = euh_folder_path + "/MSEG"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.mseg (
    MANDT STRING,
    MBLNR STRING,
    MJAHR STRING,
    ZEILE STRING,
    LINE_ID STRING,
    PARENT_ID STRING,
    LINE_DEPTH STRING,
    MAA_URZEI STRING,
    BWART STRING,
    XAUTO STRING,
    MATNR STRING,
    WERKS STRING,
    LGORT STRING,
    CHARG STRING,
    INSMK STRING,
    ZUSCH STRING,
    ZUSTD STRING,
    SOBKZ STRING,
    LIFNR STRING,
    KUNNR STRING,
    KDAUF STRING,
    KDPOS STRING,
    KDEIN STRING,
    PLPLA STRING,
    SHKZG STRING,
    WAERS STRING,
    DMBTR DECIMAL(13,2),
    BNBTR DECIMAL(13,2),
    BUALT DECIMAL(13,2),
    SHKUM STRING,
    DMBUM DECIMAL(13,2),
    BWTAR STRING,
    MENGE DECIMAL(13,3),
    MEINS STRING,
    ERFMG DECIMAL(13,3),
    ERFME STRING,
    BPMNG DECIMAL(13,3),
    BPRME STRING,
    EBELN STRING,
    EBELP STRING,
    LFBJA STRING,
    LFBNR STRING,
    LFPOS STRING,
    SJAHR STRING,
    SMBLN STRING,
    SMBLP STRING,
    ELIKZ STRING,
    SGTXT STRING,
    EQUNR STRING,
    WEMPF STRING,
    ABLAD STRING,
    GSBER STRING,
    KOKRS STRING,
    PARGB STRING,
    PARBU STRING,
    KOSTL STRING,
    PROJN STRING,
    AUFNR STRING,
    ANLN1 STRING,
    ANLN2 STRING,
    XSKST STRING,
    XSAUF STRING,
    XSPRO STRING,
    XSERG STRING,
    GJAHR STRING,
    XRUEM STRING,
    XRUEJ STRING,
    BUKRS STRING,
    BELNR STRING,
    BUZEI STRING,
    BELUM STRING,
    BUZUM STRING,
    RSNUM STRING,
    RSPOS STRING,
    KZEAR STRING,
    PBAMG DECIMAL(29,3),
    KZSTR STRING,
    UMMAT STRING,
    UMWRK STRING,
    UMLGO STRING,
    UMCHA STRING,
    UMZST STRING,
    UMZUS STRING,
    UMBAR STRING,
    UMSOK STRING,
    KZBEW STRING,
    KZVBR STRING,
    KZZUG STRING,
    WEUNB STRING,
    PALAN DECIMAL(11,0),
    LGNUM STRING,
    LGTYP STRING,
    LGPLA STRING,
    BESTQ STRING,
    BWLVS STRING,
    TBNUM STRING,
    TBPOS STRING,
    XBLVS STRING,
    VSCHN STRING,
    NSCHN STRING,
    DYPLA STRING,
    UBNUM STRING,
    TBPRI STRING,
    TANUM STRING,
    WEANZ STRING,
    GRUND STRING,
    EVERS STRING,
    EVERE STRING,
    IMKEY STRING,
    KSTRG STRING,
    PAOBJNR STRING,
    PRCTR STRING,
    PS_PSP_PNR STRING,
    NPLNR STRING,
    AUFPL STRING,
    APLZL STRING,
    AUFPS STRING,
    VPTNR STRING,
    FIPOS STRING,
    SAKTO STRING,
    BSTMG DECIMAL(13,3),
    BSTME STRING,
    XWSBR STRING,
    EMLIF STRING,
    EXBWR DECIMAL(38,18),
    VKWRT DECIMAL(38,18),
    AKTNR STRING,
    ZEKKN STRING,
    VFDAT DATE,
    CUOBJ_CH STRING,
    EXVKW DECIMAL(38,18),
    PPRCTR STRING,
    RSART STRING,
    GEBER STRING,
    FISTL STRING,
    MATBF STRING,
    UMMAB STRING,
    BUSTM STRING,
    BUSTW STRING,
    MENGU STRING,
    WERTU STRING,
    LBKUM DECIMAL(13,3),
    SALK3 DECIMAL(38,18),
    VPRSV STRING,
    FKBER STRING,
    DABRBZ DATE,
    VKWRA DECIMAL(13,2),
    DABRZ DATE,
    XBEAU STRING,
    LSMNG DECIMAL(13,3),
    LSMEH STRING,
    KZBWS STRING,
    QINSPST STRING,
    URZEI STRING,
    J_1BEXBASE DECIMAL(13,2),
    MWSKZ STRING,
    TXJCD STRING,
    EMATN STRING,
    J_1AGIRUPD STRING,
    VKMWS STRING,
    HSDAT DATE,
    BERKZ STRING,
    MAT_KDAUF STRING,
    MAT_KDPOS STRING,
    MAT_PSPNR STRING,
    XWOFF STRING,
    BEMOT STRING,
    PRZNR STRING,
    LLIEF STRING,
    LSTAR STRING,
    XOBEW STRING,
    GRANT_NBR STRING,
    ZUSTD_T156M STRING,
    SPE_GTS_STOCK_TY STRING,
    KBLNR STRING,
    KBLPOS STRING,
    XMACC STRING,
    VGART_MKPF STRING,
    BUDAT_MKPF DATE,
    CPUDT_MKPF DATE,
    CPUTM_MKPF STRING,
    USNAM_MKPF STRING,
    XBLNR_MKPF STRING,
    TCODE2_MKPF STRING,
    VBELN_IM STRING,
    VBELP_IM STRING,
    SGT_SCAT STRING,
    SGT_UMSCAT STRING,
    `/BEV2/ED_KZ_VER` STRING,
    `/BEV2/ED_USER` STRING,
    `/BEV2/ED_AEDAT` DATE,
    `/BEV2/ED_AETIM` STRING,
    `/CWM/MENGE` DECIMAL(13,3),
    `/CWM/MEINS` STRING,
    `/CWM/ERFMG` DECIMAL(13,3),
    `/CWM/ERFME` STRING,
    `/CWM/BUSTW` STRING,
    `/CWM/SMBLU` STRING,
    `/CWM/FI` INT,
    ZXBUDAT DATE,
    ZXOIB_BLTIME STRING,
    ZXBLDAT DATE,
    ZNOMTK STRING,
    ZNOMIT STRING,
    ZIMPERNO STRING,
    ZPASSNO STRING,
    ZREPTVT STRING,
    ZCARRIER STRING,
    ZSTI_REFNO STRING,
    ZSTI_REFITEM STRING,
    OINAVNW DECIMAL(13,2),
    OICONDCOD STRING,
    CONDI STRING,
    OIKNUMV STRING,
    OIEXGPTR STRING,
    OIMATPST STRING,
    OIMATIE STRING,
    OIEXGTYP STRING,
    OIFEETOT DECIMAL(13,2),
    OIFEEPST STRING,
    OIFEEDT DATE,
    OIMATREF STRING,
    OIEXGNUM STRING,
    OINETCYC STRING,
    OIJ1BNFFIM STRING,
    OITRKNR STRING,
    OITRKJR STRING,
    OIEXTNR STRING,
    OIITMNR STRING,
    OIFTIND STRING,
    OIPRIOP STRING,
    OITRIND STRING,
    OIGHNDL STRING,
    OIUMBAR STRING,
    OISBREL STRING,
    OIBASPROD STRING,
    OIBASVAL DECIMAL(13,2),
    OIGLERF DECIMAL(13,3),
    OIGLSKU DECIMAL(13,3),
    OIGLBPR DECIMAL(13,3),
    OIGLBST DECIMAL(13,3),
    OIGLCALC STRING,
    OIBBSWQTY DOUBLE,
    OIASTBW STRING,
    OIVBELN STRING,
    OIPOSNR STRING,
    OIPIPEVAL STRING,
    OIC_LIFNR STRING,
    OIC_DCITYC STRING,
    OIC_DCOUNC STRING,
    OIC_DREGIO STRING,
    OIC_DLAND1 STRING,
    OIC_OCITYC STRING,
    OIC_OCOUNC STRING,
    OIC_OREGIO STRING,
    OIC_OLAND1 STRING,
    OIC_PORGIN STRING,
    OIC_PDESTN STRING,
    OIC_PTRIP STRING,
    OIC_PBATCH STRING,
    OIC_MOT STRING,
    OIC_AORGIN STRING,
    OIC_ADESTN STRING,
    OIC_TRUCKN STRING,
    OIA_BASELO STRING,
    OICERTF1 STRING,
    OIDATFM1 DATE,
    OIDATTO1 DATE,
    OIH_LICTP STRING,
    OIH_LICIN STRING,
    OIH_LCFOL STRING,
    OIH_FOLQTY DECIMAL(13,3),
    OID_EXTBOL STRING,
    OID_MISCDL STRING,
    OIBOMHEAD STRING,
    OIB_TIMESTAMP STRING,
    OIB_GUID_OPEN STRING,
    OIB_GUID_CLOSE STRING,
    OIB_SOCNR STRING,
    OITAXFROM STRING,
    OITAXTO STRING,
    OIHANTYP STRING,
    OITAXGRP STRING,
    OIPRICIE STRING,
    OIINVREC STRING,
    OIOILCON DECIMAL(5,2),
    OIOILCON2 DECIMAL(5,2),
    OIFUTDT DATE,
    OIFUTDT2 DATE,
    OIUOMQT STRING,
    OITAXQT DECIMAL(13,3),
    OIFUTQT DECIMAL(13,3),
    OIFUTQT2 DECIMAL(13,3),
    OITAXGRP2 STRING,
    OIEDBAL_GI STRING,
    OIEDBALM_GI STRING,
    OICERTF1_GI STRING,
    OIDATFM1_GI DATE,
    OIDATTO1_GI DATE,
    OIH_LICTP_GI STRING,
    OIH_LICIN_GI STRING,
    OIH_LCFOL_GI STRING,
    OIH_FOLQTY_GI DECIMAL(13,3),
    OIHANTYP_GI STRING,
    OIO_VSTEL STRING,
    OIO_SPROC STRING,
    ZZEILE STRING,
    LAST_ACTION_CD STRING,
    LAST_DTM TIMESTAMP,
    SYSTEM_ID STRING,
    ZOIVBELN_ID STRING,
    ZZNOMTK_ID STRING,
    ZVGART_ID STRING,
    ZOIEXGNUM_ID STRING,
    ZBWART_ID STRING,
    ZLIFNR_ID STRING,
    ZOICLIFNR_ID STRING,
    ZOICTRUCKN_ID STRING,
    ZOICMOT_ID STRING,
    ZWERKS_ID STRING,
    ZOIOVSTEL_ID STRING,
    ZOIHANTYP_ID STRING,
    ZUMWRK_ID STRING,
    ZMATNR_ID STRING,
    ZBWTAR_ID STRING,
    ZKUNNR_ID STRING,
    ZWEMPF_ID STRING,
    ZUSNAMMKPF_ID STRING,
    ZOICOLAND1_ID STRING,
    ZOICDLAND1_ID STRING,
    ZOIHLICTP_ID STRING,
    ZOICERTF1_ID STRING,
    ZOIPOSNR_ID STRING,
    ZMBLNR_ID STRING,
    ZMJAHR_ID STRING,
    ZZEILE_ID STRING,
    ZOITAXGRP_ID STRING,
    ZEBELN_ID STRING,
    ZKDAUF_ID STRING,
    ZBUKRS_ID STRING,
    ZOIC_OCITYC_ID STRING,
    ZDIC_OICITYC_ID STRING,
    ZOITAXFROM_ID STRING,
    ZOITAXTO_ID STRING,
    ZLGORT_ID STRING,
    ZUMLGO_ID STRING,
    ZSTOR_LOC_ID STRING,
    ZPART_STOR_LOC_ID STRING,
    ZEXC_DUTY_LIC_ID STRING,
    ZERFME_ID STRING,
    ZORIGIN_CITY_ID STRING,
    ZDESTINATION_CITY_ID STRING,
    ZMBLNR_YR_ALT_ID STRING,
    SGT_RCAT STRING,
    `/CWM/TY2TQ` STRING,
    `/CWM/UMTY2TQ` STRING,
    DISUB_OWNER STRING,
    FSH_SEASON_YEAR STRING,
    FSH_SEASON STRING,
    FSH_COLLECTION STRING,
    FSH_THEME STRING,
    FSH_UMSEA_YR STRING,
    FSH_UMSEA STRING,
    FSH_UMCOLL STRING,
    FSH_UMTHEME STRING,
    SGT_CHINT STRING,
    FSH_DEALLOC_QTY DECIMAL(13,3),
    WRF_CHARSTC1 STRING,
    WRF_CHARSTC2 STRING,
    WRF_CHARSTC3 STRING,
    ZMATERIAL_NUMBER STRING,
    yrmono STRING,
    ingested_at TIMESTAMP,
    AEDATTM TIMESTAMP,
    OPFLAG STRING)
    USING delta
    LOCATION '{mseg_table_path}'""")

# COMMAND ----------

# DBTITLE 1,VBRP

if env == "dev": 
    VBRP_table_path = euh_folder_path + "/VBRP"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.VBRP
    (MANDT string,
    VBELN string,
    POSNR string,
    UEPOS string,
    FKIMG decimal(13,3),
    VRKME string,
    UMVKZ decimal(5,0),
    UMVKN decimal(5,0),
    MEINS string,
    SMENG decimal(13,3),
    FKLMG decimal(13,3),
    LMENG decimal(13,3),
    NTGEW decimal(15,3),
    BRGEW decimal(15,3),
    GEWEI string,
    VOLUM decimal(15,3),
    VOLEH string,
    GSBER string,
    PRSDT date,
    FBUDA date,
    KURSK decimal(9,5),
    NETWR decimal(15,2),
    VBELV string,
    POSNV string,
    VGBEL string,
    VGPOS string,
    VGTYP string,
    AUBEL string,
    AUPOS string,
    AUREF string,
    MATNR string,
    ARKTX string,
    PMATN string,
    CHARG string,
    MATKL string,
    PSTYV string,
    POSAR string,
    PRODH string,
    VSTEL string,
    ATPKZ string,
    SPART string,
    POSPA string,
    WERKS string,
    ALAND string,
    WKREG string,
    WKCOU string,
    WKCTY string,
    TAXM1 string,
    TAXM2 string,
    TAXM3 string,
    TAXM4 string,
    TAXM5 string,
    TAXM6 string,
    TAXM7 string,
    TAXM8 string,
    TAXM9 string,
    KOWRR string,
    PRSFD string,
    SKTOF string,
    SKFBP decimal(13,2),
    KONDM string,
    KTGRM string,
    KOSTL string,
    BONUS string,
    PROVG string,
    EANNR string,
    VKGRP string,
    VKBUR string,
    SPARA string,
    SHKZG string,
    ERNAM string,
    ERDAT date,
    ERZET string,
    BWTAR string,
    LGORT string,
    STAFO string,
    WAVWR decimal(13,2),
    KZWI1 decimal(13,2),
    KZWI2 decimal(13,2),
    KZWI3 decimal(13,2),
    KZWI4 decimal(13,2),
    KZWI5 decimal(13,2),
    KZWI6 decimal(13,2),
    STCUR decimal(9,5),
    UVPRS string,
    UVALL string,
    EAN11 string,
    PRCTR string,
    KVGR1 string,
    KVGR2 string,
    KVGR3 string,
    KVGR4 string,
    KVGR5 string,
    MVGR1 string,
    MVGR2 string,
    MVGR3 string,
    MVGR4 string,
    MVGR5 string,
    MATWA string,
    BONBA decimal(13,2),
    KOKRS string,
    PAOBJNR string,
    PS_PSP_PNR string,
    AUFNR string,
    TXJCD string,
    CMPRE decimal(11,2),
    CMPNT string,
    CUOBJ string,
    CUOBJ_CH string,
    KOUPD string,
    UECHA string,
    XCHAR string,
    ABRVW string,
    SERNR string,
    BZIRK_AUFT string,
    KDGRP_AUFT string,
    KONDA_AUFT string,
    LLAND_AUFT string,
    MPROK string,
    PLTYP_AUFT string,
    REGIO_AUFT string,
    VKORG_AUFT string,
    VTWEG_AUFT string,
    ABRBG date,
    PROSA string,
    UEPVW string,
    AUTYP string,
    STADAT date,
    FPLNR string,
    FPLTR string,
    AKTNR string,
    KNUMA_PI string,
    KNUMA_AG string,
    PREFE string,
    MWSBP decimal(13,2),
    AUGRU_AUFT string,
    FAREG string,
    UPMAT string,
    UKONM string,
    CMPRE_FLT double,
    ABFOR string,
    ABGES double,
    J_1ARFZ string,
    J_1AREGIO string,
    J_1AGICD string,
    J_1ADTYP string,
    J_1ATXREL string,
    J_1BCFOP string,
    J_1BTAXLW1 string,
    J_1BTAXLW2 string,
    J_1BTXSDC string,
    BRTWR decimal(15,2),
    WKTNR string,
    WKTPS string,
    RPLNR string,
    KURSK_DAT date,
    WGRU1 string,
    WGRU2 string,
    KDKG1 string,
    KDKG2 string,
    KDKG3 string,
    KDKG4 string,
    KDKG5 string,
    VKAUS string,
    J_1AINDXP string,
    J_1AIDATEP date,
    KZFME string,
    MWSKZ string,
    VERTT string,
    VERTN string,
    SGTXT string,
    DELCO string,
    BEMOT string,
    RRREL string,
    AKKUR decimal(9,5),
    WMINR string,
    VGBEL_EX string,
    VGPOS_EX string,
    LOGSYS string,
    VGTYP_EX string,
    J_1BTAXLW3 string,
    J_1BTAXLW4 string,
    J_1BTAXLW5 string,
    MSR_ID string,
    MSR_REFUND_CODE string,
    MSR_RET_REASON string,
    NRAB_KNUMH string,
    NRAB_VALUE decimal(13,2),
    DISPUTE_CASE string,
    FUND_USAGE_ITEM binary,
    FARR_RELTYPE string,
    CLAIMS_TAXATION string,
    KURRF_DAT_ORIG date,
    VGTYP_EXT string,
    SGT_RCAT string,
    SGT_SCAT string,
    `/CWM/MENGE` decimal(13,3),
    `/CWM/MEINS` string,
    AUFPL string,
    APLZL string,
    DPCNR string,
    DCPNR string,
    DPNRB string,
    PEROP_BEG date,
    PEROP_END date,
    FONDS string,
    FISTL string,
    FKBER string,
    GRANT_NBR string,
    OIEDOK string,
    OID_EXTBOL string,
    OID_MISCDL string,
    OIPIPEVAL string,
    OIC_LIFNR string,
    OIC_DCITYC string,
    OIC_DCOUNC string,
    OIC_DREGIO string,
    OIC_DLAND1 string,
    OIC_OCITYC string,
    OIC_OCOUNC string,
    OIC_OREGIO string,
    OIC_OLAND1 string,
    OIC_PORGIN string,
    OIC_PDESTN string,
    OIC_PTRIP string,
    OIC_PBATCH string,
    OIC_MOT string,
    OIC_AORGIN string,
    OIC_ADESTN string,
    OIC_TRUCKN string,
    OIA_BASELO string,
    CMETH string,
    OITAXFROM string,
    OIHANTYP string,
    OITAXGRP string,
    OITAXTO string,
    OICERTF1 string,
    OIOILCON decimal(5,2),
    OIDATFM1 date,
    OIDATTO1 date,
    OIEDBAL string,
    OIPRICIE string,
    OIINEX string,
    OIEDBALM string,
    OITAXLOD decimal(13,2),
    OITAXISS decimal(13,2),
    OITAXINV decimal(13,2),
    OIDRC string,
    OIC_DRCTRY string,
    OIC_DRCREG string,
    OIMETIND string,
    OIWAP string,
    OISLF string,
    OIPSDRC string,
    OICONTNR string,
    OIC_KMPOS string,
    OIC_TIME string,
    OIC_DATE date,
    OIFEECH string,
    OIEXGNUM string,
    OIEXGTYP string,
    OIFEETOT decimal(13,2),
    OIFEEDT date,
    OINETCYC string,
    OIH_LICTP string,
    OIH_LICIN string,
    OIH_LCFOL string,
    OIH_FOLQTY decimal(13,3),
    OIPBL string,
    OISBREL string,
    OIBASPROD string,
    OITITLE string,
    OIGNRULE string,
    OIA_IPMVAT string,
    OIINVCYC1 string,
    OIINVCYC2 string,
    OIINVCYC3 string,
    OIINVCYC4 string,
    OIINVCYC5 string,
    OIINVCYC6 string,
    OIINVCYC7 string,
    OIINVCYC8 string,
    OIINVCYC9 string,
    OILIKWTF decimal(19,2),
    OILIMETF decimal(15,3),
    OIFKIMG decimal(13,3),
    OISMENG decimal(13,3),
    OIFKLMG decimal(13,3),
    OILMENG decimal(13,3),
    OINTGEW decimal(15,3),
    OIBRGEW decimal(15,3),
    OIVOLUM decimal(15,3),
    OIHFOLQTY decimal(13,3),
    OIVMENG decimal(13,3),
    OISTYP string,
    OIRELPCGROUP string,
    OIU_RUN_TKT_NO string,
    OIU_A_H_VAL_FCT decimal(13,3),
    OIU_A_H_VAL_F_U string,
    OIU_GRV_AM double,
    OIU_DENSITY_U string,
    OIU_SALE_DT_FROM date,
    OIU_SALE_DT_TO date,
    OIU_PC_WC_NO string,
    OIU_PC_MP_NO string,
    OIU_PC_WL_NO string,
    OIU_DENSTYP string,
    OIU_VLTXNS_NO string,
    OIU_POS_NO string,
    OIU_TRNSP_NO string,
    OIU_TRNSP_REF_NO string,
    OIU_ORIG_MP_NO string,
    PRS_WORK_PERIOD string,
    PPRCTR string,
    PARGB string,
    AUFPL_OAA string,
    APLZL_OAA string,
    CAMPAIGN binary,
    COMPREAS string,
    ZBDOCITM string,
    ZBORGITM string,
    ZBREFITM string,
    ZBSDOCITM string,
    SYSTEM_ID string,
    LAST_DTM timestamp,
    LAST_ACTION_CD string,
    ZOIHANTYP_ID string,
    ZVRKME_ID string,
    ZVGBEL_ID string,
    ZOIEXGNUM_ID string,
    ZOIC_MOT_ID string,
    ZOITAXGRP_ID string,
    ZSPART_ID string,
    ZOIC_OLAND1_ID string,
    ZOIC_DLAND1_ID string,
    ZOIC_OCITYC_ID string,
    ZOIC_DCITYC_ID string,
    ZPRCTR_ID string,
    ZKOKRS_ID string,
    ZAUBEL_ID string,
    ZMWSKZ_ID string,
    ZMATNR_ID string,
    ZWERKS_ID string,
    ZBWTAR_ID string,
    ZBASE_UOM_CD string,
    ZSALES_INV_LINE_ALT_ID string,
    FMFGUS_KEY string,
    FSH_SEASON_YEAR string,
    FSH_SEASON string,
    FSH_COLLECTION string,
    FSH_THEME string,
    BUDGET_PD string,
    WRF_CHARSTC1 string,
    WRF_CHARSTC2 string,
    WRF_CHARSTC3 string,
    Z_MAT_TYPE_ID string,
    Z_SERVICE_TYPE_ID string,
    GLO_LOG_REF1_IT string,
    yrmono string,
    ingested_at timestamp,
    AEDATTM timestamp,
    OPFLAG string)
    USING delta
    LOCATION '{VBRP_table_path}' """)
    


     

# COMMAND ----------

# DBTITLE 1,VBRP data

spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.VBRP
    VALUES
    ('110','0265939933','000001','000000',30000.000,'L',100,119,'KG',0.000,24402.756,25210.084,24402.756,24402.756,'KG',29579.100,'L15','',null,null,51.31300,0.00,'','000000','0730651018','000010','J','0265775810','000010','','000000000400008591','BF AGO 50ppmS B2 3 Udy Umk Diesoline PH','','','ZPDC103B','ZKBF','','002D5004001176ZZZZ','3111','','02','000001','P133','PH','','','','1','','','','','','','','','','','X',0.00,'01','02','','','','','BLV','A011','02','','B_BILPH01_02',null,'17:58:10','TA','9999','',0.00,0.00,0.00,0.00,0.00,275.04,0.00,1.00000,'','','','0000100055','181','','','','002','','','','','','000000000400008591',0.00,'OP01','2095009395','00000000','','',0.00,'','000000000000000000','000000000000000000','','000000','X','','','','04','','PH','','02','07B','PH01','02',null,'','','C',null,'','000000','','','','',0.00,'','','','',0.0,'',0.0,'','','','','','','','','',0.00,'','000000','',null,'','','01','','','','','','',null,'','','','','','','','',0.00000,'','','000000','','','','','','','','','',0.00,'',(cast('AAAAAAAAAAAAAAAAAAAAAA==' AS BINARY)),'','',null,'','','',0.000,'','0000000000','00000000','','000','000',null,null,'','','','','X','','','X','','','','07B','PH','','','','PH','','','','1510200772','','','','','','1','TA','AA','KO','TA','',100.00,null,null,'X','X','EX','0',0.00,0.00,0.00,'','','','','','','','','000000','08:29:00',null,'','','',0.00,null,'','','','',0.000,'','000','','','000','','X','X','X','X','X','X','X','X','X',0.00,-29039.280,30000.000,0.000,24402.756,25210.084,24402.756,24402.756,29579.100,0.000,25210.084,'','','',0.000,'',0.0,'',null,null,'','','','','000000000000','000','','','','0000000','','','0000000000','00000000',(cast('AAAAAAAAAAAAAAAAAAAAAA==' AS BINARY)),'','0265939933000001','000000','0730651018000010','0265775810000010','1001',null,'C','1001AA','1001L','10010730651018','','','1001KO','100102','1001PH','1001PH','','','10010000100055','1001OP01','10010265775810','','1001000000000400008591','1001P133','1001TA','1001KG','10010265939933000001','','','','','','','','','','000000000400008591','','','VBRP_20231031_162534_001.parquet',current_timestamp(),null,null)""")

# COMMAND ----------

# DBTITLE 1,glpca
spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.glpca
    VALUES 
    ('110','22374548860','8A','0','0','2022','PKR','','S','5','A','1987625093','679','PK01','100001','35','SV06','OP01','3170300','','','','RMRP','0','110','','','0','','3092.8','3092.8','16.67','0',null,'8:21:38','PHDEL4','','','',null,null,null,'5230524561','2022','679','W','RMRP','RMRP','2022','P209','','PK01000236','','','0','','','','P209','','','','68059155','','4538018786','100','','','0','0','0','','0','','','','','0','','','0','','','','','','','','','','','','','','',null,null,'OC','','','','RS','0','','',null,'GLPCA_INIT3',current_timestamp(),null,null),
    ('110','21405393808','8A','0','0','2021','CAD','KG','H','10','A','1914479495','1','CA48','800001','2','SV31','OP01','6380303','','','','RFBU','0','110','','','0','','-97.65','-97.65','-79.11','-92.218',null,'8:01:40','R_CPS_RFC','SVMS:-ENHD-ETRF','','',null,null,null,'4012802556','2021','1','W','RFBU','BKPFF','CA482021','C282','','CA48900004','','','0','','','400004665','','','','','','','','0','','','0','0','0','','0','','','','','0','','','0','','','','','','','','','','','','','','',null,null,'OC','','','','HC','0','','',null,'GLPCA_20230812_082950_001.parquet',current_timestamp(),null,null),
    ('110','22097420995','8A','0','0','2022','USD','','H','3','A','1966684754','90','US52','100034','5','SV44','OP01','6355027','','','','SD00','0','110','','','0','','-4.32','-4.32','-4.32','0',null,'3:38:12','B_BILUS52_08','','','',null,null,null,'1409564951','2022','6','W','SD00','VBRK','','U246','','','','','0','','','550042741','','UT','','12573062','','','','0','','','2109110898','0','0','','0','ZRAD','US52','8','399904752','70','3','','6','S79','A076','','','','','','','','','','','','',null,null,'PA','','','','RV','0','','',null,'GLPCA_INIT2',current_timestamp(),null,null),
    ('110','22516945830','8A','0','0','2022','EUR','','S','6','A','1998866183','4','DE01','9999999900','35','','OP01','3620100','','','','SD00','0','110','','','0','','18.08','18.08','19.41','0',null,'16:17:46','B_BILDE_14','','','',null,null,null,'1413643313','2022','2','W','SD00','VBRK','','D267','','','','','0','','','400001812','','TA','','12245867','','','','0','','','0','0','0','','0','ZSB1','DE01','14','401168633','20','2','','2','M48','A022','','','','','','','','','','','','',null,null,'','','','','RV','0','','',null,'GLPCA_INIT3',current_timestamp(),null,null),
    ('110','22374549229','8A','0','0','2022','USD','KG','H','5','A','1987625350','1','US16','300055','2','SV31','OP01','6380303','','','','RFBU','0','110','','','0','','-813.87','-813.87','-813.87','-856.944',null,'8:21:25','R_CPS_RFC','SVMS:-ENHD-ETRF','','',null,null,null,'4027237290','2022','1','W','RFBU','BKPFF','US162022','Y370','','US16900145','','','0','','','400007375','','','','','','','','0','','','0','0','0','','0','','','','','0','','','0','','','','','','','','','','','','','','',null,null,'OC','','','','HC','0','','',null,'GLPCA_INIT3',current_timestamp(),null,null),
    ('110','21405396193','8A','0','0','2021','CAD','KG','H','10','A','1914480062','1','CA48','800001','2','SV31','OP01','6380303','','','','RFBU','0','110','','','0','','-314.05','-314.05','-254.44','-296.576',null,'8:01:46','R_CPS_RFC','SVMS:-ENHD-ETRF','','',null,null,null,'4012802588','2021','1','W','RFBU','BKPFF','CA482021','C282','','CA48900004','','','0','','','400004665','','','','','','','','0','','','0','0','0','','0','','','','','0','','','0','','','','','','','','','','','','','','',null,null,'OC','','','','HC','0','','',null,'GLPCA_20230812_082950_001.parquet',current_timestamp(),null,null),
    ('110','22097421431','8A','0','0','2022','USD','','S','3','A','1966684858','76','US52','600010','5','SV38','OP01','6355580','','','','SD00','0','110','','','0','','3.92','3.92','3.92','0',null,'3:38:12','B_BILUS52_08','','','',null,null,null,'1409564954','2022','5','W','SD00','VBRK','','U246','','','','','0','','','550039860','','UT','','12573062','','','','0','','','2109111185','0','0','','0','ZRAD','US52','8','399904759','50','3','','5','S79','A076','','','','','','','','','','','','',null,null,'PA','','','','RV','0','','',null,'GLPCA_INIT2',current_timestamp(),null,null),
    ('110','22516947071','8A','0','0','2022','EUR','','S','6','A','1998866280','8','DE01','9999999900','35','','OP01','3620100','','','','SD00','0','110','','','0','','1.77','1.77','1.9','0',null,'16:17:46','B_BILDE_14','','','',null,null,null,'1413643400','2022','4','W','SD00','VBRK','','D267','','','','','0','','','400001952','','TA','','12245867','','','','0','','','0','0','0','','0','ZSB1','DE01','14','401169805','40','2','','4','M48','A022','','','','','','','','','','','','',null,null,'','','','','RV','0','','',null,'GLPCA_INIT3',current_timestamp(),null,null),
    ('110','22374549653','8A','0','0','2022','TRY','KG','S','5','A','1987625521','2','TR04','310001','2','SV30','OP01','6380203','','','','RFBU','0','110','','','0','','313442.85','313442.85','21122.91','17434.403',null,'8:21:29','R_CPS_RFC','SVMS:-Offset-ENHDETRF','','',null,null,null,'4009280932','2022','2','W','RFBU','BKPFF','TR042022','T102','','TR04900002','','','0','','','400006000','','','','','','','','0','','','0','0','0','','0','','','','','0','','','0','','','','','','','','','','','','','','',null,null,'OC','','','','HC','0','','',null,'GLPCA_INIT3',current_timestamp(),null,null),
    ('110','21405403705','8A','0','0','2021','USD','','S','10','A','1914477377','2','US16','800001','5','OP20','OP01','6920450','','','','SD00','0','110','','','0','','23.67','23.67','23.67','0',null,'8:02:18','B_BILUS16_14','','','',null,null,null,'1402769205','2021','1','W','SD00','VBRK','','Y017','','US16000841','','','0','','','400007375','','UT','','12308038','','','','0','','','0','0','0','','0','ZTF3','US16','14','262627379','10','2','','1','LLB','A022','','','','','','','','','','','','',null,null,'OC','','','','RV','0','','',null,'GLPCA_20230812_154003_001.parquet',current_timestamp(),null,null)""")
    


# COMMAND ----------

# DBTITLE 1,mseg
spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.mseg
    VALUES 
    ('110','3198096407','2021','1','0','0','0','0','','','','','','','','','','','','','','0','0','','','','0','0','0','','0','','0','','0','','0','','','0','0','','0','0','','0','','','','','','','','','','','','','','','','','','','0','','','','','0','','0','0','0','','0','','','','','','','','','','','','','','0','','','','','0','0','0','','','','','0','','0','0','0','','','','','0','','0','','0','0','0','','','','0','','','','0','0','','0',null,'0','0','','','','','','','','','','','0','0','','',null,'0',null,'','0','','','','0','0','','','','','',null,'','','0','0','','','','','','','','','','','0','','',null,null,'0:00:00','','','','','0','','','','',null,'0:00:00','0','','0','','','0','0',null,'0:00:00',null,'','0','','','','','','0','0','','','','','','','','0','',null,'','','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','','0','','','','','','','','','','','','','','','','','','','','',null,null,'','','','0','','','','0','','','','','','','','','','0','0',null,null,'','0','0','0','','','','',null,null,'','','','0','','','','1','A','2022-07-04T07:06:34.000+0000','1001','','','','','','','','','','','','','','','','','','','','','','','1001000000','10010720920630','10012021','10010001','','','','','','1001ZDIC_OICITYC_ID','','','','','','','','','','','10010720920630','','','','','','','','','','','','','','0','','','','','MSEG_2021_3199800000.parquet',current_timestamp(),null,null),
    ('110','5073211467','2021','1','1','0','0','0','101','','550053335','T026','WHS1','UT','X','','','','','','','0','0','','S','THB','18048.54','0','18048.54','','0','UT','44','KAR','44','KAR','0','','','0','0','','0','0','','0','','','','','','','OP01','','','','','6377762','','','','','','','2021','','','GB30','','0','','0','0','0','','0','2','','','','','','','','','F','','','','0','','','','','0','0','0','','','','','0','','0','1','0','','','','','0','600010','0','','0','0','1','','','6350120','44','KAR','','','0','0','','0',null,'0','0','600010','','','','550053335','','MF01','WF01','X','X','402','164898.01','S','',null,'0',null,'','0','','','6','1','0','','','','','',null,'','','0','0','','','','','','','','2','','','0','','WF',null,null,'10:43:26','THF247','','MB31','','0','','','','',null,'0:00:00','0','','0','','','0','0',null,'17:43:26',null,'','0','','','','','','0','0','','','','','','','','0','',null,'','','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','','0','','','','','','TH','','','','TH','','','','','','T026','#','','','',null,null,'','','','0','','','','20210408104326','','','','UT','UT','HC','UA','','','100','0',null,null,'L','528','0','0','','','','',null,null,'','','','0','','','','1','C','2021-04-08T10:44:28.000+0000','1001','','','1001WF','','1001101','','','','','1001T026','','1001HC','','10010720920630','1001UT','','','1001THF247','1001TH','1001TH','','','1001000000','10010720920630','10012021','10010001','1001UA','','','1001GB30','','1001ZDIC_OICITYC_ID','1001UT','1001UT','1001WHS1','','1001WHS1T026','','','1001KAR','1001TH','1001TH','10010720920630','','','','','','','','','','','','','','0','','','','550053335','MSEG_2021_5076000000.parquet',current_timestamp(),null,null),
    ('110','3198096775','2021','3','0','0','0','0','','','','','','','','','','','','','','0','0','','','','0','0','0','','0','','0','','0','','0','','','0','0','','0','0','','0','','','','','','','','','','','','','','','','','','','0','','','','','0','','0','0','0','','0','','','','','','','','','','','','','','0','','','','','0','0','0','','','','','0','','0','0','0','','','','','0','','0','','0','0','0','','','','0','','','','0','0','','0',null,'0','0','','','','','','','','','','','0','0','','',null,'0',null,'','0','','','','0','0','','','','','',null,'','','0','0','','','','','','','','','','','0','','',null,null,'0:00:00','','','','','0','','','','',null,'0:00:00','0','','0','','','0','0',null,'0:00:00',null,'','0','','','','','','0','0','','','','','','','','0','',null,'','','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','','0','','','','','','','','','','','','','','','','','','','','',null,null,'','','','0','','','','0','','','','','','','','','','0','0',null,null,'','0','0','0','','','','',null,null,'','','','0','','','','3','A','2022-07-04T07:06:34.000+0000','1001','','','','','','','','','','','','','','','','','','','','','','','1001000000','10010720920630','10012021','10010003','','','','','','1001ZDIC_OICITYC_ID','','','','','','','','','','','10010720920630','','','','','','','','','','','','','','0','','','','','MSEG_2021_3199800000.parquet',current_timestamp(),null,null),
    ('110','5073211577','2021','1','1','0','0','0','101','','550052837','D133','WHS1','11729533','F','','','','','','','0','0','','S','EUR','895.23','0','895.23','','0','UT','45','KAR','45','KAR','0','','','0','0','','0','0','','0','X','','','','','','OP01','','','','','6365301','','','','','','','2021','','','DE01','','0','','0','0','0','','0','2','','','','','','','','','F','','','','0','','','','','0','0','0','','','','','0','','0','1','0','','','','','0','600010','0','','0','0','1','','','6350120','45','KAR','','','0','0','','0',null,'0','0','600010','','','','550052837','','MF01','WF01','X','X','0','0','S','',null,'0',null,'','0','','','6','1','0','','','','','',null,'','','0','0','','','','','','','','F','','','0','','WF',null,null,'10:47:23','TPDEG803','','MB31','','0','','','','',null,'0:00:00','0','','0','','','0','0',null,'10:47:22',null,'','0','','','UT','','','0','0','','','','','','','','0','',null,'','','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','','0','','','','','','DE','','','','DE','','','','','','D133','#','','','',null,null,'','','','0','','','','20210408104326','','','','UT','UT','HC','UA','','','100','0',null,null,'KG','605.52','0','0','','','','',null,null,'','','','0','','','','1','C','2021-04-08T10:47:58.000+0000','1001','','','1001WF','','1001101','','','','','1001D133','','1001HC','','10010720920630','1001UT','','','1001TPDEG803','1001DE','1001DE','','','1001000000','10010720920630','10012021','10010001','1001UA','','','1001DE01','','1001ZDIC_OICITYC_ID','1001UT','1001UT','1001WHS1','','1001WHS1D133','','','1001KAR','1001DE','1001DE','10010720920630','','','','','','','','','','','','','','0','','','','550052837','MSEG_2021_5076000000.parquet',current_timestamp(),null,null),
    ('110','3198097557','2021','11','0','0','0','0','','','','','','','','','','','','','','0','0','','','','0','0','0','','0','','0','','0','','0','','','0','0','','0','0','','0','','','','','','','','','','','','','','','','','','','0','','','','','0','','0','0','0','','0','','','','','','','','','','','','','','0','','','','','0','0','0','','','','','0','','0','0','0','','','','','0','','0','','0','0','0','','','','0','','','','0','0','','0',null,'0','0','','','','','','','','','','','0','0','','',null,'0',null,'','0','','','','0','0','','','','','',null,'','','0','0','','','','','','','','','','','0','','',null,null,'0:00:00','','','','','0','','','','',null,'0:00:00','0','','0','','','0','0',null,'0:00:00',null,'','0','','','','','','0','0','','','','','','','','0','',null,'','','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','','0','','','','','','','','','','','','','','','','','','','','',null,null,'','','','0','','','','0','','','','','','','','','','0','0',null,null,'','0','0','0','','','','',null,null,'','','','0','','','','11','A','2022-07-04T07:06:34.000+0000','1001','','','','','','','','','','','','','','','','','','','','','','','1001000000','10010720920630','10012021','10010011','','','','','','1001ZDIC_OICITYC_ID','','','','','','','','','','','10010720920630','','','','','','','','','','','','','','0','','','','','MSEG_2021_3199800000.parquet',current_timestamp(),null,null),
    ('110','5073211711','2021','1','1','0','0','0','Z11','','400006910','Y356','0','UT','','','','','69052725','','','0','0','','S','USD','16605.6','0','16290.65','','0','UT','25739.537','KG','9071','UG6','9071','UG6','4534697666','10','2021','5073211711','1','0','','0','X','','','','','','OP01','','','','','','','','','','','','2021','','','US16','','0','','0','0','0','','0','2','','','','','','','','','B','','','','0','','','','','0','0','0','','','','','0','','0','1','0','','','','','0','300057','0','','0','0','0','','','','215.976','BB6','','69052725','0','0','','0',null,'0','0','300057','','','','400006910','','ME01','WE01','X','X','0','0','S','',null,'0',null,'','0','','','','1','0','','','','','',null,'','','0','0','','','','','','','','F','','','0','','WE',null,null,'10:50:14','B_MODALL','','MB01','720920630','10','','','','',null,'0:00:00','0','','0','','','0','0',null,'0:00:00',null,'','0','','','','','','0','0','','','','','','','','0','',null,'','','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','720920630','10','X','','1390','155','MI','US','1390','155','MI','US','','','2019','','1','Y356','#','','','',null,null,'','','','0','467212','','','20210408104326','','','','TA','UT','AA','BS','X','','100','0',null,null,'UG6','9071','0','0','','','','',null,null,'','','','0','','','','1','C','2021-04-08T10:51:35.000+0000','1001','10010720920630','','1001WE','','1001Z11','10010720920630','','','100101','1001Y356','','1001AA','','10010720920630','1001UT','','','1001B_MODALL','1001US','1001US','','','1001000010','10010720920630','10012021','10010001','1001BS','10010720920630','','1001US16','10011390','1001ZDIC_OICITYC_ID','1001TA','1001UT','10010000','','10010000Y356','','','1001UG6','1001USMI1390','1001USMI1390','10010720920630','','','','','','','','','','','','','','0','','','','400006910','MSEG_2021_5076000000.parquet',current_timestamp(),null,null),
    ('110','3198099913','2021','3','0','0','0','0','','','','','','','','','','','','','','0','0','','','','0','0','0','','0','','0','','0','','0','','','0','0','','0','0','','0','','','','','','','','','','','','','','','','','','','0','','','','','0','','0','0','0','','0','','','','','','','','','','','','','','0','','','','','0','0','0','','','','','0','','0','0','0','','','','','0','','0','','0','0','0','','','','0','','','','0','0','','0',null,'0','0','','','','','','','','','','','0','0','','',null,'0',null,'','0','','','','0','0','','','','','',null,'','','0','0','','','','','','','','','','','0','','',null,null,'0:00:00','','','','','0','','','','',null,'0:00:00','0','','0','','','0','0',null,'0:00:00',null,'','0','','','','','','0','0','','','','','','','','0','',null,'','','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','','0','','','','','','','','','','','','','','','','','','','','',null,null,'','','','0','','','','0','','','','','','','','','','0','0',null,null,'','0','0','0','','','','',null,null,'','','','0','','','','3','A','2022-07-04T07:06:34.000+0000','1001','','','','','','','','','','','','','','','','','','','','','','','1001000000','10010720920630','10012021','10010003','','','','','','1001ZDIC_OICITYC_ID','','','','','','','','','','','10010720920630','','','','','','','','','','','','','','0','','','','','MSEG_2021_3199800000.parquet',current_timestamp(),null,null),
    ('110','5073212034','2021','1','1','0','0','0','101','','900003861','N506','','','','','','','68049987','','','0','0','','S','EUR','933.73','0','0','','0','','1','EA','1','EA','1','EA','4533586031','100','2021','5073212034','1','0','','0','X','','','NLJKQF','FLESSENREK AN.HS 7 MLO','','OP01','','','','','83730460','','','','','','','2021','','','NL02','','0','','0','0','0','','0','2','','','','','','','','','B','V','','','0','','','','','0','0','0','','','','','0','','0','0','0','','','','','0','200029','0','','0','0','0','','','7480800','1','EA','','','0','0','','0',null,'0','0','800041','','','','900003861','','ME02','WE06','','','0','0','V','',null,'0',null,'','0','','','','1','0','','','','','',null,'','','0','0','','','','','','','','F','','','0','','WE',null,null,'11:02:41','SNRSTY','2 181 601 993','MIGO_GR','','0','','','','',null,'0:00:00','0','','0','','','0','0',null,'11:02:00',null,'','0','','','','','','0','0','','','','','','','','0','',null,'','','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','','0','','','','','','NL','','','','NL','','','','','','N506','#','','','',null,null,'','','','0','','','','20210408104326','','','','','','','','','','0','0',null,null,'','0','0','0','','','','',null,null,'','','','0','','','','1','C','2021-04-08T11:02:57.000+0000','1001','','','1001WE','','1001101','10010720920630','','','','1001N506','','','','10010720920630','','','1001NLJKQF','1001SNRSTY','1001NL','1001NL','','','1001000000','10010720920630','10012021','10010001','','10010720920630','','1001NL02','','1001ZDIC_OICITYC_ID','','','','','1001N506','','','1001EA','1001NL','1001NL','10010720920630','','','','','','','','','','','','','','0','','','','900003861','MSEG_2021_5076000000.parquet',current_timestamp(),null,null),
    ('110','3198101012','2021','5','0','0','0','0','','','','','','','','','','','','','','0','0','','','','0','0','0','','0','','0','','0','','0','','','0','0','','0','0','','0','','','','','','','','','','','','','','','','','','','0','','','','','0','','0','0','0','','0','','','','','','','','','','','','','','0','','','','','0','0','0','','','','','0','','0','0','0','','','','','0','','0','','0','0','0','','','','0','','','','0','0','','0',null,'0','0','','','','','','','','','','','0','0','','',null,'0',null,'','0','','','','0','0','','','','','',null,'','','0','0','','','','','','','','','','','0','','',null,null,'0:00:00','','','','','0','','','','',null,'0:00:00','0','','0','','','0','0',null,'0:00:00',null,'','0','','','','','','0','0','','','','','','','','0','',null,'','','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','','0','','','','','','','','','','','','','','','','','','','','',null,null,'','','','0','','','','0','','','','','','','','','','0','0',null,null,'','0','0','0','','','','',null,null,'','','','0','','','','5','A','2022-07-04T07:06:34.000+0000','1001','','','','','','','','','','','','','','','','','','','','','','','1001000000','10010720920630','10012021','10010005','','','','','','1001ZDIC_OICITYC_ID','','','','','','','','','','','10010720920630','','','','','','','','','','','','','','0','','','','','MSEG_2021_3199800000.parquet',current_timestamp(),null,null),
    ('110','5073212700','2021','1','1','0','0','0','101','','400004707','C143','113','UT','','','','','69038954','','','0','0','','S','CAD','0','0','0','','0','UT','3160.492','KG','3160.492','KG','3724','L15','4534698253','10','0','','0','0','','0','X','','','','','','OP01','','','','','','','','','','','','2021','','','CA48','','0','','0','0','0','','0','2','','','','','','','','','B','','','','0','','','','','0','0','0','','','','','0','','0','1','0','','','','','0','310001','0','','0','0','0','','','','3724','L15','','69038954','0','0','','0',null,'0','0','310001','','','','400004707','','ME01','WE01','X','X','25520881.55','0','S','',null,'0',null,'','0','','','','1','0','','','','','',null,'','','0','0','','','','','','','','F','','','0','','WE',null,null,'11:24:21','B_MODALL','','MB01','','0','','','','',null,'0:00:00','0','','0','','','0','0',null,'11:00:08',null,'','0','','','','','','0','0','','','1201987497','69038954','3','X','ZTI','0','',null,'','1001312','','','','0','','0','','','','','','0','','0','0','0','0','0','','0','','','0','','','','FRT','MB','CA','','','MB','CA','','','29,702,971','','1','C143','#','','','',null,null,'','','','0','1156250','','','20210408104326','','','92941','UT','UT','HC','IA','','','100','0',null,null,'L15','3724','0','0','','','','',null,null,'','','','0','','','','1','C','2021-04-08T11:25:25.000+0000','1001','','','1001WE','10010720920630','1001101','10010720920630','','','100101','1001C143','','1001HC','','10010720920630','1001UT','','','1001B_MODALL','1001CA','1001CA','','','1001000000','10010720920630','10012021','10010001','1001IA','10010720920630','','1001CA48','','1001ZDIC_OICITYC_ID','1001UT','1001UT','10010113','','10010113C143','','','1001KG','1001CAMB','1001CAMB','10010720920630','','','','','','','','','','','','','','0','','','','400004707','MSEG_2021_5076000000.parquet',current_timestamp(),null,null)""")

# COMMAND ----------

# DBTITLE 1,dataValidation_Control_table
spark.sql(f"""INSERT INTO `{uc_catalog}`.`{raw_schema}`.dataValidation_Control_table
    VALUES 
    ('MSEG','`/CWM/MENGE`','CPUDT_MKPF',790,770),
    ('MSEG','`/CWM/ERFMG`','CPUDT_MKPF',790,770),
    ('VBRP','`/CWM/MENGE`','ERDAT',790,770),
    ('GLPCA','TSL','CPUDT',790,770),
    ('GLPCA','HSL','CPUDT',790,770),
    ('GLPCA','KSL','CPUDT',790,770),
    ('GLPCA','MSL','CPUDT',790,770),
    ('MSEG','PALAN','CPUDT_MKPF',790,770),
    ('MSEG','DMBTR','CPUDT_MKPF',790,770),
    ('MSEG','BNBTR','CPUDT_MKPF',790,770),
    ('MSEG','BUALT','CPUDT_MKPF',790,770),
    ('MSEG','DMBUM','CPUDT_MKPF',790,770),
    ('MSEG','VKWRA','CPUDT_MKPF',790,770),
    ('MSEG','J_1BEXBASE','CPUDT_MKPF',790,770),
    ('MSEG','OINAVNW','CPUDT_MKPF',790,770),
    ('MSEG','OIFEETOT','CPUDT_MKPF',790,770),
    ('MSEG','OIBASVAL','CPUDT_MKPF',790,770),
    ('MSEG','MENGE','CPUDT_MKPF',790,770),
    ('MSEG','ERFMG','CPUDT_MKPF',790,770),
    ('MSEG','BPMNG','CPUDT_MKPF',790,770),
    ('MSEG','BSTMG','CPUDT_MKPF',790,770),
    ('MSEG','LBKUM','CPUDT_MKPF',790,770),
    ('MSEG','LSMNG','CPUDT_MKPF',790,770),
    ('MSEG','OIGLERF','CPUDT_MKPF',790,770),
    ('MSEG','OIGLSKU','CPUDT_MKPF',790,770),
    ('MSEG','OIGLBPR','CPUDT_MKPF',790,770),
    ('MSEG','OIGLBST','CPUDT_MKPF',790,770),
    ('MSEG','OIH_FOLQTY','CPUDT_MKPF',790,770),
    ('MSEG','OITAXQT','CPUDT_MKPF',790,770),
    ('MSEG','OIFUTQT','CPUDT_MKPF',790,770),
    ('MSEG','OIFUTQT2','CPUDT_MKPF',790,770),
    ('MSEG','OIH_FOLQTY_GI','CPUDT_MKPF',790,770),
    ('MSEG','FSH_DEALLOC_QTY','CPUDT_MKPF',790,770),
    ('MSEG','PBAMG','CPUDT_MKPF',790,770),
    ('MSEG','EXBWR','CPUDT_MKPF',790,770),
    ('MSEG','VKWRT','CPUDT_MKPF',790,770),
    ('MSEG','EXVKW','CPUDT_MKPF',790,770),
    ('MSEG','SALK3','CPUDT_MKPF',790,770),
    ('MSEG','OIOILCON','CPUDT_MKPF',790,770),
    ('MSEG','OIOILCON2','CPUDT_MKPF',790,770),
    ('MSEG','OIBBSWQTY','CPUDT_MKPF',790,770),
    ('VBRP','CMPRE','ERDAT',790,770),
    ('VBRP','SKFBP','ERDAT',790,770),
    ('VBRP','WAVWR','ERDAT',790,770),
    ('VBRP','KZWI1','ERDAT',790,770),
    ('VBRP','KZWI2','ERDAT',790,770),
    ('VBRP','KZWI3','ERDAT',790,770),
    ('VBRP','KZWI4','ERDAT',790,770),
    ('VBRP','KZWI5','ERDAT',790,770),
    ('VBRP','KZWI6','ERDAT',790,770),
    ('VBRP','BONBA','ERDAT',790,770),
    ('VBRP','MWSBP','ERDAT',790,770),
    ('VBRP','NRAB_VALUE','ERDAT',790,770),
    ('VBRP','OITAXLOD','ERDAT',790,770),
    ('VBRP','OITAXISS','ERDAT',790,770),
    ('VBRP','OITAXINV','ERDAT',790,770),
    ('VBRP','OIFEETOT','ERDAT',790,770),
    ('VBRP','FKIMG','ERDAT',790,770),
    ('VBRP','SMENG','ERDAT',790,770),
    ('VBRP','FKLMG','ERDAT',790,770),
    ('VBRP','LMENG','ERDAT',790,770),
    ('VBRP','OIH_FOLQTY','ERDAT',790,770),
    ('VBRP','OIFKIMG','ERDAT',790,770),
    ('VBRP','OISMENG','ERDAT',790,770),
    ('VBRP','OIFKLMG','ERDAT',790,770),
    ('VBRP','OILMENG','ERDAT',790,770),
    ('VBRP','OIHFOLQTY','ERDAT',790,770),
    ('VBRP','OIVMENG','ERDAT',790,770),
    ('VBRP','OIU_A_H_VAL_FCT','ERDAT',790,770),
    ('VBRP','NETWR','ERDAT',790,770),
    ('VBRP','BRTWR','ERDAT',790,770),
    ('VBRP','NTGEW','ERDAT',790,770),
    ('VBRP','BRGEW','ERDAT',790,770),
    ('VBRP','VOLUM','ERDAT',790,770),
    ('VBRP','OILIMETF','ERDAT',790,770),
    ('VBRP','OINTGEW','ERDAT',790,770),
    ('VBRP','OIBRGEW','ERDAT',790,770),
    ('VBRP','OIVOLUM','ERDAT',790,770),
    ('VBRP','OILIKWTF','ERDAT',790,770),
    ('VBRP','UMVKZ','ERDAT',790,770),
    ('VBRP','UMVKN','ERDAT',790,770),
    ('VBRP','OIOILCON','ERDAT',790,770),
    ('VBRP','KURSK','ERDAT',790,770),
    ('VBRP','STCUR','ERDAT',790,770),
    ('VBRP','AKKUR','ERDAT',790,770)
    """)

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

# DBTITLE 1,MARM
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
