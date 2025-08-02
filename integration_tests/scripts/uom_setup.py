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

euh_table_list = ['vbrp','vbrk','t006','t006a','mara','marm']
if env == "dev":
    for table_name in euh_table_list:
        spark.sql(f"DROP TABLE IF EXISTS `{uc_catalog}`.`{euh_schema}`.table_name")

# COMMAND ----------

eh_table_list = ['md_mm_material_uom_join','md_mm_uom_text_join']
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
            OPFLAG string
            )
        USING delta
        LOCATION '{VBRP_table_path}' """)
    

# COMMAND ----------


if env == "dev": 
    VBRK_table_path = euh_folder_path + "/VBRK"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.VBRK
        (MANDT string,
            VBELN string,
            FKART string,
            FKTYP string,
            VBTYP string,
            WAERK string,
            VKORG string,
            VTWEG string,
            KALSM string,
            KNUMV string,
            VSBED string,
            FKDAT date,
            BELNR string,
            GJAHR string,
            POPER string,
            KONDA string,
            KDGRP string,
            BZIRK string,
            PLTYP string,
            INCO1 string,
            INCO2 string,
            EXPKZ string,
            RFBSK string,
            MRNKZ string,
            KURRF decimal(9,5),
            CPKUR string,
            VALTG string,
            VALDT date,
            ZTERM string,
            ZLSCH string,
            KTGRD string,
            LAND1 string,
            REGIO string,
            COUNC string,
            CITYC string,
            BUKRS string,
            TAXK1 string,
            TAXK2 string,
            TAXK3 string,
            TAXK4 string,
            TAXK5 string,
            TAXK6 string,
            TAXK7 string,
            TAXK8 string,
            TAXK9 string,
            NETWR decimal(15,2),
            ZUKRI string,
            ERNAM string,
            ERZET string,
            ERDAT date,
            STAFO string,
            KUNRG string,
            KUNAG string,
            MABER string,
            STWAE string,
            EXNUM string,
            STCEG string,
            AEDAT date,
            SFAKN string,
            KNUMA string,
            FKART_RL string,
            FKDAT_RL date,
            KURST string,
            MSCHL string,
            MANSP string,
            SPART string,
            KKBER string,
            KNKLI string,
            CMWAE string,
            CMKUF decimal(9,5),
            HITYP_PR string,
            BSTNK_VF string,
            VBUND string,
            FKART_AB string,
            KAPPL string,
            LANDTX string,
            STCEG_H string,
            STCEG_L string,
            XBLNR string,
            ZUONR string,
            MWSBK decimal(13,2),
            LOGSYS string,
            FKSTO string,
            XEGDR string,
            RPLNR string,
            LCNUM string,
            J_1AFITP string,
            KURRF_DAT date,
            AKWAE string,
            AKKUR decimal(9,5),
            KIDNO string,
            BVTYP string,
            NUMPG string,
            BUPLA string,
            VKONT string,
            FKK_DOCSTAT string,
            NRZAS string,
            SPE_BILLING_IND string,
            VTREF string,
            FK_SOURCE_SYS string,
            FKTYP_CRM string,
            STGRD string,
            VBTYP_EXT string,
            DPC_REL string,
            OICHEADOFF string,
            OIC_INPD string,
            OINETCYC string,
            OIEXGNUM string,
            OIEXGTYP string,
            OIFEEPD string,
            OIRIINVDOC string,
            OIRIPBLNR string,
            OIRICOMMCAL string,
            OIRIDCLAIM string,
            OIRIPCTOT decimal(15,2),
            OIRICHIND string,
            OIRMATHANDGRP string,
            OIR_CHOBJ string,
            OICSDPORGINV string,
            OICSDPVERNO string,
            OIU_DN_NO string,
            OIU_WL_NO string,
            OIU_WC_NO string,
            OIU_MP_NO string,
            OIU_MAJPD_CD string,
            OIU_VNAME string,
            OIU_DOI_NO string,
            OIU_VBELN string,
            OIU_PPA_RSN_CD string,
            OIU_PRCS_DT date,
            OIU_FREQ_CD string,
            MNDID string,
            PAY_TYPE string,
            SEPON string,
            MNDVG string,
            SPPAYM string,
            SPPORD string,
            LAST_ACTION_CD string,
            LAST_DTM timestamp,
            SYSTEM_ID string,
            ZCUR_ID string,
            ZFKART_ID string,
            ZSH_COMP_ID string,
            ZFKTYP_ID string,
            ZZTERM_ID string,
            ZVTWEG_ID string,
            ZKTGRD_ID string,
            ZOIEXGNUM_ID string,
            ZINCO1_ID string,
            ZERNAM_ID string,
            ZDESTINATION_CTRY_ID string,
            ZPLS_SUP_CD string,
            ZGST_PART_CD string,
            ZGST_PART_ID string,
            J_1TPBUPL string,
            INCOV string,
            INCO2_L string,
            INCO3_L string,
            GLO_LOG_REF1_HD string,
            yrmono string,
            ingested_at timestamp,
            AEDATTM timestamp,
            OPFLAG string           )
        USING delta
        LOCATION '{VBRK_table_path}' """)
    

# COMMAND ----------

# DBTITLE 1,VBRP data

if env == "dev":
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.VBRP
    VALUES('110','0278267093','000001','000000',30000.000,'L',100,119,'KG',0.000,24481.032,25210.084,24481.032,24481.032,'KG',29530.800,'L15','','2023-01-20','2023-01-20',54.63900,0.00,'','000000','0743530643','000010','J','0278056463','000010','','000000000400008591','BF AGO 50ppmS B2 3 Udy Umk Diesoline PH','','','ZPDC103B','ZKBF','','002D5004001176ZZZZ','3111','','02','000001','P133','PH','','','','1','','','','','','','','','','','X',0.00,'01','02','','','','','BLV','A011','02','','B_BILPH01_02','2023-01-25','16:10:06','TA','9999','',0.00,0.00,0.00,0.00,0.00,335.17,0.00,1.00000,'','','','0000100055','181','','','','002','','','','','','000000000400008591',0.00,'OP01','2220403240','00000000','','',0.00,'','000000000000000000','000000000000000000','','000000','X','','','','04','','PH','','02','07B','PH01','02',null,'','','C',null,'','000000','','','','',0.00,'','','','',0.0,'',0.0,'','','','','','','','','',0.00,'','000000','','2023-01-20','','','01','','','','','','',null,'','','','','','','','',0.00000,'','','000000','','','','','','','','','',0.00,'','0','','',null,'','','',0.000,'','0000000000','00000000','','000','000',null,null,'','','','','X','','','X','','','','07B','PH','','','','PH','','','','1510227858','','','','','','1','TA','AA','KO','TA','',100.00,null,null,'X','X','EX','0',0.00,0.00,0.00,'','','','','','','','','000000','10:22:00',null,'','','',0.00,'2023-01-20','','','','',0.000,'','000','','','000','','X','X','X','X','X','X','X','X','X',0.00,-29132.428,30000.000,0.000,24481.032,25210.084,24481.032,24481.032,29530.800,0.000,25210.084,'','','',0.000,'',0.0,'',null,null,'','','','','000000000000','000','','','','0000000','','','0000000000','00000000','0','','0278267093000001','000000','0743530643000010','0278056463000010','1001','2023-01-25T16:14:11.000','C','1001AA','1001L','10010743530643','','','1001KO','100102','1001PH','1001PH','','','10010000100055','1001OP01','10010278056463','','1001000000000400008591','1001P133','1001TA','1001KG','10010278267093000001','','','','','','','','','','000000000400008591','','','VBRP_20231031_162534_001.parquet','2025-04-22T04:36:32.415',null,null),('110','0276996652','000001','000000',20000.000,'L',100,102,'KG',0.000,19391.996,19607.843,19391.996,19391.996,'KG',19737.400,'L15','','2022-12-09','2022-12-09',1.00000,0.00,'','000000','0742158050','000010','J','0276522411','000010','','000000000400003520','SH RFO 3.0%S 230 FO Plus Philippines','','','ZPDC105B','ZKBF','','002D1872001176ZZZZ','3117','','02','000001','P139','PH','','','','1','','','','','','','','','','','X',0.00,'01','02','','','','','BLV','A011','02','','B_BILPH01_02','2022-12-16','16:09:38','TA','9999','',0.00,0.00,0.00,0.00,0.00,0.00,0.00,1.00000,'','','','0000100055','181','','','','002','','','','','','000000000400003520',0.00,'OP01','2207246093','00000000','','',0.00,'','000000000000000000','000000000000000000','','000000','X','','','','07','','PH','','02','06E','PH01','02',null,'','','C',null,'','000000','','','','',0.00,'','','','',0.0,'',0.0,'','','','','','','','','',0.00,'','000000','','2022-12-09','','','00','','','','','','',null,'','','','','','','','',0.00000,'','','000000','','','','','','','','','',0.00,'','0','','',null,'','','',0.000,'','0000000000','00000000','','000','000',null,null,'','','','','X','000014107','','X','','','','06E','PH','','','','PH','','','Generic Tanker','','01','','','','','1','TA','AA','NA','TA','',100.00,null,null,'X','X','EX','0',0.00,0.00,0.00,'','','','','','','','','000000','15:36:00',null,'','','',0.00,'2022-12-03','','','','',0.000,'','000','','','000','','X','X','X','X','X','X','X','X','X',0.00,-19779.836,20000.000,0.000,19391.996,19607.843,19391.996,19391.996,19737.400,0.000,19607.843,'','','',0.000,'',0.0,'',null,null,'','','','','000000000000','000','','','','0000000','','','0000000000','00000000','0','','0276996652000001','000000','0742158050000010','0276522411000010','1001','2022-12-16T16:12:35.000','C','1001AA','1001L','10010742158050','','100101','1001NA','100102','1001PH','1001PH','','','10010000100055','1001OP01','10010276522411','','1001000000000400003520','1001P139','1001TA','1001KG','10010276996652000001','','','','','','','','','','000000000400003520','','','VBRP_20231031_162534_001.parquet','2025-04-22T04:36:32.415',null,null),('110','0277488038','000001','000000',30000.000,'L',100,119,'KG',0.000,24359.196,25210.084,24359.196,24359.196,'KG',29526.300,'L15','','2022-12-29','2022-12-29',55.81500,0.00,'','000000','0742799337','000010','J','0277351424','000010','','000000000400008591','BF AGO 50ppmS B2 3 Udy Umk Diesoline PH','','','ZPDC103B','ZKBF','','002D5004001176ZZZZ','3111','','02','000001','P133','PH','','','','1','','','','','','','','','','','X',0.00,'01','02','','','','','BLV','A011','02','','PHRVJ7','2023-01-02','07:49:54','TA','9999','',0.00,0.00,0.00,0.00,0.00,328.11,0.00,1.00000,'','','','0000100055','181','','','','002','','','','','','000000000400008591',0.00,'OP01','2212719105','00000000','','',0.00,'','000000000000000000','000000000000000000','','000000','X','','','','04','','PH','','02','07B','PH01','02',null,'','','C',null,'','000000','','','','',0.00,'','','','',0.0,'',0.0,'','','','','','','','','',0.00,'','000000','','2022-12-29','','','01','','','','','','',null,'','','','','','','','',0.00000,'','','000000','','','','','','','','','',0.00,'','0','','',null,'','','',0.000,'','0000000000','00000000','','000','000',null,null,'','','','','X','','','X','','','','07B','PH','','','','PH','','','','1510226252','','','','','','1','TA','AA','KO','TA','',100.00,null,null,'X','X','EX','0',0.00,0.00,0.00,'','','','','','','','','000000','09:00:00',null,'','','',0.00,'2022-12-29','','','','',0.000,'','000','','','000','','X','X','X','X','X','X','X','X','X',0.00,-28987.443,30000.000,0.000,24359.196,25210.084,24359.196,24359.196,29526.300,0.000,25210.084,'','','',0.000,'',0.0,'',null,null,'','','','','000000000000','000','','','','0000000','','','0000000000','00000000','0','','0277488038000001','000000','0742799337000010','0277351424000010','1001','2023-01-02T07:55:59.000','C','1001AA','1001L','10010742799337','','','1001KO','100102','1001PH','1001PH','','','10010000100055','1001OP01','10010277351424','','1001000000000400008591','1001P133','1001TA','1001KG','10010277488038000001','','','','','','','','','','000000000400008591','','','VBRP_20231031_162534_001.parquet','2025-04-22T04:36:32.415',null,null),('110','0279141431','000001','000000',30000.000,'L',100,119,'KG',0.000,24481.032,25210.084,24481.032,24481.032,'KG',29530.800,'L15','','2023-02-07','2023-02-07',54.84800,0.00,'','000000','0744169429','000010','J','0278640872','000010','','000000000400008591','BF AGO 50ppmS B2 3 Udy Umk Diesoline PH','','','ZPDC103B','ZKBF','','002D5004001176ZZZZ','3111','','02','000001','P133','PH','','','','1','','','','','','','','','','','X',0.00,'01','02','','','','','BLV','A011','02','','B_BILPH01_02','2023-02-20','16:12:46','TA','9999','',0.00,0.00,0.00,0.00,0.00,333.90,0.00,1.00000,'','','','0000100055','181','','','','002','','','','','','000000000400008591',0.00,'OP01','2230366849','00000000','','',0.00,'','000000000000000000','000000000000000000','','000000','X','','','','04','','PH','','02','07B','PH01','02',null,'','','C',null,'','000000','','','','',0.00,'','','','',0.0,'',0.0,'','','','','','','','','',0.00,'','000000','','2023-02-07','','','01','','','','','','',null,'','','','','','','','',0.00000,'','','000000','','','','','','','','','',0.00,'','0','','',null,'','','',0.000,'','0000000000','00000000','','000','000',null,null,'','','','','X','','','X','','','','07B','PH','','','','PH','','','','1510229066','','','','','','1','TA','AA','KO','TA','',100.00,null,null,'X','X','EX','0',0.00,0.00,0.00,'','','','','','','','','000000','10:01:00',null,'','','',0.00,'2023-02-07','','','','',0.000,'','000','','','000','','X','X','X','X','X','X','X','X','X',0.00,-29132.428,30000.000,0.000,24481.032,25210.084,24481.032,24481.032,29530.800,0.000,25210.084,'','','',0.000,'',0.0,'',null,null,'','','','','000000000000','000','','','','0000000','','','0000000000','00000000','0','','0279141431000001','000000','0744169429000010','0278640872000010','1001','2023-02-20T16:14:44.000','C','1001AA','1001L','10010744169429','','','1001KO','100102','1001PH','1001PH','','','10010000100055','1001OP01','10010278640872','','1001000000000400008591','1001P133','1001TA','1001KG','10010279141431000001','','','','','','','','','','000000000400008591','','','VBRP_20231031_162534_001.parquet','2025-04-22T04:36:32.415',null,null)""")

# COMMAND ----------

# DBTITLE 1,VBRK
if env == "dev":
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.VBRK
    VALUES('110','0278267093','ZF2C','L','M','USD','PH01','02','ZCFUEL','0577289966','11','2023-01-20','','0000','000','','','','','','','','C','',54.63900,'','00',null,'Z042','1','09','PH','','','','PH01','6','','','','','','','','',0.00,'00302020743530643    000','B_BILPH01_02','16:10:06','2023-01-25','','0011518211','0011518211','','PHP','','',null,'','','',null,'','','','02','PH00','0011518211','PHP',1.00000,'A','','','','','PH','C','PH','0278267093','',0.00,'C94QAR3110','','','','','','2023-01-20','',0.00000,'0278267093','','000','','','','','','','','','','','','','','','','','','','','','',0.00,'','','','','','','','','','','','','','',null,'','','','','','','','C','2023-10-17T07:59:37.000','1001','1001USD','1001ZF2C','1001PH01','1001L','1001Z042','100102','100109','','','1001B_BILPH01_02','1001PH','','','','','','','','','VBRK_20231031_155902_001.parquet','2025-04-22T04:36:33.431',null,null),('110','0277488038','ZF2C','L','M','USD','PH01','02','ZCFUEL','0574740206','11','2022-12-31','','0000','000','','','','','','','','C','',55.81500,'','00',null,'Z042','1','09','PH','','','','PH01','6','','','','','','','','',0.00,'00302020742799337    000','PHRVJ7','07:49:54','2023-01-02','','0011518211','0011518211','','PHP','','',null,'','','',null,'','','','02','PH00','0011518211','PHP',1.00000,'A','','','','','PH','C','PH','0277488038','',0.00,'C94QAR3110','','','','','','2022-12-31','',0.00000,'0277488038','','000','','','','','','','','','','','','','','','','','','','','','',0.00,'','','','','','','','','','','','','','',null,'','','','','','','','C','2023-10-17T07:59:37.000','1001','1001USD','1001ZF2C','1001PH01','1001L','1001Z042','100102','100109','','','1001PHRVJ7','1001PH','','','','','','','','','VBRK_20231031_155902_001.parquet','2025-04-22T04:36:33.431',null,null),('110','0276996652','ZF2C','L','M','PHP','PH01','02','ZCFUEL','0572811802','10','2022-12-09','','0000','000','','','','','','','','C','',1.00000,'','00',null,'Z042','1','09','PH','','','','PH01','1','','','','','','','','',0.00,'00302020742158050    000','B_BILPH01_02','16:09:38','2022-12-16','','0011517730','0011517730','','PHP','','',null,'','','',null,'','','','02','PH00','0011517730','PHP',1.00000,'A','','','','','PH','C','PH','0276996652','',0.00,'C94QAR3110','','','','','','2022-12-09','',0.00000,'0276996652','','000','','','','','','','','','','','','','','','','','','','','','',0.00,'','','','','','','','','','','','','','',null,'','','','','','','','C','2023-10-17T07:59:37.000','1001','1001PHP','1001ZF2C','1001PH01','1001L','1001Z042','100102','100109','','','1001B_BILPH01_02','1001PH','','','','','','','','','VBRK_20231031_155902_001.parquet','2025-04-22T04:36:33.431',null,null),('110','0279141431','ZF2C','L','M','USD','PH01','02','ZCFUEL','0580344592','11','2023-02-07','','0000','000','','','','','','','','C','',54.84800,'','00',null,'Z042','1','09','PH','','','','PH01','1','','','','','','','','',0.00,'00302020744169429    000','B_BILPH01_02','16:12:46','2023-02-20','','0011518211','0011518211','','PHP','','',null,'','','',null,'','','','02','PH00','0011518211','PHP',1.00000,'A','','','','','PH','C','PH','0279141431','',0.00,'C94QAR3110','','','','','','2023-02-07','',0.00000,'0279141431','','000','','','','','','','','','','','','','','','','','','','','','',0.00,'','','','','','','','','','','','','','',null,'','','','','','','','C','2023-10-17T07:59:37.000','1001','1001USD','1001ZF2C','1001PH01','1001L','1001Z042','100102','100109','','','1001B_BILPH01_02','1001PH','','','','','','','','','VBRK_20231031_155902_001.parquet','2025-04-22T04:36:33.431',null,null)""")

# COMMAND ----------

# DBTITLE 1,T006 create statement

if env == "dev": 
    t006_table_path = euh_folder_path + "/t006"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.t006
        (MANDT string,
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
            ISOCODE string,
            PRIMARY string,
            TEMP_VALUE double,
            TEMP_UNIT string,
            FAMUNIT string,
            PRESS_VAL double,
            PRESS_UNIT string,
            SYSTEM_ID string,
            LAST_DTM timestamp,
            LAST_ACTION_CD string,
            ZMSEHI_ID string,
            yrmono string,
            ingested_at timestamp,
            AEDATTM timestamp,
            OPFLAG string
            )
        USING delta
        LOCATION '{t006_table_path}' """)
    

# COMMAND ----------

# DBTITLE 1,T006
if env == "dev":
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.t006
    VALUES('110','%','X','X',3,'X','','','','PROPOR',1,100,0,0.000000,0,3,'P1','X',0.0,'','',0.0,'','1001','2017-06-23T07:08:09.000','C','1001%','T006_20231031_153250_001.parquet','2025-04-07T11:53:44.724',null,null),('110','%O','X','X',0,'','','','','PROPOR',1,1000,0,0.000000,0,0,'','',0.0,'','',0.0,'','1001','2017-06-23T07:08:09.000','C','1001%O','T006_20231031_153250_001.parquet','2025-04-07T11:53:44.724',null,null),('110','1','X','X',0,'','','','','PROPOR',1,1,0,0.000000,0,0,'','',0.0,'','',0.0,'','1001','2017-06-23T07:08:09.000','C','10011','T006_20231031_153250_001.parquet','2025-04-07T11:53:44.724',null,null),('110','10','X','X',0,'','','','','TIME',86400,1,0,0.000000,0,0,'','',0.0,'','',0.0,'','1001','2017-06-23T07:08:09.000','C','100110','T006_20231031_153250_001.parquet','2025-04-07T11:53:44.724',null,null),('110','22S','X','X',0,'','','','','VISKIN',1,1000000,0,0.000000,0,0,'C17','',0.0,'','',0.0,'','1001','2017-06-23T07:08:09.000','C','100122S','T006_20231031_153250_001.parquet','2025-04-07T11:53:44.724',null,null),('110','2M','X','X',0,'','','','','SPEED',1,1,-2,0.000000,0,0,'2M','',0.0,'','',0.0,'','1001','2017-06-23T07:08:09.000','C','10012M','T006_20231031_153250_001.parquet','2025-04-07T11:53:44.724',null,null),('110','2X','X','X',0,'','','','','SPEED',1,60,0,0.000000,0,0,'2X','',0.0,'','',0.0,'','1001','2017-06-23T07:08:09.000','C','10012X','T006_20231031_153250_001.parquet','2025-04-07T11:53:44.724',null,null),('110','4G','X','X',0,'','','','','VOLUME',1,1,-9,0.000000,0,0,'4G','',0.0,'','',0.0,'','1001','2017-06-23T07:08:09.000','C','10014G','T006_20231031_153250_001.parquet','2025-04-07T11:53:44.724',null,null),('110','4O','X','X',0,'','','','','CAPACI',1,1,-6,0.000000,0,0,'4O','',0.0,'','',0.0,'','1001','2017-06-23T07:08:09.000','C','10014O','T006_20231031_153250_001.parquet','2025-04-07T11:53:44.724',null,null),('110','4T','X','X',0,'','','','','CAPACI',1,1,-12,0.000000,0,0,'4T','',0.0,'','',0.0,'','1001','2017-06-23T07:08:09.000','C','10014T','T006_20231031_153250_001.parquet','2025-04-07T11:53:44.724',null,null)""")

# COMMAND ----------

# DBTITLE 1,T006A create statement

if env == "dev": 
    t006a_table_path = euh_folder_path + "/t006a"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.t006a
        (MANDT string,
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
            ingested_at timestamp,
            AEDATTM timestamp,
            OPFLAG string
            )
        USING delta
        LOCATION '{t006a_table_path}' """)
    

# COMMAND ----------

# DBTITLE 1,T006a
if env == "dev":
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.t006a
    VALUES('110','1','22S','22S','mm2/s','mm2/s','Square millimeter/second','1001','C','2023-09-11T00:19:38.000','T006A_20231214_132918_1.parquet','2025-04-20T18:30:00.699','2023-12-14T11:30:45.076','I'),('110','1','2M','CMS','cm/s','do NOT use','do NOT use','1001','C','2023-09-11T00:19:38.000','T006A_20231214_132918_1.parquet','2025-04-20T18:30:00.699','2023-12-14T11:30:45.093','I'),('110','1','4O','ΜF','µF','µF','Microfarad','1001','C','2023-09-11T00:19:38.000','T006A_20231214_132918_1.parquet','2025-04-20T18:30:00.699','2023-12-14T11:30:45.093','I'),('110','1','4T','IB','pF','do NOT use','do NOT use','1001','C','2023-09-11T00:19:38.000','T006A_20231214_132918_1.parquet','2025-04-20T18:30:00.699','2023-12-14T11:30:45.093','I'),('110','1','A93','GM3','g/m3','do NOT use','do NOT use','1001','C','2023-09-11T00:19:38.000','T006A_20231214_132918_1.parquet','2025-04-20T18:30:00.699','2023-12-14T11:30:45.094','I'),('110','1','ACR','ACR','acre','do NOT use','do NOT use','1001','C','2023-09-11T00:19:38.000','T006A_20231214_132918_1.parquet','2025-04-20T18:30:00.699','2023-12-14T11:30:45.094','I'),('110','1','B/F','B/F','BTUSCF','do NOT use','do NOT use','1001','C','2023-09-11T00:19:38.000','T006A_20231214_132918_1.parquet','2025-04-20T18:30:00.699','2023-12-14T11:30:45.094','I'),('110','1','B34','KD3','kg/dm3','kg/dm3','Kilogram/cubic decimeter','1001','C','2023-09-11T00:19:38.000','T006A_20231214_132918_1.parquet','2025-04-20T18:30:00.699','2023-12-14T11:30:45.077','I'),('110','1','B73','MN','MN','do NOT use','do NOT use','1001','C','2023-09-11T00:19:38.000','T006A_20231214_132918_1.parquet','2025-04-20T18:30:00.699','2023-12-14T11:30:45.094','I'),('110','1','B75','MGO','MOhm','MOhm','Megohm','1001','C','2023-09-11T00:19:38.000','T006A_20231214_132918_1.parquet','2025-04-20T18:30:00.699','2023-12-14T11:30:45.077','I'),('110','1','B84','ΜA','µA','do NOT use','do NOT use','1001','C','2023-09-11T00:19:38.000','T006A_20231214_132918_1.parquet','2025-04-20T18:30:00.699','2023-12-14T11:30:45.094','I'),('110','1','BB6','BB6','BB6','Barrel 60F','Barrel 60°F','1001','C','2023-09-11T00:19:38.000','T006A_20231214_132918_1.parquet','2025-04-20T18:30:00.699','2023-12-14T11:30:45.077','I'),('110','1','BQK','BQK','Bq/kg','do NOT use','do NOT use','1001','C','2023-09-11T00:19:38.000','T006A_20231214_132918_1.parquet','2025-04-20T18:30:00.699','2023-12-14T11:30:45.094','I'),('110','1','C10','RF','mF','do NOT use','do NOT use','1001','C','2023-09-11T00:19:38.000','T006A_20231214_132918_1.parquet','2025-04-20T18:30:00.699','2023-12-14T11:30:45.094','I'),('110','1','C38','M/L','Mol/l','do NOT use','do NOT use','1001','C','2023-09-11T00:19:38.000','T006A_20231214_132918_1.parquet','2025-04-20T18:30:00.699','2023-12-14T11:30:45.095','I')""")

# COMMAND ----------

# DBTITLE 1,MARA create statement

if env == "dev": 
    mara_table_path = euh_folder_path + "/mara"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.mara
        (MANDT string,
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
            ANP string,
            FSH_MG_AT1 string,
            FSH_MG_AT2 string,
            FSH_MG_AT3 string,
            FSH_SEALV string,
            FSH_SEAIM string,
            FSH_SC_MID string,
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
            LAST_ACTION_CD string,
            LAST_DTM timestamp,
            SYSTEM_ID string,
            ZMATNR_ID string,
            ZEXTWG_ID string,
            ZMEINS_ID string,
            ZMTART_ID string,
            ZMATKL_ID string,
            ZPRDHA_CD string,
            ZPRDHA_PS_ID string,
            ZMESC_CD string,
            ZSPART_ID string,
            PSM_CODE string,
            ADSPC_SPC string,
            yrmono string,
            ingested_at timestamp,
            AEDATTM timestamp,
            OPFLAG string
            )
        USING delta
        LOCATION '{mara_table_path}' """)
    

# COMMAND ----------

if env == "dev":
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.MARA
    VALUES('110','000000000000000252','2023-11-09','UPGSN017SH',null,'','K','K','','CH00','1','31XXXX','','EA','','','','','','','','000','','','','','','','',0.000,0.000,'',0.000,'','','','','','','','','','',0.000,'','','','','','','','',0.000,0.000,0.000,'','','','','',0.000,'',0.000,'',0.0,0.0,'','','','','',0,0,'','',null,null,'','','','','','','','','','','',null,null,'','',0,0,0,'',0.000,0,'',0.000,'','000000000000000000','','','','','','','','','','','','','','','','','','00','','','','','','','','','','','','','','','','','','','','','','',0.000,0.0,0.000,0.000,0.000,'','','',0,'','','','','','','','','','','','','','','','','','','','','000000000','','','','','','','00000000','','','','','','','','','','','','','','','000','',0.000,0.000,0.000,0.000,0.000,'00','','','','','','','','','','','','0000000000','0000000000','0000000000','','','','','','','','000','','000','','000','','000','','000','','','','',null,null,null,null,null,null,null,null,null,null,null,null,'',0,'MARA_20231109_103040_1.parquet','2025-04-07T12:43:30.729','2023-11-09T09:47:49.265','I'),('110','000000000000001116','2008-07-27','INAMA7','2022-10-24','PHHPI1','KELVCXQDBAGSZ','KELVCXQDBAGS','X','YPAC','O','ZPDC221P','','KAR','','','','','','','','000','','','','','','','',5.897,5.897,'KG',4.259,'L15','','01','','','0001','','03','','',0.000,'','','','','','','71948211162','UC',28.000,21.000,22.000,'CM','001B8335002W6AA0II','','','',0.000,'',0.000,'',0.0,0.0,'1','','','','',0,0,'','SH01',null,null,'','','','15121520','','','','','','','',null,null,'','',0,0,0,'',0.000,0,'',0.000,'2','000000000000000000','X','','','','','','','','','','GPP','','','','','','','00','','','','','','','','','B','','','','','','','','','','','','','',0.000,0.0,0.000,0.000,0.000,'','','',0,'','','','','','','','','','','','','','','','','','','','','000000000','','','','','','','00000000','','','','KAR','','','','','','','','','','','000','',0.000,0.000,0.000,0.000,0.000,'00','','','','','','','','','','','','0000000000','0000000000','0000000000','','','','','','','','000','','000','','000','','000','','000','','','','','U','2022-10-24T08:16:02.000','1001','1001000000000000001116','100115121520','1001KAR','1001YPAC','1001ZPDC221P','002W6A','1001001B8335002W6AA0II','','100103','',0,'MARA_20231031_155245_001.parquet','2025-04-07T11:53:45.957',null,null),('110','000000000000001745','2008-07-27','INAMA7','2022-11-04','R_CF_MDM','KELVCXQDBAGSZ','KELVCXQDBAGS','','YPAC','O','ZPDC201P','','KAR','','','','','','','','000','','','','','','','',11.113,10.206,'KG',11.356,'L15','','01','','000','0001','','03','','',0.000,'','','','','','','71611817455','UC',34.000,27.000,25.000,'CM','001B2057002V6LA0II','','','',0.000,'',0.000,'',0.0,0.0,'1','','','','',0,0,'','SH01',null,null,'','','','15121501','','','','','','','',null,null,'','',3,48,0,'',0.000,0,'',0.000,'2','000000000000000000','X','','','','','','','','','','000','','','','','','','00','2','','','','','','','','B','','','','','','','','','','','','','',0.000,0.0,0.000,0.000,0.000,'','','',0,'','','','','','','','','','','','','','','','','','','','','000000000','','','','','','','00000000','','','','KAR','','','','','','','','','','','000','',0.000,0.000,0.000,0.000,0.000,'00','','','','','','','','','','','','0000000000','0000000000','0000000000','','','','','','','','000','','000','','000','','000','','000','','','2','','U','2022-11-04T12:34:21.000','1001','1001000000000000001745','100115121501','1001KAR','1001YPAC','1001ZPDC201P','002V6L','1001001B2057002V6LA0II','','100103','',0,'MARA_20231031_155245_001.parquet','2025-04-07T11:53:45.957',null,null),('110','000000000000003382','2008-07-27','INAMA7','2022-05-12','PHNDA5','KELVCXQDBAGSZ','KELVCXQDBAGS','','YPAC','O','ZPDC202P','','KAR','','','','','','','','000','','','','','','','',3.402,3.402,'KG',3.549,'L15','','01','','','0001','','03','','',0.000,'','','','','','','71611833820','UC',20.000,20.000,25.000,'CM','001C7080002WHXA0II','','','',0.000,'',0.000,'',0.0,0.0,'1','','','','',0,0,'','SH01',null,null,'','','','15121504','','','','','','','',null,null,'','',0,0,0,'',0.000,0,'',0.000,'2','000000000000000000','X','','','','','','','','','','000','','','','','','','00','','','','','','','','','B','','','','','','','','','','','','','',0.000,0.0,0.000,0.000,0.000,'','','',0,'','','','','','','','','','','','','','','','','','','','','000000000','','','','','','','00000000','','','','KAR','','','','','','','','','','','000','',0.000,0.000,0.000,0.000,0.000,'00','','','','','','','','','','','','0000000000','0000000000','0000000000','','','','','','','','000','','000','','000','','000','','000','','','2','','U','2022-05-12T14:18:23.000','1001','1001000000000000003382','100115121504','1001KAR','1001YPAC','1001ZPDC202P','002WHX','1001001C7080002WHXA0II','','100103','',0,'MARA_20231031_155245_001.parquet','2025-04-07T11:53:45.957',null,null),('110','000000000000003560','2008-07-27','INAMA7','2017-01-11','PHAARP','KELVCXQDBAGZ','KELVCXQDBAG','X','YBLU','O','ZPDC201B','','KG','','','','','','','','000','','','','','','','2',1.000,1.000,'KG',1.138,'L15','','02','','001','0002','','03','','',0.000,'','','','','','','','',0.000,0.000,0.000,'','001D0541001176A1SJ','','','',0.000,'',0.000,'',0.0,0.0,'1','','','','',0,0,'','SH01',null,null,'','','','15121501','','','','','','','',null,null,'','',0,0,0,'',0.000,0,'',0.000,'1','000000000000000000','X','','','','','','','','','','000','','','','','','','00','','','','','','','','','B','','','','','','','','','','','','','',0.000,0.0,0.000,0.000,0.000,'','','',0,'','','','','','','','','','','','','','','','','','','','','000000000','','','','','','','00000000','','','','KG','','','','','','','','','','','000','',0.000,0.000,0.000,0.000,0.000,'00','','','','','','','','','','','','0000000000','0000000000','0000000000','','','','','','','','000','','000','','000','','000','','000','','LUBES','','','C','2017-06-24T02:49:22.000','1001','1001000000000000003560','100115121501','1001KG','1001YBLU','1001ZPDC201B','001176','1001001D0541001176A1SJ','','100103','',0,'MARA_20231031_155245_001.parquet','2025-04-07T11:53:45.957',null,null),('110','000000000000003716','2008-07-27','INAMA7','2022-10-24','PHHPI1','KELVCXQDBAGSZ','KELVCXQDBAGS','','YPAC','O','ZPDC201P','','EA','','','','','','','','000','','','','','','','',200.038,184.615,'KG',208.197,'L15','','01','','000','0001','','03','','',0.000,'','','','','','','71611937160','UC',87.800,58.500,58.500,'CM','001B2015002VIZA0II','','','',0.000,'',0.000,'',0.0,0.0,'1','','','','',0,0,'','SH01',null,null,'','','','15121501','','','','','','','',null,null,'','',0,0,0,'',0.000,0,'',0.000,'2','000000000000000000','X','','','','','','','','','','000','','','','','','','00','','','','','','','','','B','','','','','','','','','','','','','',0.000,0.0,0.000,0.000,0.000,'','','',0,'','','','','','','','','','','','','','','','','','','','','000000000','','','','','','','00000000','','','','EA','','','','','','','','','','','000','',0.000,0.000,0.000,0.000,0.000,'00','','','','','','','','','','','','0000000000','0000000000','0000000000','','','','','','','','000','','000','','000','','000','','000','','','2','','U','2022-10-24T08:32:58.000','1001','1001000000000000003716','100115121501','1001EA','1001YPAC','1001ZPDC201P','002VIZ','1001001B2015002VIZA0II','','100103','',0,'MARA_20231031_155245_001.parquet','2025-04-07T11:53:45.957',null,null),('110','000000000000003744','2008-07-27','INAMA7','2015-10-08','PHMHE6','KELVCXQDBAGSZ','KELVCXQDBAGS','X','YPAC','O','ZPDC201P','','EA','','','','','','','','000','','','','','','','',18.099,16.738,'KG',18.852,'L15','','01','','000','0001','','03','','',0.000,'','','','','','','71611937443','UC',0.000,0.000,0.000,'','001B2018002VOQA0II','','','',0.000,'',0.000,'',0.0,0.0,'1','','','','',0,0,'','SH01',null,null,'','','','15121501','','','','','','','',null,null,'','',0,0,0,'',0.000,0,'',0.000,'2','000000000000000000','X','','','','','','','','','','000','','','','','','','00','','','','','','','','','B','','','','','','','','','','','','','',0.000,0.0,0.000,0.000,0.000,'','','',0,'','','','','','','','','','','','','','','','','','','','','000000000','','','','','','','00000000','','','','EA','','','','','','','','','','','000','',0.000,0.000,0.000,0.000,0.000,'00','','','','','','','','','','','','0000000000','0000000000','0000000000','','','','','','','','000','','000','','000','','000','','000','','','','','C','2017-06-24T02:52:57.000','1001','1001000000000000003744','100115121501','1001EA','1001YPAC','1001ZPDC201P','002VOQ','1001001B2018002VOQA0II','','100103','',0,'MARA_20231031_155245_001.parquet','2025-04-07T11:53:45.957',null,null),('110','000000000000003746','2008-07-27','INAMA7','2022-10-24','PHHPI1','KELVCXQDBAGSZ','KELVCXQDBAGS','X','YPAC','O','ZPDC201P','','EA','','','','','','','','000','','','','','','','',200.038,184.615,'KG',208.197,'L15','','01','','000','0001','','03','','',0.000,'','','','','','','','',0.000,0.000,0.000,'','001B2018002VIZA0II','','','',0.000,'',0.000,'',0.0,0.0,'1','','','','',0,0,'','SH01',null,null,'','','','15121501','','','','','','','',null,null,'','',0,0,0,'',0.000,0,'',0.000,'2','000000000000000000','X','','','','','','','','','','000','','','','','','','00','','','','','','','','','B','','','','','','','','','','','','','',0.000,0.0,0.000,0.000,0.000,'','','',0,'','','','','','','','','','','','','','','','','','','','','000000000','','','','','','','00000000','','','','EA','','','','','','','','','','','000','',0.000,0.000,0.000,0.000,0.000,'00','','','','','','','','','','','','0000000000','0000000000','0000000000','','','','','','','','000','','000','','000','','000','','000','','','','','U','2022-10-24T08:32:58.000','1001','1001000000000000003746','100115121501','1001EA','1001YPAC','1001ZPDC201P','002VIZ','1001001B2018002VIZA0II','','100103','',0,'MARA_20231031_155245_001.parquet','2025-04-07T11:53:45.957',null,null),('110','000000000000003857','2008-07-27','INAMA7','2023-07-25','MYJYOI','KELVCXQDBAGSZ','KELVCXQDBAGS','','YPAC','O','ZPDC201P','','KAR','','','','','','','','000','','','','','','','',10.945,9.839,'KG',11.356,'L15','','01','','000','0001','','03','','',0.000,'','','','','','','71611838573','UC',34.000,27.000,24.000,'CM','001B0848002V6LA0II','','','',0.000,'',0.000,'',0.0,0.0,'1','','','','',0,0,'','SH01',null,null,'','','','15121501','','','','','','','',null,null,'','',0,0,0,'',0.000,0,'',0.000,'2','000000000000000000','X','','','','','','','','','','000','','','','','','','00','','','','','','','','','B','','','','','','','','','','','','','',0.000,0.0,0.000,0.000,0.000,'','','',0,'','','','','','','','','','','','','','','','','','','','','000000000','','','','','','','00000000','','','','KAR','','','','','','','','','','','000','',0.000,0.000,0.000,0.000,0.000,'00','','','','','','','','','','','','0000000000','0000000000','0000000000','','','','','','','','000','','000','','000','','000','','000','','','2','','U','2023-07-25T16:22:19.000','1001','1001000000000000003857','100115121501','1001KAR','1001YPAC','1001ZPDC201P','002V6L','1001001B0848002V6LA0II','','100103','',0,'MARA_20231031_155245_001.parquet','2025-04-07T11:53:45.957',null,null),('110','000000000000003858','2008-07-27','INAMA7','2022-10-24','PHHPI1','KELVCXQDBAGSZ','KELVCXQDBAGS','','YPAC','O','ZPDC201P','','EA','','','','','','','','000','','','','','','','',200.038,184.615,'KG',208.197,'L15','','01','','000','0001','','03','','',0.000,'','','','','','','71611938587','UC',87.800,58.500,58.500,'CM','001B0848002VIZA0II','','','',0.000,'',0.000,'',0.0,0.0,'1','','','','',0,0,'','SH01',null,null,'','','','15121501','','','','','','','',null,null,'','',0,0,0,'',0.000,0,'',0.000,'2','000000000000000000','X','','','','','','','','','','000','','','','','','','00','','','','','','','','','B','','','','','','','','','','','','','',0.000,0.0,0.000,0.000,0.000,'','','',0,'','','','','','','','','','','','','','','','','','','','','000000000','','','','','','','00000000','','','','EA','','','','','','','','','','','000','',0.000,0.000,0.000,0.000,0.000,'00','','','','','','','','','','','','0000000000','0000000000','0000000000','','','','','','','','000','','000','','000','','000','','000','','','2','','U','2022-10-24T07:28:59.000','1001','1001000000000000003858','100115121501','1001EA','1001YPAC','1001ZPDC201P','002VIZ','1001001B0848002VIZA0II','','100103','',0,'MARA_20231031_155245_001.parquet','2025-04-07T11:53:45.957',null,null)""")


# COMMAND ----------

# DBTITLE 1,MARM

if env == "dev": 
    marm_table_path = euh_folder_path + "/marm"
    spark.sql(f"""CREATE TABLE `{uc_catalog}`.`{euh_schema}`.marm
        (MANDT string,
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
            LAST_DTM timestamp,
            SYSTEM_ID string,
            LAST_ACTION_CD string,
            yrmono string,
            ingested_at timestamp,
            AEDATTM timestamp,
            OPFLAG string
            )
        USING delta
        LOCATION '{marm_table_path}' """)
    

# COMMAND ----------

# DBTITLE 1,MARM
if env == "dev":
    spark.sql(f"""INSERT INTO `{uc_catalog}`.`{euh_schema}`.MARM
    VALUES('110','000000000000001104','Y58',90,1,'','','',0.000,0.000,0.000,'',0.000,'',0.000,'','','0000000000','00','','','','','','',0,0,0.000,'','',null,null,null,'MARM_20231031_154239_001.parquet','2025-04-07T11:53:45.168',null,null),('110','000000000000001110','APU',1,12,'','','',0.000,0.000,0.000,'',0.000,'',0.000,'','','0000000000','00','','','','','','',0,0,0.000,'','',null,null,null,'MARM_20231031_154239_001.parquet','2025-04-07T11:53:45.168',null,null),('110','000000000000001110','EA',1,12,'','71611011105','UC',9.000,5.000,21.000,'CM',0.355,'L15',0.378,'KG','','0000000000','00','','','','','','',0,0,0.000,'','',null,null,null,'MARM_20231031_154239_001.parquet','2025-04-07T11:53:45.168',null,null),('110','000000000000001116','EA',1,12,'','71948011168','UC',0.000,0.000,0.000,'',0.000,'',0.000,'','','0000000000','00','','','','','','',0,0,0.000,'','','2022-10-24T08:16:02.000','1001','C','MARM_20231031_154239_001.parquet','2025-04-07T11:53:45.168',null,null),('110','000000000000001210','APU',10,2082,'','','',0.000,0.000,0.000,'',0.000,'',0.000,'','','0000000000','00','','','','','','',0,0,0.000,'','','2022-10-24T08:18:56.000','1001','C','MARM_20231031_154239_001.parquet','2025-04-07T11:53:45.168',null,null),('110','000000000000001437','KG',1,1,'','','',0.000,0.000,0.000,'',1.147,'L15',1.000,'KG','','0000000000','00','','','','','','',0,0,0.000,'','',null,null,null,'MARM_20231031_154239_001.parquet','2025-04-07T11:53:45.168',null,null),('110','000000000000001437','LB',32523,71701,'','','',0.000,0.000,0.000,'',0.000,'',0.000,'','','0000000000','00','','','','','','',0,0,0.000,'','',null,null,null,'MARM_20231031_154239_001.parquet','2025-04-07T11:53:45.168',null,null),('110','000000000000002012','KG',4001,39672,'','','',0.000,0.000,0.000,'',0.000,'',0.000,'','','0000000000','00','','','','','','',0,0,0.000,'','',null,null,null,'MARM_20231031_154239_001.parquet','2025-04-07T11:53:45.168',null,null),('110','000000000000002012','Y58',84,1,'','','',0.000,0.000,0.000,'',0.000,'',0.000,'','','0000000000','00','','','','','','',0,0,0.000,'','',null,null,null,'MARM_20231031_154239_001.parquet','2025-04-07T11:53:45.168',null,null),('110','000000000000002017','L',1339,15206,'','','',0.000,0.000,0.000,'',0.000,'',0.000,'','','0000000000','00','','','','','','',0,0,0.000,'','',null,null,null,'MARM_20231031_154239_001.parquet','2025-04-07T11:53:45.168',null,null)
""")
