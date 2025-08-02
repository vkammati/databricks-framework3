# Databricks notebook source
# %run ./common_functions

# COMMAND ----------

"""
VBRK VBRP MARA from Curated
"""
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, substring
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

table_name="FACT_SD_SALES_HEADER_LINE_JOIN"
path = eh_folder_path+"/"+table_name

""" Function to filter client as 110 """
def client_filter(df):
    filtered_df = df.filter(df.MANDT==110) 
    return filtered_df   

df_VBRK = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.vbrk")
df_VBRP = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.vbrp") 
df_MARA = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.mara")


df_VBRK = client_filter(df_VBRK)
df_VBRP = client_filter(df_VBRP)
df_MARA = client_filter(df_MARA)

df_VBRK_VBRP=df_VBRK.join(df_VBRP, df_VBRK.VBELN==df_VBRP.VBELN,"leftouter").select(df_VBRK.MANDT,df_VBRK.VBELN,df_VBRK.BUKRS,df_VBRP.POSNR,df_VBRP.FKIMG,df_VBRP.VRKME,df_VBRP.MATNR,df_VBRK.ERDAT,df_VBRP.SMENG,df_VBRP.FKLMG,df_VBRP.LMENG,df_VBRP.NTGEW,df_VBRP.BRGEW)

df_VBRK_VBRP_MARA = df_VBRK_VBRP.join(df_MARA, ["MATNR"], "leftouter").select(df_VBRK_VBRP.MANDT,df_VBRK_VBRP.VBELN,df_VBRK_VBRP.BUKRS,df_VBRK_VBRP.POSNR,df_VBRK_VBRP.FKIMG,df_VBRK_VBRP.VRKME,df_VBRK_VBRP.MATNR,df_VBRK_VBRP.ERDAT,df_VBRK_VBRP.SMENG,df_VBRK_VBRP.FKLMG,df_VBRK_VBRP.LMENG,df_VBRK_VBRP.NTGEW,df_VBRK_VBRP.BRGEW,df_MARA.MEINS)
      
df_merge = df_VBRK_VBRP_MARA.withColumnRenamed('VBELN', 'BILLING_DOCUMENT') \
                   .withColumnRenamed('BUKRS', 'COMPANY_CODE') \
                   .withColumnRenamed('POSNR', 'BILLING_ITEM') \
                   .withColumn('ACT_BL_QTY', df_VBRK_VBRP.FKIMG.cast(DecimalType(30,6)))\
                   .withColumnRenamed('VRKME', 'UOM_UNIT')\
                   .withColumnRenamed('MATNR', 'MATERIAL_NO') \
                   .withColumnRenamed('ERDAT', 'CREATED_DATE') \
                   .withColumnRenamed('MANDT', 'CLIENT_NO') \
                   .withColumnRenamed('SMENG', 'SCALE_QTY') \
                   .withColumnRenamed('FKLMG', 'BILLING_QTY') \
                   .withColumnRenamed('LMENG', 'REQ_QTY_SKU') \
                   .withColumnRenamed('NTGEW', 'NETWT') \
                   .withColumnRenamed('BRGEW', 'GROSS_WT') \
                    .withColumnRenamed('MEINS', 'BASE_UOM')\
                    .withColumn('YEAR_FIELD', substring(col("CREATED_DATE"), 1,4))
                   
df_merge = df_merge.select('BILLING_DOCUMENT', 'COMPANY_CODE', 'BILLING_ITEM', 'ACT_BL_QTY', 'UOM_UNIT', 'MATERIAL_NO', 'CREATED_DATE', 'CLIENT_NO', 'SCALE_QTY', 'BILLING_QTY', 'REQ_QTY_SKU', 'NETWT', 'GROSS_WT','BASE_UOM','YEAR_FIELD')
    
df_merge.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{path}/")
