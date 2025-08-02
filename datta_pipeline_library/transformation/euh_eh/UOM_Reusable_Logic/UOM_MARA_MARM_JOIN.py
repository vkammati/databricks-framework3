# Databricks notebook source
# %run ./common_functions

# COMMAND ----------

"""
UOM Conversion using MARA/MARM from EUH to Curated
"""
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

table_name="MD_MM_MATERIAL_UOM_JOIN"
path = eh_folder_path+"/"+table_name

""" Function to apply language filter to E(English)"""
def language_filter(df):
    filtered_df = df.filter(df.SPRAS=="E")
    return filtered_df

""" Function to apply client filter as 110 """
def client_filter(df):
    filtered_df = df.filter(df.MANDT==110)
    return filtered_df

"""
Asset MARA,MARM 
:param transform_dict: Transform dict of transformation function
:return: Dataframe
"""
df_MARA = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.mara")
df_MARM = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.marm")

""" Function call to apply filters on dataframe """

df_MARA = client_filter(df_MARA)
df_MARM = client_filter(df_MARM)

""" Renaming the Columns """ 
df_MARA=df_MARA.withColumnRenamed("MANDT","MARA_MANDT")\
                .withColumnRenamed("MATNR","MARA_MATNR")\
                .withColumnRenamed("MEINS","MARA_MEINS")


df_MARM = df_MARM.select(df_MARM.MANDT,df_MARM.MATNR,df_MARM.MEINH,df_MARM.UMREZ,df_MARM.UMREN)

"""Joining MARA and MARM dataframe to get Source UOM"""
df_mara_marm_src = df_MARA.join(df_MARM,((df_MARA.MARA_MANDT == df_MARM.MANDT) & (df_MARA.MARA_MATNR == df_MARM.MATNR)),"leftouter")\
                            .select(df_MARA.MARA_MANDT,df_MARA.MARA_MATNR,df_MARA.MARA_MEINS,df_MARM.MEINH,df_MARM.UMREZ,df_MARM.UMREN) 

""" Renaming the Columns """
df_mara_marm_src = df_mara_marm_src.withColumn("Z_SOURCE_UOM",when(col("MARA_MEINS") == 0,0).otherwise(df_mara_marm_src.UMREN/df_mara_marm_src.UMREZ))\
                                    .withColumnRenamed("MARA_MANDT","SRC_MARA_MANDT")\
                                    .withColumnRenamed("MARA_MATNR","SRC_MARA_MATNR")\
                                    .withColumnRenamed("MARA_MEINS","SRC_MARA_MEINS")\
                                    .withColumnRenamed("MEINH","SRC_MEINH")\
                                    .withColumnRenamed("UMREZ","SRC_UMREZ")\
                                    .withColumnRenamed("UMREN","SRC_UMREN")

"""Joining MARA and MARM dataframe to get Target UOM"""
df_mara_marm_tar = df_MARA.join(df_MARM,((df_MARA.MARA_MANDT == df_MARM.MANDT) & (df_MARA.MARA_MATNR == df_MARM.MATNR)),"leftouter")\
                            .select(df_MARA.MARA_MANDT, df_MARA.MARA_MATNR, df_MARA.MARA_MEINS,df_MARM.MEINH,df_MARM.UMREZ, df_MARM.UMREN)
                        
df_mara_marm_tar = df_mara_marm_tar.withColumn("Z_TARGET_UOM",when(col("MARA_MEINS") == 0,0).otherwise(df_mara_marm_tar.UMREN/df_mara_marm_tar.UMREZ))


df_src_tar_UOM = df_mara_marm_src.join(df_mara_marm_tar,((df_mara_marm_src.SRC_MARA_MATNR == df_mara_marm_tar.MARA_MATNR)),"inner")\
                                    .withColumnRenamed("SRC_MEINH","SOURCE_UOM")\
                                    .withColumnRenamed("MEINH","TARGET_UOM")

df_src_tar_UOM = df_src_tar_UOM.withColumn("UOM_CONV_VAL_1",when(col("Z_SOURCE_UOM") == 0,0).otherwise(df_src_tar_UOM.Z_TARGET_UOM/df_src_tar_UOM.Z_SOURCE_UOM))
df_src_tar_UOM=df_src_tar_UOM.withColumn("UOM_CONV_VAL",col("UOM_CONV_VAL_1").cast(DecimalType(30,4)))

df_MARM_Asset=df_src_tar_UOM.select(df_src_tar_UOM.SRC_MARA_MANDT,df_src_tar_UOM.SRC_MARA_MATNR,df_src_tar_UOM.SOURCE_UOM,df_src_tar_UOM.Z_SOURCE_UOM,df_src_tar_UOM.TARGET_UOM,df_src_tar_UOM.Z_TARGET_UOM,df_src_tar_UOM.UOM_CONV_VAL)

df_MARM_Asset = df_MARM_Asset.withColumnRenamed('SRC_MARA_MANDT','Client_NO')\
                                .withColumnRenamed('SRC_MARA_MATNR','Material_NO')\
                                .withColumnRenamed('UOM_CONV_VAL','Conversion_Factor')\
                                .withColumnRenamed('SOURCE_UOM','Source_UOM')\
                                .withColumnRenamed('TARGET_UOM','Target_UOM')



# df_MARM_Asset = df_MARM_Asset.select(F.col('CLIENT_NO').alias('Client_Number'),F.col('MATERIAL_NO').alias('Material_Number'),F.col('SOURCE_UOM').alias('CL_Source_UOM'),F.col('TARGET_UOM').alias('CL_Target_UOM'),F.col('CONVERSION_FACTOR').alias('CL_Conversion_Factor'))

df_MARM_Asset = df_MARM_Asset.select(F.col('Client_NO'),F.col('Material_NO'),F.col('Source_UOM'),F.col('Target_UOM'),F.col('Conversion_Factor'))


df_MARM_Asset.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{path}/")
