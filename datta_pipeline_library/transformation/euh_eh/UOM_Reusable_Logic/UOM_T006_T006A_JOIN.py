# Databricks notebook source
# %run ./common_functions

# COMMAND ----------

"""
UOM Conversion using T006/T006A from EUH to Curated
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

table_name="MD_MM_UOM_TEXT_JOIN"
path = eh_folder_path+"/"+table_name

""" Function to filter language key """

def language_filter(df):
    filtered_df = df.filter(df.SPRAS=="E") 
    return filtered_df

""" Function to filter client value """

def client_filter(df):
    filtered_df = df.filter(df.MANDT==110)
    return filtered_df

"""
Asset T006,T006A 
:return: Dataframe
"""
df_T006 = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.t006")
df_T006A = spark.read.table(f"`{uc_catalog_name}`.`{uc_euh_schema}`.t006a")

""" Function call to apply filters on dataframe """

df_T006 = client_filter(df_T006)
df_T006A = client_filter(df_T006A)
df_T006A = language_filter(df_T006A)

""" T006 Asset Notebook : Implemented logic to get the SI unit conversion factors and the respective text for Units """

df = df_T006.select('MANDT','MSEHI','DIMID','ZAEHL','NENNR')
df_T006_rename = df.withColumnRenamed('MANDT','T006_CLIENT')\
                    .withColumnRenamed('MSEHI','T006_SRC_UOM')\
                    .withColumnRenamed('DIMID','T006_DIMID')\
                    .withColumnRenamed('ZAEHL','NUMERATOR')\
                    .withColumnRenamed('NENNR','DENOMINATOR')

df_t006_src = df_T006_rename.withColumn("T006_SRC_CONV_FACTOR",when(col("DENOMINATOR") == 0,0).otherwise(df_T006_rename.NUMERATOR/df_T006_rename.DENOMINATOR))
print("df_T006_src_data:::", df_t006_src)

df = df_T006.select('MANDT','MSEHI','DIMID','ZAEHL','NENNR')
df_T006_rename = df.withColumnRenamed('MANDT','T006_CLIENT')\
                    .withColumnRenamed('MSEHI','T006_TAR_UOM')\
                    .withColumnRenamed('DIMID','T006_DIMID')\
                    .withColumnRenamed('ZAEHL','NUMERATOR')\
                    .withColumnRenamed('NENNR','DENOMINATOR')

df_T006_tar = df_T006_rename.withColumn("T006_TAR_CONV_FACTOR",when(col("T006_TAR_UOM") == 0,0).otherwise(df_T006_rename.NUMERATOR/df_T006_rename.DENOMINATOR))
print(df_T006_tar)

df_t006_join = df_T006_tar.join(df_t006_src,((df_T006_tar.T006_CLIENT == df_t006_src.T006_CLIENT) & (df_T006_tar.T006_DIMID == df_t006_src.T006_DIMID)),"leftouter")\
            .select(df_T006_tar.T006_CLIENT,df_T006_tar.T006_TAR_UOM,df_T006_tar.T006_TAR_CONV_FACTOR,df_t006_src.T006_SRC_UOM,df_t006_src.T006_SRC_CONV_FACTOR)

df_t006_join = df_t006_join.select('T006_CLIENT','T006_SRC_UOM','T006_SRC_CONV_FACTOR','T006_TAR_UOM','T006_TAR_CONV_FACTOR')

df_common_UOM_T006 = df_t006_join.withColumn("Conversion_Factor",when(col("T006_SRC_CONV_FACTOR") == 0,0).otherwise(df_t006_join.T006_SRC_CONV_FACTOR/df_t006_join.T006_TAR_CONV_FACTOR))

df_T006_Asset = df_common_UOM_T006.join(df_T006A,((df_common_UOM_T006.T006_CLIENT == df_T006A.MANDT) & (df_common_UOM_T006.T006_TAR_UOM == df_T006A.MSEHI)),"inner")\
                        .select(df_common_UOM_T006.T006_CLIENT,df_common_UOM_T006.T006_SRC_UOM,df_common_UOM_T006.T006_TAR_UOM,df_T006A.MSEHL,df_common_UOM_T006.Conversion_Factor)

df_T006_Asset = df_T006_Asset.withColumnRenamed('T006_CLIENT','Client_NO')\
                                .withColumnRenamed('T006_TAR_UOM','Target_UOM')\
                                .withColumnRenamed('T006_SRC_UOM','Source_UOM')\
                                .withColumnRenamed('MSEHL','Unit_Of_Measurement_Text')\
                                .withColumnRenamed('Conversion_Factor','Conversion_Factor')

df_T006_Asset.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{path}/")
