# Databricks notebook source
# %run ./common_functions

# COMMAND ----------

"""
UOM Conversion from EUH to Curated
"""
from pyspark.sql import *;
from pyspark.sql.functions import *;
from pyspark.sql import SparkSession;
from pyspark.sql.functions import col, trim
from pyspark.sql import functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import *
from functools import reduce
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

table_name="UOM_T006_DYNAMIC"
path = eh_folder_path+"/"+table_name

# COMMAND ----------

ConversionMode = dbutils.widgets.get('ConversionMode')
MaterialNoField = dbutils.widgets.get('MaterialNoField')
AltUOM = dbutils.widgets.get('AltUOM')
QuantityField = dbutils.widgets.get('QuantityField')
UOMField = dbutils.widgets.get('UOMField')
ClientNo = dbutils.widgets.get('ClientNo')

# COMMAND ----------

# TransactionalTable = dbutils.widgets.get('TransactionalTable')
TransactionalTable = f"`{uc_catalog_name}`.`{uc_eh_schema}`.fact_sd_sales_header_line_join"
MasterTable = f"`{uc_catalog_name}`.`{uc_eh_schema}`.md_mm_material_uom_join"
MasterTxtTable = f"`{uc_catalog_name}`.`{uc_eh_schema}`.md_mm_uom_text_join"
print(TransactionalTable) 
print(MasterTable)
print(MasterTxtTable)

# COMMAND ----------

def checkMARM(TransactionalTable,AltUOM,MaterialNoField,QuantityField,ConversionMode,UOMField,MasterTable):
    print("Displaying MARM Table")
    
    if 'MARM' in ConversionMode:

        AltUOM_List = AltUOM.split(',')

        df_MARM = spark.read.table(MasterTable)
        df_MARM = df_MARM.filter(df_MARM.TARGET_UOM.isin(AltUOM_List))
  
        df_uom_table = spark.read.table(TransactionalTable)

        for AltUOM in AltUOM_List:

            columName = UOMField + '_' + AltUOM
            df_MARM1 = df_MARM.filter(col("Target_UOM") == AltUOM)
            df_MARM1 = df_MARM1.withColumnRenamed("Client_NO", "ClientNo")
            df_MARM1 = df_MARM1.withColumnRenamed("Material_NO", "MaterialNo")
            df_uom_table = df_uom_table.join(df_MARM1,(df_uom_table[UOMField] == df_MARM1.Source_UOM) & 
                                                                (df_uom_table[ClientNo] == df_MARM1.CLIENT_NO) & 
                                                                (df_uom_table[MaterialNoField] == df_MARM1.MaterialNo),"left")
            df_uom_table = df_uom_table.withColumn(columName, when(df_uom_table[UOMField] == df_uom_table.Source_UOM, df_uom_table[QuantityField] * df_uom_table.Conversion_Factor).otherwise(df_uom_table[QuantityField]))
            df_uom_table = df_uom_table.select(*[col(column_name) for column_name in df_uom_table.columns if column_name not in {'ClientNo','Target_UOM','MaterialNo','Source_UOM','ingested_at','Conversion_Factor'}])

        #df_uom_table.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable("`cross_ds-unitycatalog-dev`.`curated-fcb_a_dev`.fcb_cc_uom_asset_v3")

        return df_uom_table
    
def checkT006(TransactionalTable,AltUOM,MaterialNoField,QuantityField,ConversionMode,UOMField,MasterTxtTable):
    print("Displaying T006 Table")    
    # from functools import reduce
    # from pyspark.sql import DataFrame

    if 'T006' in ConversionMode:

        AltUOM_List = AltUOM.split(',')

        df_t006 = spark.read.table(MasterTxtTable)
        df_t006 = df_t006.filter(df_t006.Target_UOM.isin(AltUOM_List))

        df_uom_table = spark.read.table(TransactionalTable)

        for AltUOM in AltUOM_List:

            columName = UOMField + '_' + AltUOM
            df_t0061 = df_t006.filter(col("Target_UOM") == AltUOM)
            df_t0061 = df_t0061.withColumnRenamed("Client_NO", "ClientNo")
            df_uom_table = df_uom_table.join(df_t0061,(df_uom_table[UOMField] == df_t0061.Source_UOM),"left")
            df_uom_table = df_uom_table.withColumn(columName, when(df_uom_table[UOMField] == df_uom_table.Source_UOM, df_uom_table[QuantityField] * df_uom_table.Conversion_Factor).otherwise(df_uom_table[QuantityField]))
            df_uom_table = df_uom_table.select(*[col(column_name) for column_name in df_uom_table.columns if column_name not in {'ClientNo','Target_UOM','MaterialNo','Unit_Of_Measurement_Text','Source_UOM','ingested_at','Conversion_Factor'}])

        # df_uom_table.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{path}/")

        return df_uom_table
                 

def pre_reusable_UOM(TransactionalTable,ConversionMode,MaterialNoField,AltUOM,QuantityField,UOMField,ClientNo,MasterTable,MasterTxtTable):

    # df_t006 = spark.read.table(f"`{uc_catalog_name}`.`{uc_eh_schema}`.md_mm_uom_text_join")
    # TransactionalTable = spark.read.table(TransactionalTable)

    if ConversionMode == 'T006':
        data = checkT006(TransactionalTable=TransactionalTable,AltUOM=AltUOM,MaterialNoField=MaterialNoField,QuantityField=QuantityField,ConversionMode=ConversionMode,UOMField=UOMField,MasterTxtTable=MasterTxtTable)
        return data
    if ConversionMode == 'MARM':
        data = checkMARM(TransactionalTable=TransactionalTable,AltUOM=AltUOM,MaterialNoField=MaterialNoField,QuantityField=QuantityField,ConversionMode=ConversionMode,UOMField=UOMField,MasterTable=MasterTable)
        return data
    if  ConversionMode == 'T006MARM':
        data = checkT006(TransactionalTable=TransactionalTable,AltUOM=AltUOM,MaterialNoField=MaterialNoField,QuantityField=QuantityField,ConversionMode=ConversionMode,UOMField=UOMField,MasterTxtTable=MasterTxtTable)
        if data == None: # optimization step limit(1) # Checking if T006 data is null
            data = checkMARM(TransactionalTable=TransactionalTable,AltUOM=AltUOM,MaterialNoField=MaterialNoField,QuantityField=QuantityField,ConversionMode=ConversionMode,UOMField=UOMField,MasterTable=MasterTable)
            return data
        else:
            return data
    if  ConversionMode == 'MARMT006':
        data = checkMARM(TransactionalTable=TransactionalTable,AltUOM=AltUOM,MaterialNoField=MaterialNoField,QuantityField=QuantityField,ConversionMode=ConversionMode,UOMField=UOMField,MasterTable=MasterTable)
        if data == None: # optimization step limit(1) # Checking if MARM data is null
            data = checkT006(TransactionalTable=TransactionalTable,AltUOM=AltUOM,MaterialNoField=MaterialNoField,QuantityField=QuantityField,ConversionMode=ConversionMode,UOMField=UOMField,MasterTxtTable=MasterTxtTable)
            return data
        else:
            return data
    # print('Count of records returned after fileter in T006 is {}'.format(data.count()))
   

df = pre_reusable_UOM(TransactionalTable,ConversionMode,MaterialNoField,AltUOM,QuantityField,UOMField,ClientNo,MasterTable,MasterTxtTable)

df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{path}/")
