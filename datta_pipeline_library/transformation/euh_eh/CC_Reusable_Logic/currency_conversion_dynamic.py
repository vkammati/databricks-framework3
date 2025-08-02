# Databricks notebook source
# MAGIC %run ./common_functions

# COMMAND ----------

"""
Currency Conversion from EUH to Curated
"""

from pyspark.sql import *;
from pyspark.sql.functions import *;
from pyspark.sql import SparkSession;
from pyspark.sql.functions import col, trim
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()

table_name=""
path = eh_folder_path+"/"+table_name

# COMMAND ----------


def pre_calculated_currency(Transactional_Table):

    ExchangeRateType = spark.conf.get("pipeline.exchange_rate_type")
    Currency = spark.conf.get("pipeline.currency")
    TransactionalTable = spark.conf.get("pipeline.transactional_table")
    DateField = spark.conf.get("pipeline.date_field")
    AmountField = spark.conf.get("pipeline.amount_field")
    CurrencyField = spark.conf.get("pipeline.currency_field")
    Mode = spark.conf.get("pipeline.mode")

    print("currency_conversion_dynamic.py file ExchangeRateType:",ExchangeRateType)
    print("currency_conversion_dynamic.py file TargetCurrency:",Currency)
    print("currency_conversion_dynamic.py file TransactionalTable:",TransactionalTable)
    print("currency_conversion_dynamic.py file DateField:",DateField)
    print("currency_conversion_dynamic.py file AmountField:",AmountField)
    print("currency_conversion_dynamic.py file CurrencyField:",CurrencyField)
    print("currency_conversion_dynamic.py file CurrencyField:",Mode)

    df_ExchangeRate = spark.read.table('`cross_ds-unitycatalog-dev`.`curated-fcb_a_dev`.CurrencyConversion')
    df_Transactional = spark.read.table(TransactionalTable)

    df_ExchangeRate = df_ExchangeRate.filter((col("ExchangeRate_Type") == ExchangeRateType) & (df_ExchangeRate.ToCurrency == Currency))
    df_Transactional = df_Transactional.withColumn("GLOBAL_CURR",lit(Currency))
    
    if 'LTOG' in Mode:

        df_join = df_Transactional.join(df_ExchangeRate, (df_Transactional[CurrencyField] == df_ExchangeRate.FromCurrency) & 
        (df_Transactional.GLOBAL_CURR == df_ExchangeRate.ToCurrency)&(df_Transactional[DateField]== df_ExchangeRate.DateId),how="left_outer").withColumn(AmountField+"_"+Currency,(df_Transactional[AmountField])*(df_ExchangeRate.ExchangeRate))

        print("df_join inside pre_calculated_currency function under dynamic folder !!!")

        df_join=df_join.select(*[col(column_name) for column_name in df_join.columns if column_name != 'ingested_at']) 

    elif 'GTOL' in Mode:
        
        df_join = df_Transactional.join(df_ExchangeRate, (df_Transactional.GLOBAL_CURR== df_ExchangeRate.FromCurrency) &
        (df_Transactional[CurrencyField] == df_ExchangeRate.ToCurrency)& (df_Transactional[DateField]== df_ExchangeRate.DateId),how="left_outer").withColumn(AmountField+"_"+Currency,(df_Transactional[AmountField])*(df_ExchangeRate.ExchangeRate))   

        print("df_join inside pre_calculated_currency function under dynamic folder !!!")

        df_join=df_join.select(*[col(column_name) for column_name in df_join.columns if column_name != 'ingested_at'])      

    else:

        print("Please Enter Valid Mode !!!") 

# COMMAND ----------


df = pre_calculated_currency('`cross_ds-unitycatalog-dev`.`curated-fcb_a_dev`.fcb_currency_conversion_dynamic')
df.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{path}/")
