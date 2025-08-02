# Databricks notebook source
# MAGIC %run ./common_functions

# COMMAND ----------

"""
Currency Conversion from EUH to EH
"""
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.functions import to_timestamp,date_format
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime
from pyspark.sql import SparkSession
spark = SparkSession.getActiveSession()
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")

table_name="DIM_CURRENCY_CONVERSION"

path = eh_folder_path+"/"+table_name

# COMMAND ----------

df_TCURR = df_TCURR.select("MANDT","KURST","FCURR","TCURR","GDATU","UKURS","FFACT","TFACT","ingested_at")
df_TCURR_BW = df_TCURR_BW.select("MANDT","KURST","FCURR","TCURR","GDATU","UKURS","FFACT","TFACT","ingested_at")
df_TCURV = df_TCURV.select("MANDT","KURST","XINVR","BWAER","XBWRL","GKUZU","BKUZU","XFIXD","XEURO","ingested_at")
df_TCURV_BW = df_TCURV_BW.select("MANDT","KURST","XINVR","BWAER","XBWRL","GKUZU","BKUZU","XFIXD","XEURO","ingested_at")
df_TCURW = df_TCURW.select("MANDT","SPRAS","KURST","CURVW","ingested_at")
df_TCURW_BW = df_TCURW_BW.select("MANDT","SPRAS","KURST","CURVW","ingested_at")
df_TCURF = df_TCURF.select("MANDT","KURST","FCURR","TCURR","GDATU","FFACT","TFACT","ABWCT","ABWGA","ingested_at")
df_TCURF_BW = df_TCURF_BW.select("MANDT","KURST","FCURR","TCURR","GDATU","FFACT","TFACT","ABWCT","ABWGA","ingested_at")

# COMMAND ----------

df_TCURR = df_TCURR.unionByName(df_TCURR_BW)
df_TCURV = df_TCURV.unionByName(df_TCURV_BW)
df_TCURW = df_TCURW.unionByName(df_TCURW_BW)
df_TCURF = df_TCURF.unionByName(df_TCURF_BW)

# COMMAND ----------

""" Function call to apply filters on dataframe """
df_TCURR = date_conversion(df_TCURR)
df_TCURR = client_filter(df_TCURR)
df_TCURW = language_filter(df_TCURW)
df_TCURW = client_filter(df_TCURW)
df_TCURF = client_filter(df_TCURF)
df_TCURF = date_conversion(df_TCURF)
df_TCURV = client_filter(df_TCURV)
df_TCURT = client_filter(df_TCURT)
df_TCURT = language_filter(df_TCURT)
df_TCURC = client_filter(df_TCURC)

# COMMAND ----------

""" Code to fetch like to like relationship between FCURR(From Currency) and TCURR(To Currency) """
df_TCURR_union = df_TCURR.select("MANDT","KURST","TCURR",col("TCURR").alias("FCURR"),"GDATU","GDATU_DATS").distinct()
df_FCURR_union = df_TCURR.select("MANDT","KURST","FCURR",col("FCURR").alias("TCURR"),"GDATU","GDATU_DATS").distinct()
df_union = df_TCURR.unionByName(df_TCURR_union, allowMissingColumns = True)
df_TCURR =df_union.unionByName(df_FCURR_union, allowMissingColumns = True)
df_TCURR = df_TCURR.withColumn("UKURS", when(col("TCURR") == col("FCURR"), lit("1")).otherwise(coalesce(col("UKURS"))))

# COMMAND ----------

""" Adding literal value to establish like to like relationship """
df_TCURR = df_TCURR.withColumn("FFACT" , when(col("FCURR") == col("TCURR"), lit("1")).otherwise(coalesce(col("FFACT"))))\
                .withColumn("TFACT" , when(col("FCURR") == col("TCURR"), lit("1")).otherwise(coalesce(col("TFACT"))))

df_TCURR = df_TCURR.distinct()

# COMMAND ----------

from datetime import datetime

current_year = datetime.now().year
start_year = current_year - 5
end_year = current_year + 5

df_DATE = df_DATE.filter(df_DATE.Year.between(start_year, end_year))

# COMMAND ----------

""" Code to restrict data only for last three years """
# df_DATE = df_DATE.filter(df_DATE.Year.between("2020", "2030") )
df_DATE  = df_DATE.withColumn("CYear", date_format(current_date(), "Y"))
df_DATE  = df_DATE.withColumn("PYear", df_DATE['CYear']-3)
df_DATE  = df_DATE.filter(df_DATE.Year >= df_DATE.PYear)

# COMMAND ----------

""" Created Current and Previous year column and implemented code logic to look into previous year if current year data is not existing""" 
df_TCURR = df_TCURR.withColumn('TCURR_Year', F.year('GDATU_DATS').cast('string'))
df_DATE = df_DATE.withColumn('Previous_Year', df_DATE['Year'] - 1)
df_DATE.createOrReplaceTempView("df_DATE")

df_year = df_DATE.select(df_DATE.Year, df_DATE.Previous_Year).distinct().orderBy(desc(df_DATE.Year))
df_year.createOrReplaceTempView("df_year")

df_TCURR_filtered = df_TCURR.join(df_year, df_TCURR.TCURR_Year == df_year.Year, "inner")
# df_DATE_filtered = df_DATE.filter((df_DATE.Year >= df_year.Previous_Year) & (df_DATE.Year <= df_year.Year))

df_DATE_filtered = spark.sql("""
    SELECT df_DATE.*
    FROM df_DATE
    JOIN df_year ON df_DATE.Year >= df_year.Previous_Year AND df_DATE.Year <= df_year.Year
""")

# COMMAND ----------

""" Renaming Columns of TCURR_PATTERN Bridge dataframes """
df_TCURR_PATTERN = df_TCURR_PATTERN.withColumnRenamed("MANDT","TCURR_MANDT")\
                                    .withColumnRenamed("KURST","TCURR_KURST")\
                                    .withColumnRenamed("PATTERN","TCURR_PATTERN")

# COMMAND ----------

df_TCURR_PATTERN =df_TCURR_PATTERN.filter(df_TCURR_PATTERN.TCURR_PATTERN != 'null')

# COMMAND ----------

""" Joining TCURR and Bridge dataframes """
df_TCURR_filtered = df_TCURR_filtered.join( df_TCURR_PATTERN ,(df_TCURR_filtered.KURST == df_TCURR_PATTERN.TCURR_KURST) , how = "inner")

# COMMAND ----------

df_DATE_filtered = df_DATE_filtered.withColumnRenamed("Year","date_year")

# COMMAND ----------

df_tcurr_date = df_TCURR_filtered.join(df_DATE_filtered,df_TCURR_filtered.GDATU_DATS == df_DATE_filtered.Date,how = "leftouter")

# COMMAND ----------

df_tcurr_date = df_tcurr_date.withColumnRenamed("date_year","tcurr_date_year") \
    .withColumnRenamed("Quarter","TCURR_Quarter") \
    .withColumnRenamed("MonthYear","TCURR_MonthYear") \
        .withColumnRenamed("Date","TCURR_Date") \
            .withColumnRenamed("DateId","TCURR_DateId") \
            .withColumnRenamed("DayName","TCURR_DayName")

# COMMAND ----------

df_tcurr_year = df_tcurr_date.filter("TCURR_PATTERN == 'Y'")
df_tcurr_quarter = df_tcurr_date.filter("TCURR_PATTERN == 'Q'")
df_tcurr_month = df_tcurr_date.filter("TCURR_PATTERN == 'M'")
df_tcurr_day = df_tcurr_date.filter("TCURR_PATTERN == 'D'")

# COMMAND ----------

df_tcurr_year1 = df_DATE_filtered.join(df_tcurr_year,df_DATE_filtered.date_year == df_tcurr_year.TCURR_Year,"inner").select(df_tcurr_year.MANDT, df_tcurr_year.KURST,df_tcurr_year.FCURR,df_tcurr_year.TCURR ,df_DATE_filtered.DateId,df_tcurr_year.GDATU,df_tcurr_year.GDATU_DATS, df_tcurr_year.UKURS,df_tcurr_year.TFACT,df_tcurr_year.FFACT, df_tcurr_year.TCURR_Year).distinct()

# COMMAND ----------

df_tcurr_quarter1 = df_DATE_filtered.join(df_tcurr_quarter,df_DATE_filtered.Quarter == df_tcurr_quarter.TCURR_Quarter,"inner").select(df_tcurr_quarter.MANDT, df_tcurr_quarter.KURST,df_tcurr_quarter.FCURR,df_tcurr_quarter.TCURR ,df_DATE_filtered.DateId,df_tcurr_quarter.GDATU,df_tcurr_quarter.GDATU_DATS, df_tcurr_quarter.UKURS,df_tcurr_quarter.TFACT,df_tcurr_quarter.FFACT, df_tcurr_quarter.TCURR_Year).distinct()

# COMMAND ----------

df_tcurr_month1 = df_DATE_filtered.join(df_tcurr_month,df_DATE_filtered.MonthYear == df_tcurr_month.TCURR_MonthYear,"inner").select(df_tcurr_month.MANDT, df_tcurr_month.KURST,df_tcurr_month.FCURR,df_tcurr_month.TCURR ,df_DATE_filtered.DateId,df_tcurr_month.GDATU,df_tcurr_month.GDATU_DATS, df_tcurr_month.UKURS,df_tcurr_month.TFACT,df_tcurr_month.FFACT, df_tcurr_month.TCURR_Year).distinct()

# COMMAND ----------

df_tcurr_day = df_tcurr_day.filter(~df_tcurr_day.TCURR_DayName.isin("Saturday","Sunday"))

# COMMAND ----------

df_tcurr_day1 = df_DATE_filtered.join(df_tcurr_day,df_DATE_filtered.Date == df_tcurr_day.GDATU_DATS,"leftouter").select(df_tcurr_day.MANDT, df_tcurr_day.KURST,df_tcurr_day.FCURR,df_tcurr_day.TCURR ,df_DATE_filtered.DateId,df_tcurr_day.GDATU,df_tcurr_day.GDATU_DATS, df_tcurr_day.UKURS,df_tcurr_day.TFACT,df_tcurr_day.FFACT, df_tcurr_day.TCURR_Year,df_DATE_filtered.DayName).distinct()

# COMMAND ----------

df_tcurr_day2 = df_tcurr_day1.withColumn("DateId_DATS", to_date("DateId" , "yyyyMMdd"))

# COMMAND ----------

#weekday records
df_tcurr_days = df_tcurr_day2.filter("GDATU is not null").select ("MANDT","KURST","FCURR","TCURR","DateId","GDATU","GDATU_DATS","UKURS","TFACT","FFACT","TCURR_Year")

# COMMAND ----------

#weekend records along with additional records which are not present in TCURR
df_tcurr_weekend = df_tcurr_day2.filter("GDATU is null").withColumnRenamed("DateId","DateId_w").withColumnRenamed("DateId_DATS","DateId_DATS_w").withColumnRenamed("TCURR","TCURR_w").withColumnRenamed("MANDT","MANDT_w").withColumnRenamed("FCURR","FCURR_w").withColumnRenamed("GDATU","GDATU_w").withColumnRenamed("KURST","KURST_w").withColumnRenamed("TFACT","TFACT_w").withColumnRenamed("FFACT","FFACT_w").select("DateId_w","DateId_DATS_w","DayName")

# COMMAND ----------

df_tcurr_weekend_filtered=df_tcurr_weekend.withColumn("Id", lit(1)) \
    .withColumn("daytype",lit("weekend"))
df_tcurr_days=df_tcurr_days.withColumn("JoinId", lit(1)) 

# COMMAND ----------

df_final_week_end = df_tcurr_weekend_filtered.join(df_tcurr_days,df_tcurr_weekend_filtered["Id"]==df_tcurr_days["JoinId"],"inner")

# COMMAND ----------

df_final_week_end1 = df_final_week_end.select("MANDT","KURST","FCURR","TCURR","DateId_w","DateId_DATS_w","UKURS","TFACT","FFACT","TCURR_Year")

# COMMAND ----------

df_final_week_end = df_final_week_end1.withColumnRenamed("DateId_w","DateId"). \
    withColumnRenamed("DateId_DATS_w","GDATU_DATS")

# COMMAND ----------

df_final_week = df_final_week_end.withColumn("GDATU",99999999-df_final_week_end["DateId"].cast("int")).withColumn("TCURR_Year",year(col("GDATU_DATS")))

# COMMAND ----------

df_final_week = df_final_week.select("MANDT","KURST","FCURR","TCURR","DateId","GDATU","GDATU_DATS","UKURS","TFACT","FFACT","TCURR_Year")

# COMMAND ----------

df_final_week = df_final_week.withColumn("GDATU",col("GDATU").cast("string")).withColumn("Tcurr_Year",col("Tcurr_Year").cast("string"))

# COMMAND ----------

df_final_week = df_final_week.withColumn("UKURS",lit("null"))

# COMMAND ----------

df_final_week = df_final_week.distinct()

# COMMAND ----------

#weekday records
df_tcurr_days = df_tcurr_days.select ("MANDT","KURST","FCURR","TCURR","DateId","GDATU","GDATU_DATS","UKURS","TFACT","FFACT","TCURR_Year")

# COMMAND ----------

df_total_days = df_tcurr_days.union(df_final_week)

# COMMAND ----------

df_total_days = df_total_days.withColumn("UKURS1",when(col("UKURS") == 'null',None).otherwise(col("UKURS")))

# COMMAND ----------

#modified code from above cell
df_total_days = df_total_days.withColumn("key",concat(col("MANDT"),col("KURST"),col("FCURR"),col("TCURR")))

# COMMAND ----------

import sys
import pyspark.sql.functions as func
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window

# COMMAND ----------

df_total_days = df_total_days.withColumn("UKURS_NEW", func.last('UKURS1',True)
                  .over(Window.partitionBy("key")
                  .orderBy("GDATU_DATS")
                  ))

# COMMAND ----------

df_total_days = df_total_days.filter("UKURS_NEW is not null")

# COMMAND ----------

df_total_days = df_total_days.select("MANDT","KURST","FCURR","TCURR","DateId","GDATU","GDATU_DATS","UKURS_NEW","TFACT","FFACT","TCURR_Year")

# COMMAND ----------

df_total_days = df_total_days.withColumnRenamed("UKURS_NEW","UKURS")

# COMMAND ----------

result_df = df_tcurr_year1.union(df_tcurr_quarter1).union(df_tcurr_month1).union(df_total_days)

# COMMAND ----------

""" Renaming the Columns """ 
result_df=result_df.withColumnRenamed('GDATU', 'TCURR_GDATU').withColumnRenamed('GDATU_DATS', 'TCURR_GDATU_DATS').withColumn('A_UKURS', result_df.UKURS.cast(DecimalType(30, 6)))
df_TCURF=df_TCURF.withColumnRenamed('GDATU', 'TCURF_GDATU').withColumnRenamed('GDATU_DATS', 'TCURF_GDATU_DATS')

# COMMAND ----------

"""Joining TCURR and TCURF dataframe to get currency conversion factor"""
df_TCURR_TCURF = result_df.join(df_TCURF , ((result_df.MANDT == df_TCURF.MANDT) & (result_df.KURST == df_TCURF.KURST) & (result_df.FCURR == df_TCURF.FCURR) & (result_df.TCURR == df_TCURF.TCURR)),"leftouter")\
                .select(result_df.MANDT,result_df.KURST,result_df.FCURR,result_df.TCURR,result_df.DateId,result_df.TCURR_GDATU,df_TCURF.TCURF_GDATU,"A_UKURS",coalesce(df_TCURF.FFACT,result_df.FFACT).alias('FFACT'),coalesce(df_TCURF.TFACT,result_df.TFACT).alias('TFACT'),result_df.TCURR_GDATU_DATS,df_TCURF.TCURF_GDATU_DATS)

# COMMAND ----------

""" Joining TCURR_TCURF  with TCURW dataframe to get Exchange Rate Description"""
df_join = df_TCURR_TCURF.join(df_TCURW , df_TCURR_TCURF.KURST == df_TCURW.KURST,"leftouter")\
            .select(df_TCURR_TCURF.MANDT,df_TCURR_TCURF.KURST,df_TCURR_TCURF.FCURR,df_TCURR_TCURF.TCURR,df_TCURR_TCURF.DateId,df_TCURR_TCURF.TCURR_GDATU,"A_UKURS",df_TCURR_TCURF.FFACT,df_TCURR_TCURF.TFACT,df_TCURR_TCURF.TCURR_GDATU_DATS,"CURVW").distinct()

# COMMAND ----------

df_join=df_join.withColumnRenamed('TCURR_GDATU', 'GDATU').withColumnRenamed('TCURR_GDATU_DATS', 'GDATU_DATS').withColumnRenamed('A_UKURS', 'UKURS')

# COMMAND ----------

""" Join with TCURV dataframe """
df_join1 = df_join.join(df_TCURV , df_join.KURST == df_TCURV.KURST,"leftouter").select(df_join.MANDT,df_join.KURST,df_join.FCURR,df_join.TCURR,df_join.DateId,df_join.GDATU,df_join.UKURS,df_join.FFACT,df_join.TFACT,df_join.GDATU_DATS,df_join.CURVW,df_TCURV.XINVR,df_TCURV.BWAER,df_TCURV.XBWRL,df_TCURV.GKUZU,df_TCURV.BKUZU,df_TCURV.XFIXD,df_TCURV.XEURO)

# COMMAND ----------

""" Renaming of TCURC dataframe for FCURR column """
df_TCURC_FCURR=df_TCURC.withColumnRenamed('GDATU', 'TCURC_GDATU_FCURR')\
                    .withColumnRenamed('ISOCD', 'TCURC_ISOCD_FCURR')\
                    .withColumnRenamed('ALTWR', 'TCURC_ALTWR_FCURR')\
                    .withColumnRenamed('WAERS', 'TCURC_WAERS_FCURR')\
                    .withColumnRenamed('XPRIMARY', 'TCURC_XPRIMARY_FCURR')

# COMMAND ----------

""" Join with TCURC dataframe for FCURR column """
df_join2 = df_join1.join(df_TCURC_FCURR , df_join1.FCURR == df_TCURC_FCURR.TCURC_WAERS_FCURR,"leftouter").select(df_join1.MANDT,df_join1.KURST,df_join1.FCURR,df_join1.TCURR,df_join1.DateId,df_join1.GDATU,df_join1.UKURS,df_join1.FFACT,df_join1.TFACT,df_join1.GDATU_DATS,df_join1.CURVW,df_join1.XINVR,df_join1.BWAER,df_join1.XBWRL,df_join1.GKUZU,df_join1.BKUZU,df_join1.XFIXD,df_join1.XEURO,df_TCURC_FCURR.TCURC_ISOCD_FCURR,df_TCURC_FCURR.TCURC_ALTWR_FCURR,df_TCURC_FCURR.TCURC_GDATU_FCURR,df_TCURC_FCURR.TCURC_XPRIMARY_FCURR)

# COMMAND ----------

""" Renaming of TCURC dataframe for TCURR column """
df_TCURC_TCURR=df_TCURC.withColumnRenamed('GDATU', 'TCURC_GDATU_TCURR')\
                    .withColumnRenamed('ISOCD', 'TCURC_ISOCD_TCURR')\
                    .withColumnRenamed('ALTWR', 'TCURC_ALTWR_TCURR')\
                    .withColumnRenamed('WAERS', 'TCURC_WAERS_TCURR')\
                    .withColumnRenamed('XPRIMARY', 'TCURC_XPRIMARY_TCURR')

# COMMAND ----------

""" Join with TCURC dataframe for TCURR column """
df_join3 = df_join2.join(df_TCURC_TCURR , df_join2.TCURR == df_TCURC_TCURR.TCURC_WAERS_TCURR,"leftouter").select(df_join2.MANDT,df_join2.KURST,df_join2.FCURR,df_join2.TCURR,df_join2.DateId,df_join2.GDATU,df_join2.UKURS,df_join2.FFACT,df_join2.TFACT,df_join2.GDATU_DATS,df_join2.CURVW,df_join2.XINVR,df_join2.BWAER,df_join2.XBWRL,df_join2.GKUZU,df_join2.BKUZU,df_join2.XFIXD,df_join2.XEURO,df_join2.TCURC_ISOCD_FCURR,df_join2.TCURC_ALTWR_FCURR,df_join2.TCURC_GDATU_FCURR,df_join2.TCURC_XPRIMARY_FCURR,df_TCURC_TCURR.TCURC_ISOCD_TCURR,df_TCURC_TCURR.TCURC_ALTWR_TCURR,df_TCURC_TCURR.TCURC_GDATU_TCURR,df_TCURC_TCURR.TCURC_XPRIMARY_TCURR)

# COMMAND ----------

""" Join with TCURT dataframe for TCURR column """
df_join4 = df_join3.join(df_TCURT , df_join3.TCURR == df_TCURT.WAERS,"leftouter").select(df_join3.MANDT,df_join3.KURST,df_join3.FCURR,df_join3.TCURR,df_join3.DateId,df_join3.GDATU,df_join3.UKURS,df_join3.FFACT,df_join3.TFACT,df_join3.GDATU_DATS,df_join3.CURVW,df_join3.XINVR,df_join3.BWAER,df_join3.XBWRL,df_join3.GKUZU,df_join3.BKUZU,df_join3.XFIXD,df_join3.XEURO,df_join3.TCURC_ISOCD_FCURR,df_join3.TCURC_ALTWR_FCURR,df_join3.TCURC_GDATU_FCURR,df_join3.TCURC_XPRIMARY_FCURR,df_join3.TCURC_ISOCD_TCURR,df_join3.TCURC_ALTWR_TCURR,df_join3.TCURC_GDATU_TCURR,df_join3.TCURC_XPRIMARY_TCURR,df_TCURT.LTEXT,df_TCURT.KTEXT)

# COMMAND ----------

df_join4=df_join4.withColumnRenamed('KTEXT', 'TCURT_KTEXT_TCURR').withColumnRenamed('LTEXT', 'TCURT_LTEXT_TCURR')
df_TCURT_FCURR=df_TCURT.withColumnRenamed('LTEXT', 'TCURT_LTEXT_FCURR').withColumnRenamed('KTEXT', 'TCURT_KTEXT_FCURR')

# COMMAND ----------

""" Join with TCURT dataframe for FCURR column """
df_join5 = df_join4.join(df_TCURT_FCURR , df_join4.FCURR == df_TCURT_FCURR.WAERS,"leftouter") \
                .select(df_join4.MANDT, df_join4.KURST, df_join4.FCURR, df_join4.TCURR,df_join4.DateId, df_join4.GDATU, df_join4.UKURS, df_join4.FFACT, df_join4.TFACT, df_join4.GDATU_DATS, df_join4.CURVW, df_join4.XINVR, df_join4.BWAER, df_join4.XBWRL, df_join4.GKUZU, df_join4.BKUZU, df_join4.XFIXD, df_join4.XEURO, df_join4.TCURC_ISOCD_FCURR, df_join4.TCURC_ALTWR_FCURR, df_join4.TCURC_GDATU_FCURR, df_join4.TCURC_XPRIMARY_FCURR, df_join4.TCURC_ISOCD_TCURR, df_join4.TCURC_ALTWR_TCURR, df_join4.TCURC_GDATU_TCURR, df_join4.TCURC_XPRIMARY_TCURR, df_join4.TCURT_LTEXT_TCURR, df_join4.TCURT_KTEXT_TCURR, df_TCURT_FCURR.TCURT_LTEXT_FCURR, df_TCURT_FCURR.TCURT_KTEXT_FCURR)

# COMMAND ----------

""" Adding literal value to establish like to like relationship """
df_join5 = df_join5.withColumn("FFACT" , when(col("FCURR") == col("TCURR"), lit("1")).otherwise(coalesce(col("FFACT"))))\
                .withColumn("TFACT" , when(col("FCURR") == col("TCURR"), lit("1")).otherwise(coalesce(col("TFACT"))))

# COMMAND ----------

"""Calculating the Exchange Rate when negative values are encountered"""
df_join5= df_join5.withColumn("ACTUAL_UKURS", when(df_join5.UKURS < 0,((1/abs(df_join5.UKURS)) * (df_join5.TFACT/df_join5.FFACT))).otherwise((df_join5.UKURS)*(df_join5.FFACT/df_join5.TFACT)))

# COMMAND ----------

df_exchange_rate = df_join5.withColumnRenamed('MANDT', 'TCURR_Client') \
                .withColumnRenamed('KURST', 'ExchangeRate_Type') \
                .withColumnRenamed('FCURR', 'FromCurrency') \
                .withColumnRenamed('TCURR', 'ToCurrency') \
                .withColumn('DateId', F.to_date(F.coalesce(F.date_format(F.to_date(F.col("DateId"), 'yyyyMMdd'), 'yyyy-MM-dd'), F.to_date(F.col("DateId"), "yyyy-MM-dd")))) \
                .withColumnRenamed('GDATU', 'ExchangeRate_EffectiveDate') \
                .withColumn('ExchangeRate', df_join5.ACTUAL_UKURS.cast(DecimalType(30,10))) \
                .withColumnRenamed('FFACT','FromCurrency_Ratio') \
                .withColumnRenamed('TFACT', 'ToCurrency_Ratio') \
                .withColumn("Currency_ValidDate", F.date_format(F.col("GDATU_DATS"),'yyyyMMdd')) \
                .withColumnRenamed('GDATU_DATS', 'Currency_Valid_DateFormat') \
                .withColumnRenamed('CURVW', 'ExchangeRate_Description') \
                .withColumnRenamed('XINVR', 'Inverted_ExchangeRate_Indicator') \
                .withColumnRenamed('BWAER', 'Reference_Currency_ForTranslation') \
                .withColumnRenamed('XBWRL', 'FromCurrency_ExchangeRate_Indicator') \
                .withColumnRenamed('GKUZU', 'ExchangeRate_Type_BuyingRate') \
                .withColumnRenamed('BKUZU', 'ExchangeRate_Type_SellingRate') \
                .withColumnRenamed('XFIXD', 'Indicator_FixedExchangeRate') \
                .withColumnRenamed('XEURO', 'Indicator_ExchangeRateType') \
                .withColumnRenamed('TCURC_ISOCD_FCURR', 'ISOCurrencyCode_FCURR') \
                .withColumnRenamed('TCURC_ALTWR_FCURR', 'AlternativeKey_FCURR') \
                .withColumnRenamed('TCURC_GDATU_FCURR', 'Currency_ValidDate_FCURR') \
                .withColumnRenamed('TCURC_XPRIMARY_FCURR', 'PrimarySAP_CurrencyCode_FCURR') \
                .withColumnRenamed('TCURC_ISOCD_TCURR', 'ISOCurrencyCode_TCURR') \
                .withColumnRenamed('TCURC_ALTWR_TCURR', 'AlternativeKey_TCURR') \
                .withColumnRenamed('TCURC_GDATU_TCURR', 'Currency_ValidDate_TCURR') \
                .withColumnRenamed('TCURC_XPRIMARY_TCURR', 'PrimarySAP_CurrencyCode_TCURR') \
                .withColumnRenamed('TCURT_LTEXT_TCURR', 'ToCurrency_Description') \
                .withColumnRenamed('TCURT_LTEXT_FCURR', 'FromCurrency_Description')

# COMMAND ----------

df_exchange_rate = df_exchange_rate.select('TCURR_Client','ExchangeRate_Type','ExchangeRate_Description','FromCurrency','FromCurrency_Description', 'ToCurrency','ToCurrency_Description','ExchangeRate_EffectiveDate','Currency_ValidDate','Currency_Valid_DateFormat','ExchangeRate','FromCurrency_Ratio','ToCurrency_Ratio','DateId')

# COMMAND ----------

df_exchange_rate.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{path}/")
print("currency conversion data written")
print(f"{path}/")
