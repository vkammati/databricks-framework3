# Databricks notebook source
# MAGIC %run ./common_functions

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp,date_format
from pyspark.sql.functions import col, trim
spark = SparkSession.getActiveSession()
spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
from datetime import date, timedelta, datetime,time
from math import ceil,floor

table_name="M_TIME_DIMENSION"

path = eh_md_folder_path+"/"+table_name

# COMMAND ----------

#  date(year,month,day)

start_date=date(2000,1,1)
end_date=date(2050,12,31)
print(start_date,end_date)


# COMMAND ----------

def Crete_calendar(start_date,end_date):    
    dates=[]
    loop_date=start_date
    delta=timedelta(days=1)
    while( loop_date<end_date):
        timeStamp=loop_date.strftime("%Y%m%d%H%M%S")
        week=int(loop_date.strftime("%U"))+1
        week_str=f"{week:02d}"
        current_row=(
            loop_date.strftime("%b ")+str(loop_date.day)+loop_date.strftime(", %Y %I:%M:%S %p"),
            loop_date.strftime("%b ")+str(loop_date.day)+loop_date.strftime(", %Y"),
            loop_date.strftime("%Y%m%d%H%M%S"),
            loop_date.strftime("%Y%m%d"),
            str(loop_date.year),
            "0"+str(ceil(loop_date.month/3)),
            loop_date.strftime("%m"),
            week_str,
            str(loop_date.year),
            "0"+str(int(loop_date.weekday())),
            loop_date.strftime("%d"),
            
            
            loop_date.strftime("%H"),
            loop_date.strftime("%M"),
            loop_date.strftime("%S"),
            str(loop_date.year)+str(ceil(loop_date.month/3)),
            str(loop_date.year)+loop_date.strftime("%m"),
            str(loop_date.year)+week_str,
            int(loop_date.year),
            int(ceil(loop_date.month/3)),
            int(loop_date.strftime("%m")),
            int(loop_date.strftime("%U"))+1,
            int(loop_date.year),
            int("0"+str(int(loop_date.weekday()))),
            int(loop_date.strftime("%d")),
            int(loop_date.strftime("%H")),
            int(loop_date.strftime("%M")),
            int(loop_date.strftime("%S")),
            0,
            timeStamp[0:2]+","+timeStamp[2:5]+","+timeStamp[5:8]+","+timeStamp[8:11]+","+timeStamp[11:],
            timeStamp[0:2]+","+timeStamp[2:5]+","+timeStamp[5:8]+","+timeStamp[8:11]+","+timeStamp[11:],
            # loop_date.strftime("%A"),
            int(loop_date.strftime("%m")),
            loop_date.strftime("%B"),

        )
        dates.append(current_row)
        loop_date+=delta
    return dates

# COMMAND ----------

schema=["DATETIMESTAMP",
        "DATE_SQL",
        "DATETIME_SAP",
        "DATE_SAP" ,
        "YEAR",
        "QUARTER",
        "MONTH",
        "WEEK",
        "WEEK_YEAR",
        "DAY_OF_WEEK",
        "DAY",
        
        "HOUR",
        "MINUTE",
        "SECOND",
        "CALQUARTER",
        "CALMONTH",
        "CALWEEK",
        "YEAR_INT",
        "QUARTER_INT",
        "MONTH_INT",
        "WEEK_INT",
        "WEEK_YEAR_INT",
        "DAY_OF_WEEK_INT",
        "DAY_INT",
        "HOUR_INT",
        "MINUTE_INT",
        "SECOND_INT",
        "MONTH_LAST_DAY",
        "TZNTSTMPS",
        "TZNTSTMPL",
        "MONTH_NUMBER",
        "MONTH_NAME"
        ]
dates=Crete_calendar(start_date,end_date)
df_time_dimension= spark.createDataFrame(data=dates,schema=schema)

df_time_dimension.write.format("delta").mode("overwrite").option("mergeSchema", "true").save(f"{path}/")
print("m_time_dimension data written")
