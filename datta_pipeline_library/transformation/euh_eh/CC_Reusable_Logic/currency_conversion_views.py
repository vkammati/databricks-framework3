# Databricks notebook source
# DBTITLE 1,vw_tcurr_cc_global_curated
# MAGIC %sql
# MAGIC CREATE VIEW `cross_ds-unitycatalog-dev`.`eh-ds-capabilities`.vw_tcurr_cc_global_curated (
# MAGIC   TCURR_Client,
# MAGIC   ExchangeRate_Type,
# MAGIC   ExchangeRate_Description,
# MAGIC   FromCurrency,
# MAGIC   FromCurrency_Description,
# MAGIC   ToCurrency,
# MAGIC   ToCurrency_Description,
# MAGIC   ExchangeRate_EffectiveDate,
# MAGIC   Currency_ValidDate,
# MAGIC   Currency_Valid_DateFormat,
# MAGIC   ExchangeRate,
# MAGIC   FromCurrency_Ratio,
# MAGIC   ToCurrency_Ratio,
# MAGIC   DateId)
# MAGIC AS select * from `cross_ds-unitycatalog-dev`.`eh-ds-capabilities`.dim_currency_conversion

# COMMAND ----------

# DBTITLE 1,vw_vbrp_cc_cob_curated
# %sql
# CREATE OR REPLACE VIEW `cross_ds-unitycatalog-dev`.`curated-capabilities`.vw_vbrp_cc_cob_curated
# AS select 
# vbrp.*
# ,concat(vbrp.Document_Currency,vbrp.Billing_Document_Date ) as LC_DocumentDate
# from `cross_ds-unitycatalog-dev`.`eh-ds-capabilities`.fact_sd_sales_header_line_join_cc vbrp

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW `cross_ds-unitycatalog-dev`.`eh-ds-capabilities`.vw_cc_prompt_data
# MAGIC                 AS
# MAGIC                 SELECT * FROM(
# MAGIC                 SELECT DISTINCT CC.EXCHANGERATE_TYPE,CC.TOCURRENCY, 
# MAGIC                 CD.FULL_DATE,
# MAGIC                 CD.YEAR,CD.QUARTER,CD.MONTHYEAR,CD.DAYOFMONTH FROM `CROSS_DS-UNITYCATALOG-DEV`.`eh-ds-capabilities`.dim_currency_conversion CC
# MAGIC                 LEFT JOIN `CROSS_DS-UNITYCATALOG-DEV`.`euh-ds-gsap`.DATE CD
# MAGIC                 ON CC.DATEID = CD.FULL_DATE
# MAGIC                 WHERE CC.EXCHANGERATE_TYPE IN ("M","001M","002M","001Q","P","ZLE1","ZM1","ZM2","ZP1","ZQ1","ZY1"))
# MAGIC                 UNION
# MAGIC                 SELECT 
# MAGIC                 "P" AS EXCHANGERATE_TYPE
# MAGIC                 ,"USD" AS TOCURRENCY
# MAGIC                 ,"9999-01-01" AS FULL_DATE
# MAGIC                 ,"9999" AS YEAR
# MAGIC                 ,"9999-Q1" AS QUARTER
# MAGIC                 ,"9999-01" AS MONTHYEAR
# MAGIC                 ,01  AS DAYOFMONTH
# MAGIC                 FROM `CROSS_DS-UNITYCATALOG-DEV`.`euh-ds-gsap`.DATE
# MAGIC                 UNION
# MAGIC                     SELECT 
# MAGIC                     "M" AS EXCHANGERATE_TYPE
# MAGIC                     ,"USD" AS TOCURRENCY
# MAGIC                     ,"9999-01-01" AS FULL_DATE
# MAGIC                     ,"9999" AS YEAR
# MAGIC                     ,"9999-Q1" AS QUARTER
# MAGIC                     ,"9999-01" AS MONTHYEAR
# MAGIC                     ,01  AS DAYOFMONTH
# MAGIC                     FROM `CROSS_DS-UNITYCATALOG-DEV`.`euh-ds-gsap`.DATE
# MAGIC                 UNION
# MAGIC                     SELECT 
# MAGIC                     "001M" AS EXCHANGERATE_TYPE
# MAGIC                     ,"USD" AS TOCURRENCY
# MAGIC                     ,"9999-01-01" AS FULL_DATE
# MAGIC                     ,"9999" AS YEAR
# MAGIC                     ,"9999-Q1" AS QUARTER
# MAGIC                     ,"9999-01" AS MONTHYEAR
# MAGIC                     ,01  AS DAYOFMONTH
# MAGIC                     FROM `CROSS_DS-UNITYCATALOG-DEV`.`euh-ds-gsap`.DATE
# MAGIC                 UNION
# MAGIC                     SELECT 
# MAGIC                     "002M" AS EXCHANGERATE_TYPE
# MAGIC                     ,"USD" AS TOCURRENCY
# MAGIC                     ,"9999-01-01" AS FULL_DATE
# MAGIC                     ,"9999" AS YEAR
# MAGIC                     ,"9999-Q1" AS QUARTER
# MAGIC                     ,"9999-01" AS MONTHYEAR
# MAGIC                     ,01  AS DAYOFMONTH
# MAGIC                     FROM `CROSS_DS-UNITYCATALOG-DEV`.`euh-ds-gsap`.DATE
# MAGIC                 UNION
# MAGIC                     SELECT 
# MAGIC                     "001Q" AS EXCHANGERATE_TYPE
# MAGIC                     ,"USD" AS TOCURRENCY
# MAGIC                     ,"9999-01-01" AS FULL_DATE
# MAGIC                     ,"9999" AS YEAR
# MAGIC                     ,"9999-Q1" AS QUARTER
# MAGIC                     ,"9999-01" AS MONTHYEAR
# MAGIC                     ,01  AS DAYOFMONTH
# MAGIC                     FROM `CROSS_DS-UNITYCATALOG-DEV`.`euh-ds-gsap`.DATE
# MAGIC                 UNION
# MAGIC                     SELECT 
# MAGIC                     "ZLE1" AS EXCHANGERATE_TYPE
# MAGIC                     ,"USD" AS TOCURRENCY
# MAGIC                     ,"9999-01-01" AS FULL_DATE
# MAGIC                     ,"9999" AS YEAR
# MAGIC                     ,"9999-Q1" AS QUARTER
# MAGIC                     ,"9999-01" AS MONTHYEAR
# MAGIC                     ,01  AS DAYOFMONTH
# MAGIC                     FROM `CROSS_DS-UNITYCATALOG-DEV`.`euh-ds-gsap`.DATE
# MAGIC                 UNION
# MAGIC                     SELECT 
# MAGIC                     "ZM1" AS EXCHANGERATE_TYPE
# MAGIC                     ,"USD" AS TOCURRENCY
# MAGIC                     ,"9999-01-01" AS FULL_DATE
# MAGIC                     ,"9999" AS YEAR
# MAGIC                     ,"9999-Q1" AS QUARTER
# MAGIC                     ,"9999-01" AS MONTHYEAR
# MAGIC                     ,01  AS DAYOFMONTH
# MAGIC                     FROM `CROSS_DS-UNITYCATALOG-DEV`.`euh-ds-gsap`.DATE
# MAGIC                 UNION
# MAGIC                     SELECT 
# MAGIC                     "ZM2" AS EXCHANGERATE_TYPE
# MAGIC                     ,"USD" AS TOCURRENCY
# MAGIC                     ,"9999-01-01" AS FULL_DATE
# MAGIC                     ,"9999" AS YEAR
# MAGIC                     ,"9999-Q1" AS QUARTER
# MAGIC                     ,"9999-01" AS MONTHYEAR
# MAGIC                     ,01  AS DAYOFMONTH
# MAGIC                     FROM `CROSS_DS-UNITYCATALOG-DEV`.`euh-ds-gsap`.DATE
# MAGIC                 UNION
# MAGIC                     SELECT 
# MAGIC                     "ZP1" AS EXCHANGERATE_TYPE
# MAGIC                     ,"USD" AS TOCURRENCY
# MAGIC                     ,"9999-01-01" AS FULL_DATE
# MAGIC                     ,"9999" AS YEAR
# MAGIC                     ,"9999-Q1" AS QUARTER
# MAGIC                     ,"9999-01" AS MONTHYEAR
# MAGIC                     ,01  AS DAYOFMONTH
# MAGIC                     FROM `CROSS_DS-UNITYCATALOG-DEV`.`euh-ds-gsap`.DATE
# MAGIC                 UNION
# MAGIC                     SELECT 
# MAGIC                     "ZQ1" AS EXCHANGERATE_TYPE
# MAGIC                     ,"USD" AS TOCURRENCY
# MAGIC                     ,"9999-01-01" AS FULL_DATE
# MAGIC                     ,"9999" AS YEAR
# MAGIC                     ,"9999-Q1" AS QUARTER
# MAGIC                     ,"9999-01" AS MONTHYEAR
# MAGIC                     ,01  AS DAYOFMONTH
# MAGIC                     FROM `CROSS_DS-UNITYCATALOG-DEV`.`euh-ds-gsap`.DATE
# MAGIC                 UNION
# MAGIC                     SELECT 
# MAGIC                     "ZY1" AS EXCHANGERATE_TYPE
# MAGIC                     ,"USD" AS TOCURRENCY
# MAGIC                     ,"9999-01-01" AS FULL_DATE
# MAGIC                     ,"9999" AS YEAR
# MAGIC                     ,"9999-Q1" AS QUARTER
# MAGIC                     ,"9999-01" AS MONTHYEAR
# MAGIC                     ,01  AS DAYOFMONTH
# MAGIC                     FROM `CROSS_DS-UNITYCATALOG-DEV`.`euh-ds-gsap`.DATE                 
# MAGIC                  
