# Databricks notebook source
spark


# COMMAND ----------

df = spark.read.csv(r"/FileStore/batch10_part1.csv", header=True, inferSchema=True)
df.display()
df.printSchema()
df.count()

# COMMAND ----------

#Column Rename (Capitilize by Integer datatype columns)
from pyspark.sql.types import IntegerType
for col in df.columns:
    if (df.schema[col].dataType)==IntegerType():
        df = df.withColumnRenamed(col, col.upper())
df.display()
    

    

# COMMAND ----------

df = spark.sql("select * from <your table name >")
new_column_name_list= list(map(lambda x: x.lower(), df.columns))
df = df.toDF(*new_column_name_list)
display(df)df


##test code

##Deleted the unnecessary columns
##second test
df2 = df.drop('Five Star Data Availability Code')\
        .drop('Chain Owned')\
        .drop('EQRS Date')\
        .drop('STrR Date')\
        .drop('HGB<10 data availability code')\
        .drop('Patient Transfusion data availability Code')\
        .drop('Adult HD Kt/V data availability code')\
        .drop('Adult PD Kt/V data availability code')\
        .drop('Percentage of Pediatric HD patients with Kt/V >= 1.2')\
        .drop('Pediatric HD Kt/V Data Availability Code')\
        .drop('Number of pediatric HD patient-months with KT/V data')\
        .drop('Hypercalcemia Data Availability Code')\
        .drop('Serum phosphorus Data Availability Code')\
        .drop('Patient Hospitalization data availability Code')\
        .drop('Patient Hospital Readmission data availability Code')

df2 = df2.drop('Patient Survival data availability code')\
         .drop('Pediatric PD Kt/V Data Availability Code')\
         .drop('Number of pediatric PD patient-months with KT/V data')\
         .drop('Number of pediatric PD patient-months with KT/V data')\
         .drop('Percentage of pediatric PD patients with Kt/V>=1.8')\
         .drop('Patient Infection Data Availability Code')\
         .drop('Fistula data availability code')\
         .drop('Number of patient-months in nPCR summary')\
         .drop('nPCR Data Availability Code')\
         .drop('Percentage of pediatric HD patients with nPCR')\
         .drop('Patient prevalent transplant waitlist data availability code')

df2.display()

##
##
##concating the address columns

from pyspark.sql.functions import concat_ws

df3 = df2.withColumn('Facility_Address', concat_ws(',','Address Line 1','Address Line 2','County/Parish','ZIP Code'))\
         .drop('Address Line 1')\
         .drop('Address Line 2')\
         .drop('ZIP Code')\
         .drop('County/Parish')

df3.display()
print(len(df3.columns))

#Order by certification number

df4 = df3.orderBy('Certification Number')
df4.display()

#reaname Network column

df4 = df4.withColumnRenamed('Network', 'NO.of_chain_facilities')
df4.display()