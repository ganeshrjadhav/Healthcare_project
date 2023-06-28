# Databricks notebook source
spark

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()


# COMMAND ----------

df = spark.read.csv(r"dbfs:/FileStore/tables/Project_DFC_FACILITY.csv", header=True,inferSchema=True)
df.display()


# COMMAND ----------

df.printSchema()

# COMMAND ----------

df1=df.drop("Five Star Data Availability Code","EQRS Date","STrR Date")
df1.display()

# COMMAND ----------

print(len(df1.columns))

# COMMAND ----------

df2=df1.drop("Adult PD Kt/V data availability code","Adult HD Kt/V data availability code","Patient Transfusion data availability Code",)
df2.display()

# COMMAND ----------

print(len(df2.columns))

# COMMAND ----------

from pyspark.sql.functions import concat_ws,split


# COMMAND ----------

df3=df2.withColumn("Address",concat_ws(',', "Address Line 1", "Address Line 2","ZIP Code","County/Parish"))\
       .drop("Address Line 1",',', "Address Line 2",',',"ZIP Code",',',"County/Parish") 
df3.display()

# COMMAND ----------

df5=df3.drop("Chain Owned","HGB<10 data availability code","Percentage of Pediatric HD patients with Kt/V >= 1.2","Pediatric HD Kt/V Data Availability Code","Number of pediatric HD patient-months with KT/V data","Hypercalcemia Data Availability Code","Serum phosphorus Data Availability Code","Patient Hospitalization data availability Code","Patient Hospital Readmission data availability Code","Patient Survival data availability code","Pediatric PD Kt/V Data Availability Code","Number of pediatric PD patient-months with KT/V data","Percentage of pediatric PD patients with Kt/V>=1.8","Patient Infection Data Availability Code","Fistula data availability code","Number of patient-months in nPCR summary","nPCR Data Availability Code","Percentage of pediatric HD patients with nPCR","Patient prevalent transplant waitlist data availability code")
df5.display()

# COMMAND ----------

df6 = df5.withColumnRenamed("Five Star","Hospital rating")\
         .withColumnRenamed("Telephone Number","Contact")\
         .withColumnRenamed("Late Shift","Work in shift")\
         .withColumnRenamed("# of Dialysis Stations","Available dialysis stations")\
         .withColumnRenamed("Offers in-center hemodialysis","Availability of in-center hemodialysis")\
         .withColumnRenamed("Offers peritoneal dialysis","Availability of in-center peritoneal dialysis")\
         .withColumnRenamed("Offers home hemodialysis training","Availability of  home hemodialysis training")\
         .withColumnRenamed("Effective Date"," Facility available from")\
         .withColumnRenamed("Percentage of Medicare patients with Hgb<10 g/dL","Low  Hb Patients  Percentage")\
         .withColumnRenamed("Percentage of Medicare patients with Hgb>12 g/dL","Normal Hb Patients Percentage")\
         .withColumnRenamed("Hgb > 12 data availability code","Normal Hb data availability-code")\
         .withColumnRenamed("Number of Dialysis Patients with Hgb data","Number  of Dialysis Patients with Hb data")\
         .withColumnRenamed("Patient Transfusion category text","Patient condition after transfusion")\
         .withColumnRenamed("# Number of adult HD patients with KT/V data","No. of Hemodialysis  with KT/V data")\
         .withColumnRenamed("Number of adult HD patient-months with Kt/V data","No. of Hemodialysis months KT/V data")\
         .withColumnRenamed("Number of adult PD patient-months with Kt/V data","No. of Peritoneal Dialysis_months_KT/V data")\
         .withColumnRenamed("Number of adult PD patients with KT/V data","No. of Peritoneal Dialysis KT/V data")\
         .withColumnRenamed("Number of adult PD patient-months with Kt/V data","No. of Peritoneal Dialysis months KT/V data")\
         .withColumnRenamed("Number of patients in hypercalcemia summary","Blood Calcium Summary")\
         .withColumnRenamed("Number of patient-months in hypercalcemia summary","Blood Calcium Summary in Months")\
         .withColumnRenamed("Percentage of Adult patients with hypercalcemia (serum calcium greater than 10.2 mg/dL)","Percentage of Blood Calcium > 10.2 mg/dL")\
         .withColumnRenamed("Number of patients in Serum phosphorus summary","Serum phosphorus summary")\
         .withColumnRenamed("Number of patient-months in Serum phosphorus summary","Serum phosphorus summary in months")\
         .withColumnRenamed("Percentage of Adult patients with serum phosphorus less than 3.5 mg/dL","Percentage of Serum phosphorus less than 3.5 mg/dL")\
         .withColumnRenamed("Percentage of Adult patients with serum phosphorus between 3.5-4.5 mg/dL","Percentage of Serum phosphorus between 3.5-4.5 mg/dL")\
         .withColumnRenamed("Percentage of Adult patients with serum phosphorus between 4.6-5.5 mg/dL","Percentage of Serum phosphorus between 4.6-5.5 mg/dL")\
         .withColumnRenamed("Percentage of Adult patients with serum phosphorus between 5.6-7.0 mg/dL","Percentage of Serum phosphorus between 5.6-7.0 mg/dL")\
         .withColumnRenamed("Percentage of Adult patients with serum phosphorus greater than 7.0 mg/dL","Percentage of Adult Phosphorus greater than 7.0 mg/dL")\
         .withColumnRenamed("Number of patients included in hospitalization summary","Number of Hospitalized Patients")\
         .withColumnRenamed("Number of hospitalizations included in hospital readmission summary","Number of Hospitalized Patients in Readmission Summary")\
         .withColumnRenamed("Number of Patients included in survival summary","Number of Patients in Survival Summary")\
         .withColumnRenamed("Mortality Rate (Facility)","Death to Case Ratio ")\
         .withColumnRenamed("Mortality Rate: Upper Confidence Limit (97.5%)","Death to Case Ratio: Upper Limit 97.5%")\
         .withColumnRenamed("Mortality Rate: Lower Confidence Limit (2.5%)","Death to Case Ratio: Lower Limit 2.5%")\
         .withColumnRenamed("Readmission Rate (Facility)","Readmission Rate (Facility)")\
         .withColumnRenamed("Readmission Rate: Upper Confidence Limit (97.5%)","Readmission Rate: Upper Confidence Limit (97.5%)")\
         .withColumnRenamed("Readmission Rate: Lower Confidence Limit (2.5%)","Readmission Rate: Lower Confidence Limit (2.5%)")\
         .withColumnRenamed("Hospitalization Rate (Facility)","Hospitalization Rate(Facility)")\
         .withColumnRenamed("Hospitalization Rate: Upper Confidence Limit (97.5%)","Hospitalization Rate: Upper Confidence Limit (97.5%)")\
         .withColumnRenamed("Hospitalization Rate: Lower Confidence Limit (2.5%)","Hospitalization Rate: Lower Confidence Limit (2.5%)")\
         .withColumnRenamed("Number of pediatric PD patients with Kt/V data","Number of PD  patients Kt/V data")\
         .withColumnRenamed("Standard Infection Ratio","SIR")\
         .withColumnRenamed("SIR: Upper Confidence Limit (97.5%)","SIR: Upper Confidence Limit (97.5%)")\
         .withColumnRenamed("SIR: Lower Confidence Limit (2.5%)","SIR: Lower Confidence Limit (2.5%)")\
         .withColumnRenamed("Transfusion Rate (Facility)","Transfusion Rate (Facility)")\
         .withColumnRenamed("Number of patients in long term catheter summary","Patients in long term Catheter data")\
         .withColumnRenamed("Number of patient months in long term catheter summary","Patients in months long term Catheter data")\
         .withColumnRenamed("Long term catheter Data Availability Code","Availability code Catheter Data")\
         .withColumnRenamed("Percentage of Adult patients with long term catheter in use","Percentage use of long term catheter")\
         .withColumnRenamed("Number of patients in nPCR summary","Total nPCR Data")\
         .withColumnRenamed("Patient transplant waitlist data availability code","Availability code for trsnsplant data")\
         .withColumnRenamed("95% C.I. (upper limit) for SWR","SWR Upper limit")\
         .withColumnRenamed("95% C.I. (lower limit) for SWR","SWR Lower limit")\
         .withColumnRenamed("Number of patients in this facility for SWR","SWR Patients")\
         .withColumnRenamed("Standardized First Kidney Transplant Waitlist Ratio","Ratio of order of Kidney Transplant")\
         .withColumnRenamed("95% C.I. (upper limit) for PPPW","PPPW Upper limit")\
         .withColumnRenamed("95% C.I. (lower limit) for PPPW","PPPW Lower limit")\
         .withColumnRenamed("Number of patients for PPPW","Patients for PPPW")\
         .withColumnRenamed("Percentage of Prevalent Patients Waitlisted","PPPW")
df6.display()

# COMMAND ----------

from pyspark.sql.functions import explode

# COMMAND ----------

df6.printSchema()

# COMMAND ----------

from pyspark.sql.functions import substring 


# COMMAND ----------

df7=df6.withColumn('Claims Start date', substring('Claims Date', 1,9))\
       .withColumn('Claims End date', substring('Claims Date', 11,9))\
       .drop('Claims Date', 'Network','Five Star Date','Patient condition after transfusion','Patient hospitalization category text','Patient Hospital Readmission Category','Patient Survival Category Text')
df7.display()

# COMMAND ----------

from pyspark.sql.functions import *


# COMMAND ----------

df8=df7.orderBy(col("Certification Number").asc())
df8.display()

# COMMAND ----------

from pyspark.sql.functions import *
df9=df8.sort(col("Chain Organization").asc(),col("State").asc())
df9.display()

# COMMAND ----------

df10=df9.drop('Profit','Non-Profit')
df10.display()

# COMMAND ----------

df11=df10.withColumn('SHR Start date', substring('SHR Date', 1,9))\
         .withColumn('SHR End date', substring('SHR Date', 11,9))\
         .withColumn('SRR Start date', substring('SRR Date', 1,9))\
         .withColumn('SRR End date', substring('SRR Date', 11,9))\
         .withColumn('SMR Start date', substring('SMR Date', 1,9))\
         .withColumn('SMR End date', substring('SMR Date', 11,9))\
         .drop('SHR Date', 'SRR Date', 'SMR Date')
df11.display()

# COMMAND ----------

df12 = df11.groupBy("Facility Name").pivot("Profit or Non-Profit").sum("Available dialysis stations")
df12.display()
