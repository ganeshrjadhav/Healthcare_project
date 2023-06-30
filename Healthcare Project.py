# Databricks notebook source
spark

# COMMAND ----------

df= spark.read.csv(r"dbfs:/FileStore/DFC_FACILITY.csv" , header= True , inferSchema= True)
df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

len(df.columns)

# COMMAND ----------

df1= df.drop("Five Star Data Availability Code","Chain Owned", "EQRS Date","STrR Date", "HGB<10 data availability code",
    "Patient Transfusion data availability Code","Adult HD Kt/V data availability code", "Adult PD Kt/V data availability code" ,"Percentage of Pediatric HD patients with Kt/V >= 1.2", "Pediatric HD Kt/V Data Availability Code", "Pediatric HD Kt/V Data Availability Code", "Hypercalcemia Data Availability Code" ,"Serum phosphorus Data Availability Code", "Patient Hospitalization data availability Code" ,"Patient Hospital Readmission data availability Code","Patient Survival data availability code" ,"Pediatric PD Kt/V Data Availability Code" ,"Number of pediatric PD patient-months with KT/V data", "Percentage of pediatric PD patients with Kt/V>=1.8" ,"Patient Infection Data Availability Code" ,"Fistula data availability code" ,"Number of patient-months in nPCR summary ", "nPCR Data Availability Code" ,"Percentage of pediatric HD patients with nPCR" ,"Patient prevalent transplant waitlist data availability code")

df1.display()

# COMMAND ----------

len(df1.columns)

# COMMAND ----------

df2 = df1.withColumnRenamed("Five Star","Hospital rating") \
    .withColumnRenamed("Telephone Number","Contact") \
    .withColumnRenamed("Late Shift","Work in shift") \
    .withColumnRenamed("# of Dialysis Stations","available dialysis stations") \
    .withColumnRenamed("Offers in-center hemodialysis","availability of in-center hemodialysis") \
    .withColumnRenamed("Offers peritoneal dialysis","availability of in-center peritoneal dialysis") \
    .withColumnRenamed("Offers home hemodialysis training","availability of  home hemodialysis training") \
    .withColumnRenamed("Effective Date"," facility available from") \
    .withColumnRenamed("Percentage of Medicare patients with Hgb<10 g/dL","Low  Hb Patients  Percentage") \
    .withColumnRenamed("Percentage of Medicare patients with Hgb>12 g/dL","Normal Hb Patients Percentage") \
    .withColumnRenamed("Hgb > 12 data availability code","Normal Hb data availability-code") \
    .withColumnRenamed("Number of Dialysis Patients with Hgb data","Number  of Dialysis Patients with Hb data") \
    .withColumnRenamed("Patient Transfusion category text","patient_condition_after_transfusion") \
    .withColumnRenamed("Number of adult HD patients with KT/V data","No. of Hemodialysis  with KT/V data") \
    .withColumnRenamed("Number of adult HD patient-months with Kt/V data","No. of Hemodialysis_Months_KT/V data") \
    .withColumnRenamed("Number of adult PD patient-months with Kt/V data","No. of Peritoneal Dialysis_months_KT/V data") \
    .withColumnRenamed("Number of adult PD patients with KT/V data","No. of Peritoneal Dialysis_KT/V data") \
    .withColumnRenamed("Number of adult PD patient-months with Kt/V data","No. of Peritoneal Dialysis_months_KT/V data") \
    .withColumnRenamed("Number of patients in hypercalcemia summary","Blood Calcium Summary") \
    .withColumnRenamed("Number of patient-months in hypercalcemia summary","Blood Calcium Summary in Months") \
    .withColumnRenamed("Percentage of Adult patients with hypercalcemia (serum calcium greater than 10.2 mg/dL)","Percentage of Blood Calcium > 10.2 mg/dL") \
    .withColumnRenamed("Number of patients in Serum phosphorus summary","Serum phosphorus summary") \
    .withColumnRenamed("Number of patient-months in Serum phosphorus summary","Serum phosphorus summary in months") \
    .withColumnRenamed("Percentage of Adult patients with serum phosphorus less than 3.5 mg/dL","Percentage of Serum phosphorus less than 3.5 mg/dL") \
    .withColumnRenamed("Percentage of Adult patients with serum phosphorus between 3.5-4.5 mg/dL","Percentage of Serum phosphorus between 3.5-4.5 mg/dL") \
    .withColumnRenamed("Percentage of Adult patients with serum phosphorus between 4.6-5.5 mg/dL","Percentage of Serum phosphorus between 4.6-5.5 mg/dL") \
    .withColumnRenamed("Percentage of Adult patients with serum phosphorus between 5.6-7.0 mg/dL","Percentage of Serum phosphorus between 5.6-7.0 mg/dL") \
    .withColumnRenamed("Percentage of Adult patients with serum phosphorus greater than 7.0 mg/dL","Percentage of Adult Phosphorus greater than 7.0 mg/dL") \
    .withColumnRenamed("Number of patients included in hospitalization summary","Number of Hospitalized Patients") \
    .withColumnRenamed("Number of hospitalizations included in hospital readmission summary","Number of Hospitalized Patients in Readmission Summary") \
    .withColumnRenamed("Number of Patients included in survival summary","Number of Patients in Survival Summary") \
    .withColumnRenamed("Mortality Rate (Facility)","Death_to_Case_Ratio ") \
    .withColumnRenamed("Mortality Rate: Upper Confidence Limit (97.5%)","Death_ to_Case_Ratio: Upper Limit 97.5%") \
    .withColumnRenamed("Mortality Rate: Lower Confidence Limit (2.5%)","Death_to_Case_Ratio: Lower Limit 2.5%") \
    .withColumnRenamed("Readmission Rate (Facility)","Readmission Rate (Facility)") \
    .withColumnRenamed("Readmission Rate: Upper Confidence Limit (97.5%)","Readmission Rate: Upper Confidence Limit (97.5%)") \
    .withColumnRenamed("Readmission Rate: Lower Confidence Limit (2.5%)","Readmission Rate: Lower Confidence Limit (2.5%)") \
    .withColumnRenamed("Hospitalization Rate (Facility)","Hospitalization Rate (Facility)") \
    .withColumnRenamed("Hospitalization Rate: Upper Confidence Limit (97.5%)","Hospitalization Rate: Upper Confidence Limit (97.5%)") \
    .withColumnRenamed("Hospitalization Rate: Lower Confidence Limit (2.5%)","Hospitalization Rate: Lower Confidence Limit (2.5%)") \
    .withColumnRenamed("Number of pediatric PD patients with Kt/V data","Number of PD  patients_Kt/V data") \
    .withColumnRenamed("Standard Infection Ratio","SIR") \
    .withColumnRenamed("SIR: Upper Confidence Limit (97.5%)","SIR: Upper Confidence Limit (97.5%)") \
    .withColumnRenamed("SIR: Lower Confidence Limit (2.5%)","SIR: Lower Confidence Limit (2.5%)") \
    .withColumnRenamed("Transfusion Rate (Facility)","Transfusion Rate (Facility)") \
    .withColumnRenamed("Transfusion Rate: Upper Confidence Limit (97.5%)","Transfusion Rate: Upper Confidence Limit (97.5%)") \
    .withColumnRenamed("Transfusion Rate: Lower Confidence Limit (2.5%)","Transfusion Rate: Lower Confidence Limit (2.5%)") \
    .withColumnRenamed("Number of Patients included in fistula summary","Number of Patients included in fistula summary") \
    .withColumnRenamed("Fistula Rate (Facility)","Fistula Rate (Facility)") \
    .withColumnRenamed("Fistula Rate: Upper Confidence Limit (97.5%)","Fistula Rate: Upper Confidence Limit (97.5%)") \
    .withColumnRenamed("Fistula Rate: Lower Confidence Limit (2.5%)","Fistula Rate: Lower Confidence Limit (2.5%)") \
    .withColumnRenamed("Number of patients in long term catheter summary","Patients in long term Catheter data") \
    .withColumnRenamed("Number of patient months in long term catheter summary","Patients in months_long term Catheter data") \
    .withColumnRenamed("Long term catheter Data Availability Code","Availability code_Catheter Data") \
    .withColumnRenamed("Percentage of Adult patients with long term catheter in use","Percentage_use of long term catheter ") \
    .withColumnRenamed("Number of patients in nPCR summary","Total nPCR Data") \
    .withColumnRenamed("Patient transplant waitlist data availability code","Availability code for trsnsplant data") \
    .withColumnRenamed("95% C.I. (upper limit) for SWR","SWR_Upper limit ") \
    .withColumnRenamed("95% C.I. (lower limit) for SWR","SWR_Lower limit") \
    .withColumnRenamed("Number of patients in this facility for SWR","SWR Patients") \
    .withColumnRenamed("Standardized First Kidney Transplant Waitlist Ratio","Ratio of order of Kidney Transplant") \
    .withColumnRenamed("95% C.I. (upper limit) for PPPW","PPPW_Upper limit") \
    .withColumnRenamed("95% C.I. (lower limit) for PPPW","PPPW_Lower limit") \
    .withColumnRenamed("Number of patients for PPPW","Patients for PPPW") \
    .withColumnRenamed("Percentage of Prevalent Patients Waitlisted","PPPW") 

df2.display()

# COMMAND ----------

from pyspark.sql.functions import col
df2.orderBy(col("Certification Number").asc()).display()
#df2.orderBy(col("Certification Number").asc()).display()

# COMMAND ----------

from pyspark.sql.functions import concat_ws

df3=df2.withColumn("Address",concat_ws(',', 'Address Line 1', 'Address Line 2' , 'ZIP Code','County/Parish'))\
       .drop("Address Line 1", "Address Line 2","ZIP Code","County/Parish") 

df3.display()


# COMMAND ----------

len(df3.columns)

# COMMAND ----------

from pyspark.sql.functions import substring 
df4=df3.withColumn('Claims Start date', substring('Claims Date', 1,9))\
       .withColumn('Claims End date', substring('Claims Date', 11,9))
df4.display()

# COMMAND ----------

len(df4.columns)

# COMMAND ----------

from pyspark.sql.functions import split
df5 = df4.withColumn('Profit', split(df['Profit or Non-Profit'], '-').getItem(0)) \
       .withColumn('Non Profit', split(df['Profit or Non-Profit'], '-').getItem(1))        
df5.display()

# COMMAND ----------

#df6 = df5.groupBy("Network").count()
#df6.display()

# COMMAND ----------


