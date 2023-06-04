# Databricks notebook source
spark


# COMMAND ----------

df = spark.read.csv(r"/FileStore/batch10_part1.csv", header=True, inferSchema=True)
df.display()
df.printSchema()
print(len(df.columns))

# COMMAND ----------

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
        .drop('Patient Hospital Readmission data availability Code')\
        

df2.display()
         



        
        
        
    

    

# COMMAND ----------

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


# COMMAND ----------

print(len(df2.columns))

# COMMAND ----------

##concating the address columns

from pyspark.sql.functions import concat_ws

df3 = df2.withColumn('Facility_Address', concat_ws(',','Address Line 1','Address Line 2','County/Parish','ZIP Code'))\
         .drop('Address Line 1')\
         .drop('Address Line 2')\
         .drop('ZIP Code')\
         .drop('County/Parish')

df3.display()
print(len(df3.columns))


# COMMAND ----------

df4 = df3.orderBy('Certification Number')
df4.display()

# COMMAND ----------

df4 = df4.withColumnRenamed('Network', 'NO.of_chain_facilities')
df4.display()

# COMMAND ----------

df5 = df4.drop('Five Star Date','Hgb > 12 data availability code','Number of Dialysis Patients with Hgb data','Patient Transfusion category text','Number of patients included in the transfusion summary')\
         .withColumnRenamed('Five Star', 'Facility rating')\
         .withColumnRenamed('Late Shift', 'Rotational Shifts')\
         .withColumnRenamed('# of Dialysis Stations', 'Dialysis_Stations_Available')\
         .withColumnRenamed('Offers in-center hemodialysis', 'availability of in-center hemodialysis')\
         .withColumnRenamed('Offers peritoneal dialysis', 'availability of peritoneal dialysis')\
         .withColumnRenamed('Offers home hemodialysis training', 'Availibility of HD Training')\
         .withColumnRenamed('Effective Date', 'Facility_Start_Date')\
         .withColumnRenamed('Percentage of Medicare patients with Hgb<10 g/dL', '%_patients_Hgb<10 g/dL')\
         .withColumnRenamed('Percentage of Medicare patients with Hgb>12 g/dL', '%_patients_Hgb>10 g/dL')\
         .withColumnRenamed('Percentage of Medicare patients with Hgb>12 g/dL', '%_patients_Hgb>10 g/dL')\

df5.display()
print(f"Total number of columns in df5 = {len(df5.columns)}")


# COMMAND ----------

df5 = df5.drop('Number of adult HD patients with KT/V data','Number of adult PD patients with KT/V data','Number of pediatric HD patients with Kt/V data',
'Number of patient-months in hypercalcemia summary','SRR Date','Patient hospitalization category text','Patient Hospital Readmission Category','Mortality Rate: Upper Confidence Limit (97.5%)','Readmission Rate: Upper Confidence Limit (97.5%)','Readmission Rate: Lower Confidence Limit (2.5%)','Hospitalization Rate: Upper Confidence Limit (97.5%)','Readmission Rate: Lower Confidence Limit (2.5%)','Hospitalization Rate: Upper Confidence Limit (97.5%)','Hospitalization Rate: Lower Confidence Limit (2.5%)','SIR Date','Patient Infection category text','SIR: Upper Confidence Limit (97.5%)','SIR: Upper Confidence Limit (97.5%)','SIR: Lower Confidence Limit (2.5%)','Transfusion Rate: Upper Confidence Limit (97.5%)','Transfusion Rate: Lower Confidence Limit (2.5%)','Fistula Category Text','Fistula Rate: Upper Confidence Limit (97.5%)','Fistula Rate: Lower Confidence Limit (2.5%)','Long term catheter Data Availability Code','Number of patients in nPCR summary','SWR Date','SWR category text','Patient transplant waitlist data availability code','95% C.I. (upper limit) for SWR','95% C.I. (lower limit) for SWR','PPPW category text','95% C.I. (upper limit) for PPPW','95% C.I. (lower limit) for PPPW','')\
        .withColumnRenamed('Percent of Adult HD patients with Kt/V >= 1.2', '%_Adult_HDpatients_Hgb<10 g/dL')\
        .withColumnRenamed('Percent of Adult HD patients with Kt/V >= 1.2', '%_Adult_HD_patients_Hgb<10 g/dL')\
        .withColumnRenamed('Percentage of Adult PD PTS with Kt/V >= 1.7', '%_Adult_PD_patients_Hgb<10 g/dL')\
        .withColumnRenamed('Percentage of Adult PD PTS with Kt/V >= 1.7', '%_Adult_PD_patients_Hgb<10 g/dL')\
        .withColumnRenamed('Number of adult HD patient-months with Kt/V data', 'Patients_Month_with_Kt/V_data')\
        .withColumnRenamed('Number of adult HD patient-months with Kt/V data', 'Patients_Month_with_Kt/V_data')\
        .withColumnRenamed('Number of patients in hypercalcemia summary', 'Patients_Number_with_hypercalcemia_summary')\
        .withColumnRenamed('Percentage of Adult patients with hypercalcemia (serum calcium greater than 10.2 mg/dL)', '%_Patients_with_Hypercalcemia_(>10.2 mg/dL)')\
        .withColumnRenamed('Percentage of Adult PD PTS with Kt/V >= 1.7', '%_Adult_PD_patients_Hgb<10 g/dL')\
        .withColumnRenamed('Number of patients in Serum phosphorus summary', 'Patients_in_Serum_Phosphorous_summary')\
        .withColumnRenamed('Percentage of Adult patients with serum phosphorus less than 3.5 mg/dL', '%_Adult_Patients_with_serum_phosph._<3.5mg/dL')\
        .withColumnRenamed('Percentage of Adult patients with serum phosphorus between 3.5-4.5 mg/dL', '%_Adult_Patients_with_serum_phosph._btw_3.5-4.5 mg/dL')\
        .withColumnRenamed('Percentage of Adult patients with serum phosphorus between 4.6-5.5 mg/dL', '%_Adult_Patients_with_serum_phosph._btw_4.6-5.5 mg/dL mg/dL')\
        .withColumnRenamed('Percentage of Adult patients with serum phosphorus between 5.6-7.0 mg/dL', '%_Adult_Patients_with_serum_phosph._btw_5.6-7.0 mg/dL')\
        .withColumnRenamed('Percentage of Adult patients with serum phosphorus greater than 7.0 mg/dL', '%_Adult_Patients_with_serum_phosph._>7.0mg/dL')\
        .withColumnRenamed('Number of patients included in hospitalization summary', 'Num_Patients_in_hospitalization_summary')\
        .withColumnRenamed('Number of hospitalizations included in hospital readmission summary', 'Num_Patients_in_hospitalization_readmission_summary')\
        .withColumnRenamed('Number of Patients included in survival summary', 'Num_Patients_in_survival_summary')\
        .withColumnRenamed('Mortality Rate (Facility)', 'Mortality_Rate_per_Facility')\
        .withColumnRenamed('Readmission Rate (Facility)', 'Readmission_Rate_per_Facility')\
        .withColumnRenamed('Hospitalization Rate (Facility)', 'Hospitalization_Fees(Facility_Wise)')\
        .withColumnRenamed('Number of pediatric PD patients with Kt/V data', 'Num_of_pediatric_PD_Patients_with_Kt/V_data')\
        .withColumnRenamed('Standard Infection Ratio', 'Standard_Infection_Ratio_(Facility_wise)')\
        .withColumnRenamed('Transfusion Rate (Facility)', 'Transfusion_Rate_(Facility_Wise)')\
        .withColumnRenamed('Number of Patients included in fistula summary', 'Num_Patients_in_fistula_summary')\
        .withColumnRenamed('Fistula Rate (Facility)', 'Fitsula_rate_(facility_wise)')\
        .withColumnRenamed('Number of patients in long term catheter summary', 'Num_Patients_in_long_term_catheter_summary')\
        .withColumnRenamed('Number of patient months in long term catheter summary', 'Num_Patients_months_in_long_term_catheter_summary')\
        .withColumnRenamed('Percentage of Adult patients with long term catheter in use', '%_Adult_patients_with_long_term_catheter')\
        .withColumnRenamed('Number of patients in this facility for SWR', 'Num_patients_in_facility_for_SWR')\
        .withColumnRenamed('Standardized First Kidney Transplant Waitlist Ratio', 'Kidney_transplant_waitlist_ratio')\
        .withColumnRenamed('Number of patients for PPPW', 'Num_patients_for_PPPW')\
        .withColumnRenamed('Percentage of Prevalent Patients Waitlisted', '%_prevalent_patients_waitlisted')\

df5.display()

# COMMAND ----------

print(f"Total number of columns in df5 = {len(df5.columns)}")

# COMMAND ----------

from pyspark.sql.functions import lit

#df6 = df5.withColumn('Profit')\
#         .withColumn('non-profit')
from pyspark.sql.functions import when

matches = df5["Profit or Non-Profit"].isin("Profit")
df6 = df5.withColumn("Profit", when(matches, "YES").otherwise("NO"))
                             


# COMMAND ----------

matches2 = df5["Profit or Non-Profit"].isin("Non-profit")
df6 = df6.withColumn("Non-Profit", when(matches2, "YES").otherwise("NO"))

# COMMAND ----------

df6 = df6.drop('Profit or Non-Profit')
df6.display()

# COMMAND ----------

print(f"Total number of columns in df5 = {len(df6.columns)}")

# COMMAND ----------

df7 = df6.na.fill(value = '0',subset = ['Facility rating'])

df7.display()





# COMMAND ----------


