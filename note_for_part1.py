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

