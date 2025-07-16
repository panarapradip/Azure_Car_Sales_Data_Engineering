# Databricks notebook source
# MAGIC %md
# MAGIC # Data Reading from Bronze Layer

# COMMAND ----------

df = spark.read.format('parquet')\
            .option('inferSchema', True)\
            .load('abfss://bronze@carpradipdatalake.dfs.core.windows.net/RawData')

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Transformations

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('Units_Sold',col('Units_Sold').cast(StringType())).printSchema()

# COMMAND ----------

df = df.withColumn('Revenue_per_unit', col('Revenue')/col('Units_Sold'))
df.display()

# COMMAND ----------

df.groupBy('Year','BranchName').agg(sum('Units_Sold').alias('Total_Units_Sold')).sort('Year','Total_Units_Sold', ascending = [1,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing to Silver Layer

# COMMAND ----------

df.write.format('parquet')\
        .mode('overwrite')\
        .option('path', 'abfss://silver@carpradipdatalake.dfs.core.windows.net/carsales')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Querying Parquet Data from Silver Layer

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@carpradipdatalake.dfs.core.windows.net/carsales`

# COMMAND ----------

