# Databricks notebook source
# MAGIC %md
# MAGIC ## Create Parameter for Incremental Load

# COMMAND ----------

dbutils.widgets.text('Incremental_Flag','0')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Assgining Variable to Parameter

# COMMAND ----------

Incremental_Flag = dbutils.widgets.get('Incremental_Flag')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading Silver Data and create Dimenstion Table for Dim_Model

# COMMAND ----------

df_src = spark.sql('''
    select distinct(Model_ID) as Model_ID, Product_Name
    from parquet.`abfss://silver@carpradipdatalake.dfs.core.windows.net/carsales`
    ''')

# COMMAND ----------

df_src.display()

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):

    df_sink = spark.sql('''
        select dim_model_key, Model_ID, Product_Name
        from delta.`abfss://gold@carpradipdatalake.dfs.core.windows.net/dim_model`
        ''')

else:

    df_sink = spark.sql('''
        select 1 as dim_model_key, Model_ID, Product_Name
        from parquet.`abfss://silver@carpradipdatalake.dfs.core.windows.net/carsales`
        where 1 = 0
        ''')

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.Model_ID == df_sink.Model_ID, 'left').select(df_src.Model_ID,df_src.Product_Name, df_sink.dim_model_key)
df_filter.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering Old Records

# COMMAND ----------

dim_model_old = df_filter.filter(df_filter.dim_model_key.isNotNull())
dim_model_old.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering New Records

# COMMAND ----------

dim_model_new = df_filter.filter(df_filter.dim_model_key.isNull()).select(df_filter.Model_ID,df_filter.Product_Name)


# COMMAND ----------

dim_model_new.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

if (Incremental_Flag == '0'):
    max_value = 1
else:
    max_value_df = spark.sql("select max(dim_model_key) from cars_catalog.gold.dim_model")
    max_value = max_value_df.collect()[0][0]+1

# COMMAND ----------

df_filter_new = dim_model_new.withColumn('dim_model_key',max_value+monotonically_increasing_id())

# COMMAND ----------

df_final = df_filter_new.union(dim_model_old)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_model'):
    delta_table = DeltaTable.forPath(spark, 'abfss://gold@carpradipdatalake.dfs.core.windows.net/dim_model')
    delta_table.alias('trg').merge(df_final.alias('src'), 'trg.dim_model_key = src.dim_model_key')\
            .whenMatchedUpdateAll()\
            .whenNotMatchedInsertAll()\
            .execute()

else:
    df_final.write.format('delta')\
            .mode('overwrite')\
            .option('path','abfss://gold@carpradipdatalake.dfs.core.windows.net/dim_model')\
            .saveAsTable('cars_catalog.gold.dim_model')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_model

# COMMAND ----------

