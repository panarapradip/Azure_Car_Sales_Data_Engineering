# Databricks notebook source
dbutils.widgets.text('Incremental_Flag','0')

# COMMAND ----------

Incremental_Flag = dbutils.widgets.get('Incremental_Flag')
print(Incremental_Flag)

# COMMAND ----------

df_src = spark.sql('''
    select distinct(Date_ID) as Date_ID
    from parquet.`abfss://silver@carpradipdatalake.dfs.core.windows.net/carsales`
    ''')

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
    df_sink = spark.sql('''
        select dim_date_key, Date_ID
        from delta.`abfss://gold@carpradipdatalake.dfs.core.windows.net/dim_date`
        ''')
else:
    df_sink = spark.sql('''
        select 1 as dim_date_key, Date_ID
        from parquet.`abfss://silver@carpradipdatalake.dfs.core.windows.net/carsales`
        where 1 = 0
        ''')


# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.Date_ID == df_sink.Date_ID, 'left').select(df_src.Date_ID,df_sink.dim_date_key)

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter.dim_date_key.isNotNull())
df_filter_new = df_filter.filter(df_filter.dim_date_key.isNull()).select(df_filter.Date_ID)

# COMMAND ----------

if (Incremental_Flag == '0'):
    max_value = 1
else:
    max_value_df = spark.sql("select max(dim_date_key) from cars_catalog.gold.dim_date")
    max_value = max_value_df.collect()[0][0]+1

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_date_key', max_value+monotonically_increasing_id())

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_date'):
    delta_table = DeltaTable.forPath(spark, 'abfss://gold@carpradipdatalake.dfs.core.windows.net/dim_date')

    delta_table.alias('trg').merge(df_final.alias('src'), "trg.Date_ID = src.Date_ID")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:    
    df_final.write.format('delta')\
            .mode('overwrite')\
            .option('path','abfss://gold@carpradipdatalake.dfs.core.windows.net/dim_date')\
            .saveAsTable('cars_catalog.gold.dim_date')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_date

# COMMAND ----------

