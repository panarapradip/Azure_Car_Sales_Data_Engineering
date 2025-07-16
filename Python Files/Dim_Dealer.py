# Databricks notebook source
dbutils.widgets.text('Incremental_Flag','0')

# COMMAND ----------

Incremental_Flag = dbutils.widgets.get('Incremental_Flag')
print(Incremental_Flag)

# COMMAND ----------

df_src = spark.sql('''
    select distinct(Dealer_ID) as Dealer_ID, DealerName 
    from parquet.`abfss://silver@carpradipdatalake.dfs.core.windows.net/carsales`
    ''')

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    df_sink = spark.sql('''
        select dim_dealer_key, Dealer_ID, DealerName
        from delta.`abfss://gold@carpradipdatalake.dfs.core.windows.net/dim_dealer`
        ''')
else:
    df_sink = spark.sql('''
        select 1 as dim_dealer_key, Dealer_ID, DealerName
        from parquet.`abfss://silver@carpradipdatalake.dfs.core.windows.net/carsales`
        where 1 = 0
        ''')


# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.Dealer_ID == df_sink.Dealer_ID, 'left').select(df_src.Dealer_ID, df_src.DealerName, df_sink.dim_dealer_key)

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter.dim_dealer_key.isNotNull())
df_filter_new = df_filter.filter(df_filter.dim_dealer_key.isNull()).select(df_filter.Dealer_ID, df_filter.DealerName)

# COMMAND ----------

if (Incremental_Flag == '0'):
    max_value = 1
else:
    max_value_df = spark.sql("select max(dim_dealer_key) from cars_catalog.gold.dim_dealer")
    max_value = max_value_df.collect()[0][0]+1

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_dealer_key', max_value+monotonically_increasing_id())

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_dealer'):
    delta_table = DeltaTable.forPath(spark, 'abfss://gold@carpradipdatalake.dfs.core.windows.net/dim_dealer')

    delta_table.alias('trg').merge(df_final.alias('src'), "trg.Dealer_ID = src.Dealer_ID")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:    
    df_final.write.format('delta')\
            .mode('overwrite')\
            .option('path','abfss://gold@carpradipdatalake.dfs.core.windows.net/dim_dealer')\
            .saveAsTable('cars_catalog.gold.dim_dealer')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_dealer

# COMMAND ----------

