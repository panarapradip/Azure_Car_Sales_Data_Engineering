# Databricks notebook source
dbutils.widgets.text('Incremental_Flag','0')

# COMMAND ----------

Incremental_Flag = dbutils.widgets.get('Incremental_Flag')
print(Incremental_Flag)

# COMMAND ----------

df_src = spark.sql('''
    select distinct(Branch_id) as Branch_ID, BranchName 
    from parquet.`abfss://silver@carpradipdatalake.dfs.core.windows.net/carsales`
    ''')

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    df_sink = spark.sql('''
        select dim_branch_key, Branch_ID, BranchName
        from delta.`abfss://gold@carpradipdatalake.dfs.core.windows.net/dim_branch`
        ''')
else:
    df_sink = spark.sql('''
        select 1 as dim_branch_key, Branch_ID, BranchName
        from parquet.`abfss://silver@carpradipdatalake.dfs.core.windows.net/carsales`
        where 1 = 0
        ''')


# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.Branch_ID == df_sink.Branch_ID, 'left').select(df_src.Branch_ID, df_src.BranchName, df_sink.dim_branch_key)

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter.dim_branch_key.isNotNull())
df_filter_new = df_filter.filter(df_filter.dim_branch_key.isNull()).select(df_filter.Branch_ID, df_filter.BranchName)

# COMMAND ----------

if (Incremental_Flag == '0'):
    max_value = 1
else:
    max_value_df = spark.sql("select max(dim_branch_key) from cars_catalog.gold.dim_branch")
    max_value = max_value_df.collect()[0][0]+1

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_branch_key', max_value+monotonically_increasing_id())

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('cars_catalog.gold.dim_branch'):
    delta_table = DeltaTable.forPath(spark, 'abfss://gold@carpradipdatalake.dfs.core.windows.net/dim_branch')

    delta_table.alias('trg').merge(df_final.alias('src'), "trg.Branch_ID = src.Branch_ID")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
else:    
    df_final.write.format('delta')\
            .mode('overwrite')\
            .option('path','abfss://gold@carpradipdatalake.dfs.core.windows.net/dim_branch')\
            .saveAsTable('cars_catalog.gold.dim_branch')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.dim_branch

# COMMAND ----------

