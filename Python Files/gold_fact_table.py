# Databricks notebook source
df_src = spark.sql("select * from parquet.`abfss://silver@carpradipdatalake.dfs.core.windows.net/carsales`")

# COMMAND ----------

df_src.display()

# COMMAND ----------

df_branch = spark.sql("select * from cars_catalog.gold.dim_branch")

df_date = spark.sql("select * from cars_catalog.gold.dim_date")

df_dealer = spark.sql("select * from cars_catalog.gold.dim_dealer")

df_model = spark.sql("select * from cars_catalog.gold.dim_model")

# COMMAND ----------

df_fact = df_src.join(df_branch, df_src.Branch_ID == df_branch.Branch_ID, how='left')\
                .join(df_date, df_src.Date_ID == df_date.Date_ID, how='left')\
                .join(df_dealer, df_src.Dealer_ID == df_dealer.Dealer_ID, how='left')\
                .join(df_model, df_src.Model_ID == df_model.Model_ID, how='left')\
                .select(df_src.Revenue, df_src.Units_Sold, df_src.Revenue_per_unit, df_branch.dim_branch_key, df_date.dim_date_key, df_dealer.dim_dealer_key, df_model.dim_model_key)

# COMMAND ----------

df_fact.display()

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("cars_catalog.gold.factsales"):
    deltatable = DeltaTable.forPath(spark,"abfss://gold@carpradipdatalake.dfs.core.windows.net/factsales")

    deltatable.alias("trg").merge(df_fact.alias("src"), "trg.dim_branch_key = src.dim_branch_key AND trg.dim_date_key = src.dim_date_key AND trg.dim_dealer_key = src.dim_dealer_key AND trg.dim_model_key = src.dim_model_key")\
    .whenMatchedUpdateAll()\
    .whenNotMatchedInsertAll()\
    .execute()

else:
    df_fact.write.format('delta')\
            .mode('overwrite')\
            .option('path','abfss://gold@carpradipdatalake.dfs.core.windows.net/factsales')\
            .saveAsTable('cars_catalog.gold.factsales')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cars_catalog.gold.factsales

# COMMAND ----------

