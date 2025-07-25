# Databricks notebook source
# MAGIC %md
# MAGIC # Create Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog cars_catalog

# COMMAND ----------

# MAGIC %md
# MAGIC # Create Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.silver

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema cars_catalog.gold

# COMMAND ----------

