# Databricks notebook source
df_ingested = spark.read.format("delta").load("/mnt/sample_ingestion5/")

# COMMAND ----------

display(df_ingested)

# COMMAND ----------

(dbutils.fs.ls("/mnt/sample_ingestion5/"))

# COMMAND ----------

# MAGIC %fs
# MAGIC 
# MAGIC ls "mnt/sample_ingestion5/"

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/sample_ingestion5/",True)
