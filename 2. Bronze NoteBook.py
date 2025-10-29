# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ## INCREMENTAL DATA INJECTION

# COMMAND ----------

src_value = []

for i in dbutils.fs.ls('/Volumes/workspace/raw/rawvolume/rawdata/'):
    src_value.append(i.name[:-1])

src_value

# COMMAND ----------

for src in src_value:
    df = spark.readStream.format('cloudFiles')\
          .option('cloudFiles.format', 'csv')\
          .option('cloudFiles.schemaLocation', f'/Volumes/workspace/bronze/bronze_vol/{src}/checkpoints')\
          .option('cloudFiles.schemaEvolutionMode', 'rescue')\
          .load(f'/Volumes/workspace/raw/rawvolume/rawdata/{src}/')

    df.writeStream.format('delta')\
        .outputMode('append')\
        .trigger(once=True)\
        .option('checkPointLocation', f'/Volumes/workspace/bronze/bronze_vol/{src}/checkpoints/')\
        .option('path', f'/Volumes/workspace/bronze/bronze_vol/{src}/data')\
        .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DELTA.`/Volumes/workspace/bronze/bronze_vol/customer/data/`