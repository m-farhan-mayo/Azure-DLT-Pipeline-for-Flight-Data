# Databricks notebook source
# MAGIC %md
# MAGIC ## **FETCHING PARAMETERS AND CREATING VARIABLES**

# COMMAND ----------

# #CATALOG NAME
# catalog_name = 'workspace'
# # KEY COLUMN
# key_col = "['flight_id']"
# key_col_list = eval(key_col)

# #CDC COLUMN
# cdc_col = 'modify_date'

# #BACK-DATED REFRESH
# backdated_refresh = ""

# #SOURCE OBJECT
# source_object = 'silver_flight'

# #SOURCE SCHEMA
# source_schema = 'silver'

# #TARGET SCHEMA
# target_schema = 'gold'

# #TARGET TABLE
# target_object = 'flight'

# #Surrogate Key
# surrogate_key = 'DimFlightKey'

# COMMAND ----------

# #CATALOG NAME
# catalog_name = 'workspace'
# # KEY COLUMN
# key_col = "['airport_id']"
# key_col_list = eval(key_col)

# #CDC COLUMN
# cdc_col = 'modify_date'

# #BACK-DATED REFRESH
# backdated_refresh = ""

# #SOURCE OBJECT
# source_object = 'silver_airport'

# #SOURCE SCHEMA
# source_schema = 'silver'

# #TARGET SCHEMA
# target_schema = 'gold'

# #TARGET TABLE
# target_object = 'airport'

# #Surrogate Key
# surrogate_key = 'DimAirportKey'

# COMMAND ----------

#CATALOG NAME
catalog_name = 'workspace'
# KEY COLUMN
key_col = "['passenger_id']"
key_col_list = eval(key_col)

#CDC COLUMN
cdc_col = 'modify_date'

#BACK-DATED REFRESH
backdated_refresh = ""

#SOURCE OBJECT
source_object = 'silver_passangers'

#SOURCE SCHEMA
source_schema = 'silver'

#TARGET SCHEMA
target_schema = 'gold'

#TARGET TABLE
target_object = 'passenger'

#Surrogate Key
surrogate_key = 'DimPassengerKey'

# COMMAND ----------

# MAGIC %md
# MAGIC # INCREMENTAL DATA INJECTION

# COMMAND ----------

# MAGIC %md
# MAGIC ### LAST LOAD DATE

# COMMAND ----------

if len(backdated_refresh) == 0:

    if spark.catalog.tableExists(f'{catalog_name}.{target_schema}.{target_object}'):
        last_load = spark.sql(f'SELECT MAX({cdc_col}) FROM {catalog_name}.{target_schema}.{target_object}').collect()[0][0]

    else:
        last_load = '1900-01-01 00:00:00'

else:
    last_load = backdated_refresh




# COMMAND ----------

df_src = spark.sql(f"SELECT * FROM {source_schema}.{source_object} WHERE {cdc_col} > '{last_load}'")

# COMMAND ----------

df_src.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### OLD VS NEW RECORDS

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog_name}.{target_schema}.{target_object}"):
  #KEY COLUMN STRING INCREMENTAL
  key_col_string = ', '.join(key_col_list)

  df_trg = spark.sql(f"""SELECT {key_col_string}, {surrogate_key}, create_date, update_date FROM {catalog_name}.{target_schema}.{target_object}""")
else:
  #KEY COLUMN FOR INITIAL
  key_col_string_init = [f"'' AS {i} " for i in key_col_list]
  key_col_string_init = ', '.join(key_col_string_init)
  
  df_trg = spark.sql(f"SELECT {key_col_string_init}, CAST(0 AS INT) AS {surrogate_key}, CAST('1900-01-01 00:00:00' AS TIMESTAMP) AS create_date, CAST('1900-01-01 00:00:00' AS TIMESTAMP) AS update_date")

display(df_trg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## JOIN CONDITION

# COMMAND ----------

join_condition = 'And'.join([f"src.{i} = trg.{i}" for i in key_col_list])

# COMMAND ----------

df_src.createOrReplaceTempView("src")
df_trg.createOrReplaceTempView("trg")

df_join = spark.sql(f"""
          SELECT src.*,
          trg.{surrogate_key},
          trg.create_date,
          trg.update_date
          FROM src
          LEFT JOIN trg
          ON {join_condition}         
          

          """)

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

#Old Record
df_old = df_join.filter(col(f"{surrogate_key}").isNotNull())

#New Record
df_new = df_join.filter(col(f"{surrogate_key}").isNull())


# COMMAND ----------

# MAGIC %md
# MAGIC **PREPARING DF `OLD`**

# COMMAND ----------

df_old_enr = df_old.withColumn('update_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC **PREPARING DF `NEW`**

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog_name}.{target_schema}.{target_object}"):
  max_surrogate_key = spark.sql(f"SELECT MAX({surrogate_key}) FROM {catalog_name}.{target_schema}.{target_object}").collect()[0][0]

  df_new_enr = df_new.withColumn(f"{surrogate_key}", lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                .withColumn('create_date', current_timestamp())\
                .withColumn('update_date', current_timestamp())
else:
  max_surrogate_key = 0
  df_new_enr = df_new.withColumn(f"{surrogate_key}", lit(max_surrogate_key)+lit(1)+monotonically_increasing_id())\
                .withColumn('create_date', current_timestamp())\
                .withColumn('update_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ### `UNION OLD AND NEW`

# COMMAND ----------

df_union = df_old_enr.unionByName(df_new_enr)

# COMMAND ----------

# MAGIC %md
# MAGIC # `UPSERT`

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists(f"{catalog_name}.{target_schema}.{target_object}"):
  dlt_obj = DeltaTable.forName(spark, f'{catalog_name}.{target_schema}.{target_object}')
  dlt_obj.alias('trg').merge(df_union.alias('src'),f"trg.{surrogate_key} = src.{surrogate_key}")\
                      .whenMatchedUpdateAll(condition= f"trg.{cdc_col} >= src.{cdc_col}")\
                      .whenNotMatchedInsertAll()\
                      .execute()


else:
  df_union.write.format('delta')\
    .mode('append')\
    .saveAsTable(f"{catalog_name}.{target_schema}.{target_object}")

# COMMAND ----------

