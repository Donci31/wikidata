# Databricks notebook source
storage_account = 'wikidatasubset'

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="databricks-secret-keys", key="blob")
)

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA parquet.`abfss://init@wikidatasubset.dfs.core.windows.net/preprocessed/claims`;

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`abfss://init@wikidatasubset.dfs.core.windows.net/preprocessed/claims` ZORDER BY property;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM delta.`abfss://init@wikidatasubset.dfs.core.windows.net/preprocessed/claims`
