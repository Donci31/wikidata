# Databricks notebook source
storage_account = 'wikidatasubset'

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="databricks-secret-keys", key="blob")
)

# COMMAND ----------

from pyspark.sql.types import MapType, ArrayType, StructType, StructField, StringType

alias_struct = StructType([
    StructField("language", StringType(), True),
    StructField("value", StringType(), True)
])

aliases_map = MapType(StringType(), ArrayType(alias_struct))

aliases_schema = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("aliases", aliases_map, True)
])

# COMMAND ----------

container = 'init'
path = 'wikidatawiki/entities/latest-all.json'

df_aliases = spark.read.json(f'abfss://{container}@{storage_account}.dfs.core.windows.net/{path}', schema=aliases_schema)

# COMMAND ----------

from pyspark.sql.functions import explode, col

df_aliases_final = (
    df_aliases.select("id", "type", explode("aliases").alias("language", "array"))
    .select("id", "type", "language", explode("array.value").alias("alias"))
)

# COMMAND ----------

df_aliases_final.write.partitionBy("type").format('delta').save(f'abfss://{container}@{storage_account}.dfs.core.windows.net/preprocessed/aliases')

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`abfss://init@wikidatasubset.dfs.core.windows.net/preprocessed/aliases` ZORDER BY id;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM delta.`abfss://init@wikidatasubset.dfs.core.windows.net/preprocessed/aliases`;
