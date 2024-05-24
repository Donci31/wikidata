# Databricks notebook source
storage_account = 'wikidatasubset'

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="databricks-secret-keys", key="blob")
)

# COMMAND ----------

from pyspark.sql.types import MapType, ArrayType, StructType, StructField, StringType

labels_struct = StructType([
    StructField("language", StringType(), True),
    StructField("value", StringType(), True)
])

labels_map = MapType(StringType(), labels_struct)

labels_schema = StructType([
    StructField("id", StringType(), True),
    StructField("type", StringType(), True),
    StructField("descriptions", labels_map, True)
])

# COMMAND ----------

container = 'init'
path = 'wikidatawiki/entities/latest-all.json'

df_label = spark.read.json(f'abfss://{container}@{storage_account}.dfs.core.windows.net/{path}', schema=labels_schema)

# COMMAND ----------

from pyspark.sql.functions import explode, col

df_label_final = (
    df_label.select("id", "type", explode("descriptions").alias("language", "struct"))
    .select("id", "type", "language", col("struct.value").alias("description"))
)

# COMMAND ----------

df_label_final.write.partitionBy("type").format('delta').save(f'abfss://{container}@{storage_account}.dfs.core.windows.net/preprocessed/descriptions')

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE delta.`abfss://init@wikidatasubset.dfs.core.windows.net/preprocessed/descriptions` ZORDER BY id;

# COMMAND ----------

# MAGIC %sql
# MAGIC VACUUM delta.`abfss://init@wikidatasubset.dfs.core.windows.net/preprocessed/descriptions`;
