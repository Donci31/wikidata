# Databricks notebook source
from graphframes import GraphFrame

# COMMAND ----------

storage_account = 'wikidatasubset'

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="databricks-secret-keys", key="blob")
)

# COMMAND ----------

container = 'init'

labels = spark.read.load(f'abfss://{container}@{storage_account}.dfs.core.windows.net/preprocessed/labels').where("type == 'item' and language == 'en'")
descriptions = spark.read.load(f'abfss://{container}@{storage_account}.dfs.core.windows.net/preprocessed/descriptions').where("type == 'item' and language == 'en'")

vertices = labels.join(descriptions, on='id', how='outer').select('id', 'label', 'description')
edges = spark.read.load(f'abfss://{container}@{storage_account}.dfs.core.windows.net/preprocessed/claims')

# COMMAND ----------

graph = GraphFrame(vertices, edges)

# COMMAND ----------

animal_graph = graph.find("(a)-[e]->(b); (b)-[e2]->(c)")\
  .filter("c.id = 'Q35120'")

# COMMAND ----------

from pyspark.sql.functions import col

final_df = animal_graph \
    .select(col("a.id").alias("id_source"), "a.label", "a.description", "e.property", col("b.id").alias("id_dest")) \
    .union(animal_graph.select(col("b.id").alias("id_source"), "b.label", "b.description", "e2.property", col("c.id").alias("id_dest")).distinct())

# COMMAND ----------

final_df.write.format('csv').save(f'abfss://{container}@{storage_account}.dfs.core.windows.net/output/entities.csv', header=True)
