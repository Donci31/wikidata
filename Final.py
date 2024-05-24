# Databricks notebook source
storage_account = 'wikidatasubset'

spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    dbutils.secrets.get(scope="databricks-secret-keys", key="blob")
)

# COMMAND ----------

dbutils.widgets.text("initial_element","")
initial_element = dbutils.widgets.get("initial_element")
dbutils.widgets.text("depth","")
depth = int(dbutils.widgets.get("depth"))
dbutils.widgets.text("file_name","")
file_name = dbutils.widgets.get("file_name")

# COMMAND ----------

from graphframes import GraphFrame

container = 'init'

labels = spark.read.load(f'abfss://{container}@{storage_account}.dfs.core.windows.net/preprocessed/labels').where("type == 'item' and language == 'en'")
descriptions = spark.read.load(f'abfss://{container}@{storage_account}.dfs.core.windows.net/preprocessed/descriptions').where("type == 'item' and language == 'en'")

vertices = labels.join(descriptions, on='id', how='outer').select('id', 'label', 'description')
edges = spark.read.load(f'abfss://{container}@{storage_account}.dfs.core.windows.net/preprocessed/claims')

graph = GraphFrame(vertices, edges)

# COMMAND ----------

query = ';'.join(f"(a{i})-[e{i}]->(a{i + 1})" for i in range(depth))

result_graph = graph.find(query).filter(f"a{depth}.id = '{initial_element}'")

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("id_source", StringType(), True),
    StructField("label", StringType(), True),
    StructField("description", StringType(), True),
    StructField("property", StringType(), True),
    StructField("id_dest", StringType(), True)
])

final_df = spark.createDataFrame([], schema)

for i in range(depth):
    temp_df = result_graph.select(
        col(f"a{i}.id").alias("id_source"), 
        col(f"a{i}.label").alias("label"), 
        col(f"a{i}.description").alias("description"), 
        col(f"e{i}.property").alias("property"), 
        col(f"a{i + 1}.id").alias("id_dest")
    ).distinct()

    final_df = final_df.union(temp_df)

# COMMAND ----------

final_df.write.format('csv').save(f'abfss://subset@{storage_account}.dfs.core.windows.net/{file_name}', header=True)
