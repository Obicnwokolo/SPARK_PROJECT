from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("Initial load").enableHiveSupport().getOrCreate()

df = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.132.73.146:5432/testdb").option("dbtable", "obi_shopping_trend").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "WelcomeItc@2022").load()
df.printSchema()

# df.write.mode("overwrite").saveAsTable("bigdata_nov_2024.obi_shopping_trend")
# print("Successfully Load to Hive")