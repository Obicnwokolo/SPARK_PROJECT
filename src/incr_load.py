from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local").appName("incremental load").enableHiveSupport().getOrCreate()

max_cust_id = spark.sql("SELECT MAX(customer_id) FROM bigdata_nov_2024.obi_shopping_trend")
max_cust_id = max_cust_id.collect()[0][0]

query = 'SELECT * FROM obi_shopping_trend WHERE customer_id >' + str(max_cust_id)

more_data = spark.read.format("jdbc").option("url", "jdbc:postgresql://18.132.73.146:5432/testdb").option("driver", "org.postgresql.Driver").option("user", "consultants").option("password", "WelcomeItc@2022").option("query", query).load()

record_count = more_data.count()

more_data.write.mode("append").saveAsTable("bigdata_nov_2024.obi_shopping_trend")

print(f"Number of new records: {record_count}")