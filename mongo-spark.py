# pyright: reportMissingImports=false

import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1:27017/sales.user_reviews") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1:27017/sales.user_reviews") \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
df.createOrReplaceTempView("temp")

comments = spark.sql("SELECT review_body FROM temp WHERE star_rating = 1")

comments.show()