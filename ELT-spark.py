# pyright: reportMissingImports=false

import findspark
findspark.init()

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("myApp") \
    .getOrCreate()

df = spark.read.csv("./amazon_reviews_us_PC_v1_00.tsv", sep=r'\t', header=True)
df.createOrReplaceTempView("temp")
df.printSchema()

df.select("star_rating","review_body").filter("star_rating >= 4").show(n=5)

count = spark.sql("SELECT count(*) FROM temp")
count.show()