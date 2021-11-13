# pyright: reportMissingImports=false

import findspark
findspark.init()

from pyspark import HiveContext

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("myApp") \
    .enableHiveSupport() \
    .getOrCreate()

# Carregando todos os dados
df = spark.read.csv("./amazon_reviews_us_PC_v1_00.tsv", sep=r'\t', header=True)
df.createOrReplaceTempView("temp")
df.printSchema()

hiveContext = HiveContext(spark.sparkContext)

# Review table
## customer_id
## review_id
## product_id
## star_rating
reviewsTable = spark.sql("SELECT customer_id, review_id, product_id, star_rating FROM temp")

# Product table
## product_id
## product_title
## star_avg (CRIAR)
## star_median (CRIAR)
## total_reviews (CRIAR)

# Remover linhas duplicadas - (Review com usuário e conteudo igual ou produtos com mesmo id )
# spark.sql("SELECT product_id, COUNT(product_id) as c FROM products GROUP BY product_id HAVING c > 1").show()

# Total count of products: 441965
# spark.sql("SELECT count(*) FROM products").show()

# # duplicate entries: 1
# print(spark.sql("SELECT product_id, COUNT(product_id) as c FROM products GROUP BY product_id HAVING c > 1").count())

# # total count of duplicates: 2
# spark.sql("SELECT sum(c) FROM (SELECT product_id, COUNT(product_id) as c FROM products GROUP BY product_id HAVING c > 1)").show()

# Produto com ID = B0049SIJ7K -> duplicado
# productTable = spark.sql("SELECT product_id, avg(star_rating) FROM temp")
# productTable = productTable.dropDuplicates(["product_id"])
# productTable.createOrReplaceTempView("products")
# 441965 - 1 = 441964
# print(productTable.count())

# Calcular número total de avaliações por produto
# Calcular média de estrelas por produto
# Calcular mediana de estrelas por produto
productInfos = spark.sql("SELECT product_id, percentile_approx(star_rating, 0.5) as star_median, avg(star_rating) as star_average, count(product_id) as total_reviews FROM temp GROUP BY product_id")
productInfos.createOrReplaceTempView("products")

productNames = spark.sql("SELECT DISTINCT product_id, product_title FROM temp")
productNames = productNames.dropDuplicates(["product_id"])
productTable = productNames.join(productInfos, 'product_id')

productTable.createOrReplaceTempView("products")

## REVIEWS DUPLICADAS

# 6908554 número de linhas orignalmente
# spark.sql("SELECT count(*) FROM reviews").show()

#duplicate entries: 1680
# spark.sql("SELECT customer_id, product_id, star_rating, COUNT(customer_id, product_id, star_rating) as c FROM reviews GROUP BY customer_id, product_id, star_rating HAVING c > 1")

# total count of duplicates: 3444
# spark.sql("SELECT sum(c) FROM (SELECT customer_id, product_id, star_rating, COUNT(customer_id, product_id, star_rating) as c FROM reviews GROUP BY customer_id, product_id, star_rating HAVING c > 1)").show()

## customer_id + product_id + star_rating duplicados
# 6908554 - (3444-1680) = 6906790 total
reviewsTable = reviewsTable.dropDuplicates(["customer_id", "product_id", "star_rating"])
# print(reviewsTable.count())
reviewsTable.createOrReplaceTempView("reviews")

reviewsTable.write.mode("overwrite").saveAsTable("default.user_reviews")
productTable.write.mode("overwrite").saveAsTable("default.products")

hiveContext.sql("Show tables;")

