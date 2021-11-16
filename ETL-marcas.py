# pyright: reportMissingImports=false

import findspark
findspark.init()

from pyspark import HiveContext

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("brands") \
    .enableHiveSupport() \
    .getOrCreate()

hiveContext = HiveContext(spark.sparkContext)

# Consulta 1
# Retornar os produtos mais avaliados
# consulta1 = hiveContext.sql("SELECT * FROM products ORDER BY total_reviews DESC")
# consulta1.show()

# # Consulta 2
# # Retornar os produtos com melhor média de avaliação a partir de um número mínimo de avaliações
# min_reviews = 3
# consulta2 = hiveContext.sql("SELECT * FROM products WHERE total_reviews >= {} ORDER BY star_average DESC".format(min_reviews))
# consulta2.show()

# # Consulta 3
# # Retornar os consumidores que mais avaliaram produtos
# consulta3 = hiveContext.sql("SELECT customer_id, count(customer_id) as count_reviews FROM user_reviews GROUP BY customer_id ORDER BY count(customer_id) DESC")
# consulta3.show()

# Consulta 4
# Retornar média de estrelas das marcas presentes no sistema
marcas = ["apple", "microsoft", "dell", "nvidia", "sony", "polaroid", "hp", "tp-link", "lenovo", "linksys", "nintendo", "philips", "canon", "panasonic", "kingston", "hitachi", "fermax", "asus", "xiaomi", "nokia", "logitech", "samsung", "motorola", "amazfit", "altera", "cisco", "lg", "evga", "microsoft", "philco", "western", "panasonic", "seagate", "alienware", "thermaltake", "sandisk", "ibm", "compaq", "acer", "toshiba", "belkin", "siemens", "crucial", "fujitsu", "d-link", "rayovac", "steelseries", "duracell", "benq", "hitachi", "asrock", "zotac", "gigabyte", "silverstone", "blackberry", "fujifilm", "logisys", "kodak", "casio", "yamaha"]

consulta4 = hiveContext.sql("SELECT product_title FROM products")

consulta4.show()

df = consulta4.select("*").toPandas()

marcas_produtos = []

for index, row in df.iterrows():
  found = 0
  for marca in marcas:
    nome_produto = row["product_title"]
    nome_produto = nome_produto.lower()
    if marca in nome_produto:
      found = 1
      marcas_produtos.append(marca)
      break
  if found == 0:
    marcas_produtos.append("unknown")

df["brand"] = marcas_produtos
df.to_csv('marcas_produtos.csv', sep= '\t')

marcas = spark.read.csv("./marcas_produtos.csv", sep= '\t', header=True)
marcas.createOrReplaceTempView("marcas")

products = hiveContext.sql("SELECT * FROM products")
marcas = spark.sql("SELECT brand FROM marcas")

from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id, row_number
w = Window.orderBy(monotonically_increasing_id())

products = products.withColumn("columnindex", row_number().over(w))
marcas = marcas.withColumn("columnindex", row_number().over(w))

productTable = products.join(marcas, "columnindex").drop(products.columnindex)
productTable.write.mode("overwrite").saveAsTable("default.products_with_brands")

brands = hiveContext.sql("SELECT brand, avg(star_average) FROM products_with_brands WHERE brand <> 'unknown' GROUP BY brand ORDER BY avg(star_average) DESC")

brands.show()