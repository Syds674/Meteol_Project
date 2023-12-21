from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("yarn") \
    .appName("Ingestao - dados") \
    .enableHiveSupport() \
    .getOrCreate()

print("CRIANDO DATABASE!")

spark.sql(f"""
CREATE DATABASE IF NOT EXISTS meterio
""")

print("DATABASE CRIADO!")