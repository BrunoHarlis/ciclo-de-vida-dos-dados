from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('Ingest').getOrCreate()
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")


#-----------------------------------------------------
# CRIANDO RDD'S A PARTIR DE ARQUIVOS NO HDFS
#-----------------------------------------------------
car_installs = spark.read.csv("hdfs:///tmp/data/dataenrichment/car_installs/car_installs.csv", header=True, inferSchema=True)
car_sales = spark.read.csv("hdfs:///tmp/data/dataenrichment/car_sales/car_sales.csv", header=True, inferSchema=True)
customer_data = spark.read.csv("hdfs:///tmp/data/dataenrichment/customer_data/customer_data.csv", header=True, inferSchema=True)
factory_data = spark.read.csv("hdfs:///tmp/data/dataenrichment/experimental_motors/experimental_motors.csv",header=True, inferSchema=True)
postal_codes = spark.read.csv("hdfs:///tmp/data/dataenrichment/postal_codes/postal_codes.csv", header=True, inferSchema=True)


#-----------------------------------------------------
# CRIANDO OS DATABASES
#-----------------------------------------------------
spark.sql("CREATE DATABASE SALES")
spark.sql("CREATE DATABASE FACTORY")
spark.sql("CREATE DATABASE MARKETING")

print("DATABASES CRIADOS")
spark.sql("SHOW DATABASES").show()


#-----------------------------------------------------
# CRIANDO AS TABELAS EM SEUS RESPECTIVOS DATABASES
#-----------------------------------------------------
car_installs.write.mode("overwrite").saveAsTable('FACTORY.CAR_INSTALLS', format="parquet")
car_sales.write.mode("overwrite").saveAsTable('SALES.CAR_SALES', format="parquet")
customer_data.write.mode("overwrite").saveAsTable('MARKETING.CUSTOMER_DATA', format="parquet")
factory_data.write.mode("overwrite").saveAsTable('FACTORY.EXPERIMENTAL_MOTORS', format="parquet")
postal_codes.write.mode("overwrite").saveAsTable('MARKETING.POSTAL_CODES', format="parquet")

