# -*- coding: utf-8 -*-
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Ingestao').getOrCreate()
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsinNinemptyLocation", "true")


#-----------------------------------------------------
# CRIANDO RDD'S A PARTIR DE ARQUIVOS NO HDFS
#-----------------------------------------------------
codigo_postal = spark.read.csv("hdfs:///tmp/data/dataenrichment/codigo_postal.csv", header=True, inferSchema=True)
dados_cliente = spark.read.csv("hdfs:///tmp/data/dataenrichment/dados_cliente.csv", header=True, inferSchema=True)
instalacao_carro = spark.read.csv("hdfs:///tmp/data/dataenrichment/instalacao_carro.csv", header=True, inferSchema=True)
motores_experimentais = spark.read.csv("hdfs:///tmp/data/dataenrichment/motores_experimentais.csv", header=True, inferSchema=True)
vendas_carro = spark.read.csv("hdfs:///tmp/data/dataenrichment/vendas_carro.csv", header=True, inferSchema=True)


#-----------------------------------------------------
# Limpando Databases, tabelas e views
#-----------------------------------------------------
spark.sql("DROP DATABASE IF EXISTS VENDAS CASCADE")
spark.sql("DROP DATABASE IF EXISTS FABRICA CASCADE")
spark.sql("DROP DATABASE IF EXISTS MARKETING CASCADE")

#-----------------------------------------------------
# CRIANDO OS DATABASES
#-----------------------------------------------------
spark.sql("CREATE DATABASE VENDAS")
spark.sql("CREATE DATABASE FABRICA")
spark.sql("CREATE DATABASE MARKETING")

print("DATABASES CRIADOS")
spark.sql("SHOW DATABASES").show()


#-----------------------------------------------------
# CRIANDO AS TABELAS EM SEUS RESPECTIVOS DATABASES
#-----------------------------------------------------
vendas_carro.write.mode("overwrite").saveAsTable("VENDAS.VENDAS_CARRO", format="parquet")
instalacao_carro.write.mode("overwrite").saveAsTable("FABRICA.INSTALACAO_CARRO", format="parquet")
motores_experimentais.write.mode("overwrite").saveAsTable("FABRICA.MOTORES_EXPERIMENTAIS", format="parquet")
dados_cliente.write.mode("overwrite").saveAsTable("MARKETING.DADOS_CLIENTE", format="parquet")
codigo_postal.write.mode("overwrite").saveAsTable("MARKETING.CODIGO_POSTAL", format="parquet")



