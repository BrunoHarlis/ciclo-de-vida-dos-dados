from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F


spark = SparkSession.builder.appName('Ingest').getOrCreate()
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")


#---------------------------------------------------
#                READ SOURCE TABLES
#---------------------------------------------------
vendas_carro = spark.sql("SELECT * FROM vendas.vendas_carro")
instalacao_carro = spark.sql("SELECT * FROM fabrica.instalacao_carro")
motores_experimentais = spark.sql("SELECT * FROM fabrica.motores_experimentais")
codigo_postal = spark.sql("SELECT codigo_postal, latitude, longitude FROM FROM marketing.codigo_postal")
dados_cliente = spark.sql("SELECT * FROM marketing.dados_cliente")


#---------------------------------------------------
# APLICAR FILTRO
# REMOVER MOTORISTAS MENORES DE 18 ANOS E MAIORES DE 90 ANOS
#---------------------------------------------------
antes = dados_cliente.count()
dados_cliente = dados_cliente.filter(col("aniversario") <= F.add_months(F.current_date(), -216))
dados_cliente = dados_cliente.filter(col("aniversario") >= F.add_months(F.current_date(), -1080))

# JOIN
#---------------------------------------------------
tabela_temp = spark.sql("SELECT cliente.*, vendas.data_venda, vendas.preco_venda, vendas.modelo, vendas.VIN \
                            FROM vendas.vendas_carro vendas JOIN marketing.dados_cliente cliente\
                             ON vendas.id_cliente = cliente.id_cliente")

tabela_temp.show(5)


# Adicionando geolocalização baseado em "codigo_postal"
tabela_temp = tabela_temp.join(codigo_postal, "codigo_postal")
tabela_temp.show(5)

# Adicionando instalações (Qual parte entrou em qual carro?)
tabela_temp = tabela_temp.join(instalacao_carro, ["VIN", "modelo"])
tabela_temp.show(5)

# Adicionando informações de fábrica (para cada peça, em que fábrica ela foi feita, em que máquina e em que momento)
tabela_temp = tabela_temp.join(motores_experimentais, "num_serie")
tabela_temp.show(5)



# Criando uma nova tabela no HIVE 
#---------------------------------------------------
tabela_temp.write.mode("overwrite").saveAsTable("FABRICA.MOTORES_EXPERIMENTAIS_INFO", format="parquet")


