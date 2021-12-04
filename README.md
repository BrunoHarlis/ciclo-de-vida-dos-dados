# Ciclo de Vida dos Dados

## Descrição
Esse projeto tem o intúito de mostrar as estapas de extração, transformação e carregamento dos dados usando Spark e Hive. Tomamos como exemplo uma fábrica de automóveis que está novos motores em seus carros. Para isso, será ralizada a extração de dados armazenados em data warehouse diferentes e realizar junções, enriquecendo uma tabela com informações que podem auxiliar a equipe de negócio na melhor tomada de decisão.

Faremos isso através de dois scripts python. Você pode velos aqui (LINK SCRIPTS)

## Iniciando
Nosso primeiro passo é setar as configurações do Spark Session
```
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Ingestao').getOrCreate()
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsinNinemptyLocation", "true")
```

Então vamos carregar os dados que já estão no HDFS e criar DataFrames para cada arquivo .CSV
```
codigo_postal = spark.read.csv("hdfs:///tmp/data/dataenrichment/codigo_postal.csv", header=True, inferSchema=True)
dados_cliente = spark.read.csv("hdfs:///tmp/data/dataenrichment/dados_cliente.csv", header=True, inferSchema=True)
instalacao_carro = spark.read.csv("hdfs:///tmp/data/dataenrichment/instalacao_carro.csv", header=True, inferSchema=True)
motores_experimentais = spark.read.csv("hdfs:///tmp/data/dataenrichment/motores_experimentais.csv", header=True, inferSchema=True)
vendas_carro = spark.read.csv("hdfs:///tmp/data/dataenrichment/vendas_carro.csv", header=True, inferSchema=True)
```
Agora estamos prontos para criar nossa base de dados no HIVE, mas existe a possibilidade de haver algum database ou tabela com o mesmo nome que pretendemos colocar, então valos dropar possíveis databases homônimos. Planejamos criar os databases VENDAS, FABRICA E MARKETING.
```
spark.sql("DROP DATABASE IF EXISTS VENDAS CASCADE")
spark.sql("DROP DATABASE IF EXISTS FABRICA CASCADE")
spark.sql("DROP DATABASE IF EXISTS MARKETING CASCADE")

spark.sql("CREATE DATABASE VENDAS")
spark.sql("CREATE DATABASE FABRICA")
spark.sql("CREATE DATABASE MARKETING")
```

Vamos ver como ficou nossos DataBases

![databases](https://github.com/BrunoHarlis/ciclo-de-vida-dos-dados/blob/main/imagens/databases.png)

Só falta criar as tabelas e preenchê-las com os dados dos DataFrames criados anteriormente. Vamos fazer isso agora.
```
vendas_carro.write.mode("overwrite").saveAsTable("VENDAS.VENDAS_CARRO", format="parquet")
instalacao_carro.write.mode("overwrite").saveAsTable("FABRICA.INSTALACAO_CARRO", format="parquet")
motores_experimentais.write.mode("overwrite").saveAsTable("FABRICA.MOTORES_EXPERIMENTAIS", format="parquet")
dados_cliente.write.mode("overwrite").saveAsTable("MARKETING.DADOS_CLIENTE", format="parquet")
codigo_postal.write.mode("overwrite").saveAsTable("MARKETING.CODIGO_POSTAL", format="parquet")
```

Vamos ver como ficou cada tabela?

1 - Database VENDAS e abela VENDAS_CARRO

![vedas](https://github.com/BrunoHarlis/ciclo-de-vida-dos-dados/blob/main/imagens/tabela%20vendas.png)


2 - DataBase FABRICA e tabelas INSTALACAO_CARRO e MOTORES_EXPERIMENTAIS

![fabrica](https://github.com/BrunoHarlis/ciclo-de-vida-dos-dados/blob/main/imagens/tabelas%20motores_exp%20e%20instalacao_car.png)


3 - DataBase MARKETING e tabelas CODIGO_POSTAL e DADOS_CLIENTE

![marketig](https://github.com/BrunoHarlis/ciclo-de-vida-dos-dados/blob/main/imagens/tabelas%20codigo_posta%20e%20dados_cliente.png)


## Resumo até aqui
Até esse momento foi mostrado como é possível carregar dados, criar databases e tabelas no Hive a partir do pyspark. Na próxima etapa vamos iniciar a parte de transformação dos dados que é a parte final que será usada pelos engenheiros de negócio da empresa.



