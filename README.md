# Ciclo de Vida dos Dados

## Descrição
Esse projeto tem o intúito de mostrar as estapas de extração, transformação e carregamento dos dados usando Spark e Hive. Tomamos como exemplo uma fábrica de automóveis que está novos motores em seus carros. Para isso, será ralizada a extração de dados armazenados em data warehouse diferentes e realizar junções, enriquecendo uma tabela com informações que podem auxiliar a equipe de negócio na melhor tomada de decisão.

Faremos isso através de dois scripts python. Você pode velos aqui (LINK SCRIPTS).

## Iniciando
Nosso primeiro passo é setar as configurações do Spark Session.
```
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('Ingestao').getOrCreate()
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsinNinemptyLocation", "true")
```

Então vamos carregar os dados que já estão no HDFS e criar DataFrames para cada arquivo .CSV.
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

Vamos ver como ficou nossos DataBases.

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

1 - Database VENDAS e abela VENDAS_CARRO.

![vedas](https://github.com/BrunoHarlis/ciclo-de-vida-dos-dados/blob/main/imagens/tabela%20vendas.png)


2 - DataBase FABRICA e tabelas INSTALACAO_CARRO e MOTORES_EXPERIMENTAIS.

![fabrica](https://github.com/BrunoHarlis/ciclo-de-vida-dos-dados/blob/main/imagens/tabelas%20motores_exp%20e%20instalacao_car.png)


3 - DataBase MARKETING e tabelas CODIGO_POSTAL e DADOS_CLIENTE.

![marketig](https://github.com/BrunoHarlis/ciclo-de-vida-dos-dados/blob/main/imagens/tabelas%20codigo_posta%20e%20dados_cliente.png)


## Resumo até aqui
Até esse momento foi mostrado como é possível carregar dados, criar databases e tabelas no Hive a partir do pyspark. Na próxima etapa vamos iniciar a parte de transformação dos dados que é a parte final que será usada pelos engenheiros de negócio da empresa.

## junções
### Ininiando

Vamos ler as tabelas criadas anteriormente que estão no HIVE.
```
vendas_carro = spark.sql("SELECT * FROM vendas.vendas_carro")
instalacao_carro = spark.sql("SELECT * FROM fabrica.instalacao_carro")
motores_experimentais = spark.sql("SELECT * FROM fabrica.motores_experimentais")
codigo_postal = spark.sql("SELECT codigo_postal, latitude, longitude FROM FROM marketing.codigo_postal")
dados_cliente = spark.sql("SELECT * FROM marketing.dados_cliente")
```

E vamos plicar alguns 2 filtros de idade. O primeiro será para remover da tabela "dados_cliente" motoristas que sejam menores de idade (mennos de 18 anos), e o segundo será para remover motoristas que tenham mais de 90 anos. Com a plicação desse filtro, nossa tabela original que tinha 10 mil linhas, agora terá somente 6230 linhas.
```
dados_cliente = dados_cliente.filter(col("aniversario") <= F.add_months(F.current_date(), -216))
dados_cliente = dados_cliente.filter(col("aniversario") >= F.add_months(F.current_date(), -1080))
```

Com o filtro aplicado, vamos efetivamente fazer as junções. Comecemos com as tabelas "vedas_carro" e "dados_cliente" que tem a coluna "id_cliente" em comum. Armazenaremos todas as junções em um DataFrame chamado "tabela_temp".
```
tabela_temp = spark.sql("SELECT cliente.*, vendas.data_venda, vendas.preco_venda, vendas.modelo, vendas.VIN \
                         FROM vendas.vendas_carro vendas JOIN marketing.dados_cliente cliente\
                         ON vendas.id_cliente = cliente.id_cliente")
```
Aqui está uma amostra de como ficou essa junção.

![juncao1](https://github.com/BrunoHarlis/ciclo-de-vida-dos-dados/blob/main/imagens/juncao1.png)

A segunda junção será acrescentar a tabela "codigo_postal" que possue a coluna "codigo_postal" em comum. Ela servirá para mostrar a localização onde se encontra o altomóvel. 
```
tabela_temp = tabela_temp.join(codigo_postal, "codigo_postal")
```

![juncao2](https://github.com/BrunoHarlis/ciclo-de-vida-dos-dados/blob/main/imagens/juncao2.png)


A terceira junção será com a tabela "instalacao_carro" que informa qual peça entrou em qual carro. Nela temos duas colunas em comum, "VIN" e "modelo".
```
tabela_temp = tabela_temp.join(instalacao_carro, ["VIN", "modelo"])
```

![juncao3](https://github.com/BrunoHarlis/ciclo-de-vida-dos-dados/blob/main/imagens/juncao3.png)

A ultima junção será com a tabela "motores_experimentais". Ela adiciona informações sobre as peças, em que fábrica foi feita, em que máquina e em que momento. A coluna que será usada para fazer a junção pe a "num_serie".
```
tabela_temp = tabela_temp.join(motores_experimentais, "num_serie")
```

![juncao4](https://github.com/BrunoHarlis/ciclo-de-vida-dos-dados/blob/main/imagens/juncao4.png)

## Conclusão
Com isso temos nossa nova tabela criada com todas as informações que são importantes para o auxílio na tomada de decisão da equipe de negócios da empresa. Salvaremos essa tabela no HIVE para consultas posteriores caso necessário pela equipe. 
```
tabela_temp.write.mode("overwrite").saveAsTable("FABRICA.MOTORES_EXPERIMENTAIS_INFO", format="parquet")
```
