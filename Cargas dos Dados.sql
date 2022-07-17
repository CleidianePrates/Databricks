-- Databricks notebook source
-- DBTITLE 1,Carga dos Dados csv via PySpark
-- MAGIC %python
-- MAGIC clientes = spark.read.format('csv').options(header='true', inferSchema='true', delimiter=';').load('/FileStore/tables/carga/clientes_cartao.csv')
-- MAGIC 
-- MAGIC display(clientes)

-- COMMAND ----------

-- DBTITLE 1,Carga dos Dados csv via Scala
-- MAGIC %scala 
-- MAGIC  val cliente = spark.read.format("csv")
-- MAGIC  .option("header", "true")
-- MAGIC  .option("inferSchema", "true")
-- MAGIC  .option("delimiter", ";")
-- MAGIC  .load("/FileStore/tables/carga/clientes_cartao.csv")
-- MAGIC display(cliente)

-- COMMAND ----------

-- DBTITLE 1,Cria visão de Scala para SQL
-- MAGIC %scala
-- MAGIC /**Cria uma visão temporária*/
-- MAGIC cliente.createOrReplaceTempView("dados_cliente")

-- COMMAND ----------

-- DBTITLE 1,Exibe os dados executado um comando SELECT em SQL
-- MAGIC %sql
-- MAGIC select * from dados_cliente
