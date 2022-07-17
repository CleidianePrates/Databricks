-- Databricks notebook source
-- DBTITLE 1,Auxiliar functions
-- MAGIC %python
-- MAGIC from pyspark.sql.types import *
-- MAGIC from pyspark.sql.functions import *
-- MAGIC from pyspark.sql.window import *
-- MAGIC import numpy as np
-- MAGIC import pandas as pd
-- MAGIC import seaborn as sns
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import plotly as py

-- COMMAND ----------

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

-- COMMAND ----------

-- DBTITLE 1,Exibe os dados executado o Pyspark
-- MAGIC %python
-- MAGIC display(spark.read.table('dados_cliente'))

-- COMMAND ----------

-- DBTITLE 1,Idade média dos consumidores
-- MAGIC %python
-- MAGIC Middle_Ages = display(spark.read.table('dados_cliente').select(col("Customer_Age")) \
-- MAGIC                       .agg(avg(col('Customer_Age')).alias('middle_ages')))

-- COMMAND ----------

-- DBTITLE 1,Quantidade de clientes por gênero
-- MAGIC %python
-- MAGIC Gender_qty = display(spark.read.table('dados_cliente').select(col("Gender"), col('CLIENTNUM')) \
-- MAGIC                       .groupBy(col('Gender'))\
-- MAGIC                       .agg(count(col('CLIENTNUM')).alias('Gender_Qty')))

-- COMMAND ----------

-- DBTITLE 1,Quantidade de clientes por categoria
-- MAGIC %python
-- MAGIC Card_Category = display(spark.read.table('dados_cliente').select(col("Card_Category"), col('CLIENTNUM')) \
-- MAGIC                       .groupBy(col('Card_Category'))\
-- MAGIC                       .agg(count(col('CLIENTNUM')).alias('Category_Qty')))

-- COMMAND ----------

-- DBTITLE 1,Quantidade de clientes por status civil
-- MAGIC %python
-- MAGIC Marital_Status = display(spark.read.table('dados_cliente').select(col("Marital_Status"), col('CLIENTNUM')) \
-- MAGIC                       .groupBy(col('Marital_Status'))\
-- MAGIC                       .agg(count(col('CLIENTNUM')).alias('Status_Qty')))

-- COMMAND ----------

-- DBTITLE 1,Limite de créditos por staus civil
-- MAGIC %python
-- MAGIC 
-- MAGIC Credit_Limit = display(spark.read.table('dados_cliente').select(col("Marital_Status"), col('Credit_Limit')) \
-- MAGIC                       .groupBy(col('Marital_Status'))\
-- MAGIC                       .agg(avg(col('Credit_Limit')).alias('Avg_Credit_Limit')) \
-- MAGIC                        .orderBy(col('Avg_Credit_Limit').desc()))

-- COMMAND ----------

-- DBTITLE 1,Quantidade de clientes por nível educacional
-- MAGIC %python
-- MAGIC Credit_Limit = display(spark.read.table('dados_cliente').select(col("Education_Level"), col('CLIENTNUM')) \
-- MAGIC                       .groupBy(col('Education_Level'))\
-- MAGIC                       .agg(count(col('CLIENTNUM')).alias('Qty_by_Level')) \
-- MAGIC                        .orderBy(col('Qty_by_Level').desc()))
