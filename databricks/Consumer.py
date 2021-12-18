# Databricks notebook source
from pyspark.sql import SparkSession, DataFrame as SparkDataFrame
from pyspark.sql.functions import (from_json, col, explode,pandas_udf ,from_unixtime,
                                   regexp_extract, hour, minute, ascii, date_trunc, window, 
                                   concat, lit, upper, trunc, to_date, countDistinct, when, lower, split, avg, min,
                                   substring,expr, current_date, to_utc_timestamp, current_timestamp, date_add,date_sub)
from pyspark.sql.types import *
import pyspark.sql.functions as F
                   

# COMMAND ----------

def load_base_topic(spark:SparkSession, topic_name:str, kafka_brokers:str) -> SparkDataFrame:
  """Se utiliza para definir la conexión inicial del streaming sobre Kafka y cargar la trama base
  
  Args: 
       spark: La sesión de spark
       topic_name(str): Nombre del topico para la conexión
       kafka_brokers(str): Direcciones IP de los Brokers del servicio de Kafka.
  Returns: 
          stream(SparkDataFrame): Devuelve un dataframe de Spark con al trama base del topico
  """

  stream = spark.readStream\
     .format('kafka')\
     .option("kafka.bootstrap.servers", kafka_brokers)\
     .option("subscribe", topic_name)\
     .option("failOnDataLoss", "false")\
     .load()
  return stream

def parse_message(df:SparkDataFrame,Schema:StructType) -> SparkDataFrame:
  """Se parsea la trama base del topico según el esquema provisto.

  Args: 
        df(SparkDataFrame): DataFrame de Spark base del topico.
        Schema(StruckType): Eschema de datos esperados en la trama.
  Returns: 
        df(SparkDataFrame): DataFrame de Spark con los datos de la trama parseados según el esquema definido.
  """
  df = base.selectExpr("timestamp","CAST(value AS STRING) as data")
  df = df.select(explode(split(df.data, '\n')).alias("explode"), df.timestamp)
  df = df.select(from_json(col("explode"), schema).alias('value'), df.timestamp)
  df = df.selectExpr('value.*','timestamp')
  return df

def process(df:SparkDataFrame) -> SparkDataFrame:
  """Definicion de la lógica de procesamiento sobre los datos
  Args: 
    df(SparkDataFrame): DataFrame de Spark del topico parseado
    
  Returns: 
    df(SparkDataFrame): DataFrame resultado de la aplicación de la lógica de negocio
  """
  
  df = df.withWatermark("timestamp", "15 seconds")
  df = df.groupBy(window(df.timestamp, "15 seconds", "15 seconds").alias("timestamp"), df.payment_method)
  df = df.agg(avg("price").alias("avg_price"), min("cost").alias("min_cost"))
  df =  df.select(col("timestamp.start").alias("StartTime"),col("timestamp.end").alias("EndTime"),
                  col("payment_method"),
                  col("avg_price"),
                  col("min_cost"))
  return df

def save_data(df:SparkDataFrame, format:str, output_path:str, query_name:str, mode:str, checkpoint_path:str):
  """
  Funcion principal que define todo el proceso de streaming: carga la trama base, genera la funcion de procesamiento y las rutas de checkpoint. 
  Args: 
      df(SparkDataFrame): DataFrame de Spark
      format(str): Formato de Almacenamiento: delta, parquet, etc
      output_path(str): Ruta de almacenamiento
      query_name(str): Nombre del Query 
      mode(str): Modo de almacenamiento
      checkpoint_path(str): Ruta de Almacenamiento del checkpoint
      
  Returns 
      Streaming 
  """
  
  return df.writeStream\
      .format(format)\
      .option("path", output_path)\
      .queryName(topic)\
      .outputMode(mode)\
      .option("checkpointLocation", checkpoint_path)\
      .trigger(processingTime='30 seconds')

def main(spark, topic, kafka_brokers,schema, output_path, format, mode):
  """
  Funcion principal que define todo el proceso de streaming: carga la trama base, la parsea, ejecuta la logica de negocio y escribe los datos resultado. 
  Args: 
      spark: La sesión de spark
      topic(str): topico de kafka 
      kafka_brokers(str): Direcciones IP de los Brokers del servicio de Kafka.
      Schema(StruckType): Eschema de datos esperados en la trama de entrada.
      output_path(str): Ruta de almacenamiento
      format(str): Formato de Almacenamiento: delta, parquet, etc
      mode(str): Modo de almacenamiento
  Returns 
      Streaming 
  """
  base = load_base_topic(spark,topic,kafka_brokers)
  
  parsed = parse_message(base, schema)
  
  proceced = process(parsed)
  
  checkpoint_path =  "/".join(output_path.split("/")[:-1]) + "/stream_check_" + topic
  
  streaming = save_data(proceced, format, output_path, topic, mode ,checkpoint_path)
  return streaming

# COMMAND ----------

topic = "kafka-topic"
kafka_brokers = "23.21.44.206:9092"
schema = StructType([StructField("id", IntegerType(), True),
                     StructField("uid", StringType(), True),
                     StructField("status", StringType(), True),
                     StructField("payment_method", StringType(), True), 
                     StructField("subscription_term", StringType(), True), 
                     StructField("payment_term", StringType(), True),
                     StructField("price", DoubleType(), True), 
                     StructField("cost", DoubleType(), True)])
output_path = "/mnt/sample_ingestion5/"
mode = "append"
format = "delta"

# COMMAND ----------

base = load_base_topic(spark,topic,kafka_brokers)
parsed =  parse_message(base, schema)
proceced = process(parsed)
streaming = main(spark, topic, kafka_brokers, schema, output_path, format, mode)

# COMMAND ----------

streaming.start()

# COMMAND ----------


