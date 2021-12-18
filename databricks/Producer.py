# Databricks notebook source
!pip install kafka-python

# COMMAND ----------

import random 

# COMMAND ----------

price_1 = random.gauss(500, 5)
price_2 = random.gauss(200, 10)

# COMMAND ----------

from time import sleep
from json import dumps, loads, load, dump
from kafka import KafkaProducer
import urllib.request
import random 

def data_generator():
    url = "https://random-data-api.com/api/subscription/random_subscription"
    f = urllib.request.urlopen(url)
    content = f.read()
    return content.decode('utf-8')

def send_data():
  producer = KafkaProducer(bootstrap_servers=['52.202.57.255:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
  
  while True:
    sleep(1)
    data = data_generator()
    data = loads(data)
    data["price"] = random.gauss(500, 5)
    data["cost"] = random.gauss(200, 10)
    producer.send('sample5', value=data)
  

# COMMAND ----------

data = data_generator()
data = loads(data)
data["price"] = random.gauss(500, 5)
data["cost"] = random.gauss(200, 10)

# COMMAND ----------

data

# COMMAND ----------

dumps(data).encode('utf-8')

# COMMAND ----------

dumps(loads(data))

# COMMAND ----------

send_data()

# COMMAND ----------

from time import sleep
from json import dumps, loads
from kafka import KafkaProducer
import urllib.request

def data_generator():
    url = "https://random-data-api.com/api/subscription/random_subscription"
    f = urllib.request.urlopen(url)
    content = f.read()
    return content.decode('utf-8')

def send_data():
  producer = KafkaProducer(bootstrap_servers=['23.21.44.206:9092'],
                         value_serializer=lambda x: 
                         dumps(x).encode('utf-8'))
  
  while True:
    sleep(1)
    data = data_generator()
    producer.send('test', value=loads(data))

# COMMAND ----------

send_data()

# COMMAND ----------


