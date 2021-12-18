from time import sleep
from json import dumps, load, loads
from kafka import KafkaProducer
import urllib.request
import random

TOPICO = "kafka-topic"
SERVER = "localhost:9092"
INTERVALO = 1

def data_generator():
    url = "https://random-data-api.com/api/subscription/random_subscription"
    f = urllib.request.urlopen(url)
    content = f.read()
    return content.decode('utf-8')
def send_data(topico, server, intervalo):
  producer = KafkaProducer(bootstrap_servers=[server],
                         value_serializer=lambda x:
                         dumps(x).encode('utf-8'))
  while True:
    sleep(intervalo)
    data = data_generator()
    data = loads(data)
    data["price"] = random.gauss(500, 5)
    data["cost"] = random.gauss(200, 10)
    producer.send(topico, value=data)

send_data(TOPICO,SERVER, INTERVALO)