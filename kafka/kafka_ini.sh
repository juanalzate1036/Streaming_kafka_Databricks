#!/bin/sh

#Actualizamos instancia
sudo yum update -y

#Instalamos la version de java requerida para Kafka
sudo yum install -y java-1.8.0-openjdk.x86_64

#Instalamos python
sudo yum install -y python3

#Libreria para kafka
pip3 install kafka-python==2.0.2

#Descargamos Kafka descomprimimos paquete
wget https://dlcdn.apache.org/kafka/3.0.0/kafka_2.13-3.0.0.tgz
tar -xvf kafka_2.13-3.0.0.tgz

#Renombramos directorio kafka
rm -f kafka_2.13-3.0.0.tgz && mv kafka_2.13-3.0.0/ kafka

cd kafka
mkdir -p data/kafka
mkdir -p data/zookeeper

echo 'listeners=PLAINTEXT://:9092' >> config/server.properties
echo 'advertised.listeners=PLAINTEXT://23.21.44.206:9092' >> config/server.properties

export JAVA_HOME="/usr/lib/jvm/jre-1.8.0-openjdk"
export JRE_HOME="/usr/lib/jvm/jre"
export PATH="/usr/local/bin/scala-2.13.3/bin:/usr/local/bin/kafka/bin":$PATH


nohup bin/zookeeper-server-start.sh config/zookeeper.properties &

sleep 20

nohup bin/kafka-server-start.sh config/server.properties &

sleep 10


nohup bin/kafka-topics.sh --create --partitions 1 --replication-factor 1 --topic kafka-topic --bootstrap-server localhost:9092  &

sleep 10