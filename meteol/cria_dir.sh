#!/bin/bash

# Criação dos diretórios do HDFS
hdfs dfs -mkdir /hdfs
hdfs dfs -chmod 775 /hdfs

hdfs dfs -mkdir /hdfs/data
hdfs dfs -chmod 775 /hdfs/data

hdfs dfs -mkdir /hdfs/data/order
hdfs dfs -chmod 775 /hdfs/data/order

# Diretórios específicos para o script PySpark
hdfs dfs -mkdir /hdfs/data/order/tmp
hdfs dfs -chmod 775 /hdfs/data/order/tmp

hdfs dfs -mkdir /hdfs/data/order/tmp/dados_temp
hdfs dfs -chmod 775 /hdfs/data/order/tmp/dados_temp


