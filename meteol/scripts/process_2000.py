from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
import os

def get_files_hdfs(hdfs_path):
    # Lista os arquivos CSV na pasta especificada no HDFS
    return [f for f in os.listdir(hdfs_path) if f.endswith('.csv')]

def merge_and_save_as_table(table_name, dfs):
    # Mescla os DataFrames e salva a tabela no Hive
    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = merged_df.union(df.select(merged_df.columns))
    
    # Adiciona uma coluna 'source' para identificar a origem dos dados
    merged_df = merged_df.withColumn('source', lit(table_name))
    
    # Salva a tabela no Hive
    merged_df.write.mode('overwrite').format('orc').saveAsTable("meterio.{}".format(table_name))

date_path = '2000'

# Caminho no HDFS onde estão os arquivos CSV
hdfs_path = "/hdfs/data/order/{}".format(date_path)

try:
    spark = SparkSession.builder \
        .master("yarn") \
        .appName("Ingestao - dados para Hive") \
        .enableHiveSupport() \
        .getOrCreate()

    # Lista os arquivos na pasta do HDFS
    csv_files = get_files_hdfs(hdfs_path)

    # Lista para armazenar os DataFrames lidos de cada arquivo CSV
    dfs = []

    for csv_file in csv_files:
        # Caminho completo do arquivo no HDFS
        hdfs_file_path = os.path.join(hdfs_path, csv_file)

        # Lê o CSV
        df = spark.read.csv(hdfs_file_path, header=True)
        # Adiciona o DataFrame à lista
        dfs.append(df)

    # Gera o nome da tabela mesclada
    merged_table_name = "merged_{}".format(date_path)

    # Mescla os DataFrames e salva como tabela no Hive
    merge_and_save_as_table(merged_table_name, dfs)

except Exception as e:
    print("ERRO:", e)
