from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import requests
from zipfile import ZipFile
import shutil

def download_data(url, local_zip_path):
    # Criar SparkSession
    spark = SparkSession.builder.appName('Download - CSV').getOrCreate()

    try:
        # Baixar o arquivo ZIP
        response = requests.get(url)

        # Certificar-se de que o diretório pai existe
        local_zip_dir = spark._jvm.java.io.File(local_zip_path).getParentFile()
        local_zip_dir.mkdirs()

        with open(local_zip_path, "wb") as zip_file:
            zip_file.write(response.content)

    except Exception as e:
        print("ERRO no download:", e)

    finally:
        spark.stop()  # Parar a SparkSession após o download

def extract_csv(local_zip_path, local_extract_path):
    # Criar SparkSession
    spark = SparkSession.builder.appName('Extract - CSV').getOrCreate()

    try:
        # Extrair o conteúdo do ZIP diretamente para o local_extract_path
        with ZipFile(local_zip_path, "r") as zip_ref:
            for file_info in zip_ref.infolist():
                file_info.filename = file_info.filename.encode('cp437').decode('utf-8')  # Corrigir codificação para Spark
                zip_ref.extract(file_info, local_extract_path)

    except Exception as e:
        print("ERRO na extração:", e)

    finally:
        spark.stop()  # Parar a SparkSession após a extração

def get_csv(file_path):
    # Criar SparkSession
    spark = SparkSession.builder.appName('Read - CSV').getOrCreate()

    try:
        df = spark.read.format("csv").option("header", True).load(file_path)
        return df

    except Exception as e:
        print("ERRO na leitura do CSV:", e)

    finally:
        spark.stop()  # Parar a SparkSession após a leitura do CSV

# Caminho local para salvar o arquivo ZIP
local_zip_path = "hdfs:///hdfs/data/order/tmp/dados_brutos"

# Caminho local para extrair os arquivos CSV
local_extract_path = "hdfs:///hdfs/data/order/tmp/dados_temp"

# Caminho no HDFS para armazenar os arquivos CSV
hdfs_path = "hdfs:///hdfs/data/order/2000"

date_link = '2000'  # Nome do arquivo ZIP

try:
    # Baixar o CSV diretamente para a pasta local no HDFS
    download_data("https://portal.inmet.gov.br/uploads/dadoshistoricos/{}.zip".format(date_link), local_zip_path)

    # Extrair o CSV
    extract_csv(local_zip_path, local_extract_path)

    # Listar arquivos extraídos
    csv_files = [f for f in spark._jvm.java.io.File(local_extract_path).list() if f.endswith('.CSV')]

    for csv_file in csv_files:
        # Caminho completo do arquivo local
        local_file_path = spark._jvm.java.io.File(local_extract_path, csv_file).getPath()

        # Caminho no HDFS para armazenar os arquivos CSV com seus próprios nomes
        hdfs_file_path = spark._jvm.java.io.Path(hdfs_path, csv_file).toString()

        # Ler CSV e salvar no HDFS
        df = get_csv(local_file_path)
        df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(hdfs_file_path)

    print("ARQUIVOS CSV BAIXADOS NO HDFS")

except Exception as e:
    print("ERRO:", e)

finally:
    try:
        # Remover os arquivos temporários
        if spark._jvm.java.nio.file.Files.exists(spark._jvm.java.nio.file.Paths.get(local_extract_path)):
            spark._jvm.java.nio.file.Files.delete(spark._jvm.java.nio.file.Paths.get(local_extract_path))

        # Verificar a existência do arquivo antes de removê-lo
        if spark._jvm.java.nio.file.Files.exists(spark._jvm.java.nio.file.Paths.get(local_zip_path)):
            spark._jvm.java.nio.file.Files.delete(spark._jvm.java.nio.file.Paths.get(local_zip_path))
        
        print("ARQUIVOS TEMPORÁRIOS EXCLUÍDOS")
    except Exception as e:
        print("ERRO ao remover arquivos temporários:", e)
