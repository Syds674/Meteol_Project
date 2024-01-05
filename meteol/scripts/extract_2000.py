from pyspark.sql import SparkSession
from pyspark import SparkFiles
import requests
from zipfile import ZipFile

# Inicializa a sessão do Spark
spark = SparkSession.builder.appName("YourAppName").getOrCreate()

def download_data(url, local_zip_path):
    try:
        # Baixar o arquivo ZIP
        response = requests.get(url)

        # Certificar-se de que o diretório pai existe
        local_zip_dir = SparkFiles.getRootDirectory()
        local_zip_path = f"{local_zip_dir}/{local_zip_path}"

        with open(local_zip_path, "wb") as zip_file:
            zip_file.write(response.content)

    except Exception as e:
        print("ERRO no download:", e)
        raise  # Propaga a exceção

def extract_csv(local_zip_path, local_extract_path):
    try:
        # Extrair o conteúdo do ZIP diretamente para o local_extract_path
        with ZipFile(local_zip_path, "r") as zip_ref:
            zip_ref.extractall(local_extract_path)

    except Exception as e:
        print("ERRO na extração:", e)
        raise  # Propaga a exceção

def get_csv(file_path):
    try:
        df = spark.read.format("csv").option("header", True).load(file_path)
        return df

    except Exception as e:
        print("ERRO na leitura do CSV:", e)
        raise  # Propaga a exceção


# Ano
date_link = "2000"

# Caminho local para salvar o arquivo ZIP
local_zip_path = "/hdfs/data/order/tmp/dados_brutos"

# Caminho local para extrair os arquivos CSV
local_extract_path = "/hdfs/data/order/tmp/dados_temp"

# Caminho no HDFS para armazenar os arquivos CSV
hdfs_path = "/hdfs/data/order/{}".format(date_link)


try:
    # Baixar o CSV diretamente para a pasta local no HDFS
    download_data("https://portal.inmet.gov.br/uploads/dadoshistoricos/{}.zip".format(date_link), local_zip_path)
    
    # Extrair o CSV
    extract_csv(local_zip_path, local_extract_path)

    # Listar arquivos extraídos
    csv_files = [f for f in SparkFiles.get() if f.endswith('.CSV')]

    for csv_file in csv_files:
        # Caminho completo do arquivo local
        local_file_path = f"{local_extract_path}/{csv_file}"

        # Caminho no HDFS para armazenar os arquivos CSV com seus próprios nomes
        hdfs_file_path = f"{hdfs_path}/{csv_file}"

        # Ler CSV e salvar no HDFS
        df = get_csv(local_file_path)
        df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(hdfs_file_path)

    print("ARQUIVOS CSV BAIXADOS NO HDFS")

except Exception as e:
    print("ERRO:", e)

finally:
    try:
        # Remover os arquivos temporários no HDFS
        if spark._jvm.java.io.File(local_extract_path).exists():
            spark._jvm.org.apache.hadoop.fs.FileUtil.fullyDelete(spark._jvm.org.apache.hadoop.fs.Path(local_extract_path))

        # Verificar a existência do arquivo antes de removê-lo no HDFS
        if spark._jvm.java.io.File(local_zip_path).exists():
            spark._jvm.org.apache.hadoop.fs.FileUtil.fullyDelete(spark._jvm.org.apache.hadoop.fs.Path(local_zip_path))

        print("ARQUIVOS TEMPORÁRIOS EXCLUÍDOS")
    except Exception as e:
        print("ERRO ao remover arquivos temporários:", e)

    finally:
        # Encerrar a SparkSession
        spark.stop()

