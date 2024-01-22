import os
import zipfile
import requests
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType
import shutil

# Função para criar diretórios locais se não existirem
def create_directories():
    directories = ['/Zip', '/CSVs']
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            print(f'Directory created: {directory}')

# Função para baixar o arquivo zip
def download_zip(zip_url, zip_dir):
    response = requests.get(zip_url)
    zip_file_path = os.path.join(zip_dir, 'dados.zip')

    with open(zip_file_path, 'wb') as zip_file:
        zip_file.write(response.content)

    return zip_file_path

# Função para extrair arquivos CSV do zip para o diretório /CSVs
def extract_csv_from_zip(zip_file, csv_dir):
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        # Criar diretório se não existir
        os.makedirs(csv_dir, exist_ok=True)
        
        # Extrair todos os arquivos do zip
        zip_ref.extractall(csv_dir)
        
        # Retornar o diretório onde os arquivos foram extraídos
        print(f'Files extracted to: {csv_dir}')
        return csv_dir

# Função para renomear os arquivos removendo espaços
def rename_files_with_spaces(csv_dir):
    for root, dirs, files in os.walk(csv_dir):
        for file in files:
            if ' ' in file:
                new_file = file.replace(' ', '_')
                os.rename(os.path.join(root, file), os.path.join(root, new_file))
                print(f'Renamed file: {file} -> {new_file}')

# Função para ler CSVs, criar DataFrames e exportar para o HDFS
def process_csvs(zip_dir, csv_parent_dir, hdfs_dir):
    spark = SparkSession.builder.appName('CSVProcessing').getOrCreate()

    # Combinar o diretório pai CSV com o subdiretório contendo os arquivos CSV (por exemplo, '2000')
    csv_dir = os.path.join(csv_parent_dir, os.listdir(csv_parent_dir)[0])

    for csv_file in os.listdir(csv_dir):
        csv_file_path = os.path.join(csv_dir, csv_file)

        # Obtém o nome do arquivo sem extensão
        file_name = Path(csv_file).stem

        # Salva o CSV no HDFS usando a sessão Spark
        df = spark.read.format("csv").option("header", "false").option("delimiter", ";").load(f"file://{csv_file_path}")

        # Corrige o caminho para o HDFS
        hdfs_path = os.path.join(hdfs_dir, file_name, "input", f"{file_name}.csv")

        # Salva o CSV no HDFS
        df.coalesce(1).write.mode('overwrite').option('header', 'true').csv(hdfs_path)

# Função para apagar diretórios locais
def delete_directories():
    directories = ['/Zip', '/CSVs']
    for directory in directories:
        if os.path.exists(directory):
            shutil.rmtree(directory)
            print(f'Directory deleted: {directory}')

# Diretórios locais e no HDFS
local_zip_dir = '/Zip'
local_csv_dir = '/CSVs'
hdfs_dir = '/hdfs/data/order/tmp/dados_temp'
date_of_data = '2000'

# Link para download do arquivo zip
zip_url = ('https://portal.inmet.gov.br/uploads/dadoshistoricos/{}.zip'.format(date_of_data))

# Passo 1: Criar diretórios locais
create_directories()

# Passo 2: Baixar o arquivo zip
zip_file_path = download_zip(zip_url, local_zip_dir)

# Passo 3: Extrair arquivos CSV
extracted_csv_dir = extract_csv_from_zip(zip_file_path, local_csv_dir)

# Passo adicional: Renomear arquivos removendo espaços
rename_files_with_spaces(extracted_csv_dir)

# Passo 4: Processar CSVs e exportar para o HDFS
process_csvs(local_csv_dir, extracted_csv_dir, hdfs_dir)

# Passo 5: Apagar diretórios locais
delete_directories()
