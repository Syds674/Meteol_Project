from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType

# 1. Criar uma sessão Spark
spark = SparkSession.builder.appName('MergeCSVToHive').enableHiveSupport().getOrCreate()

# 2. Definir o schema
schema = StructType([
    StructField("REGIAO", StringType(), True),
    StructField("UF", StringType(), True),
    StructField("ESTACAO", StringType(), True),
    StructField("CODIGO (WMO)", StringType(), True),
    StructField("LATITUDE", FloatType(), True),
    StructField("LONGITUDE", FloatType(), True),
    StructField("ALTITUDE", FloatType(), True),
    StructField("DATA DE FUNDACAO", StringType(), True),
    StructField("Data", StringType(), True),
    StructField("Hora UTC", StringType(), True),
    StructField("PRECIPITACAO TOTAL, HORARIO (mm)", FloatType(), True),
    StructField("PRESSAO ATMOSFERICA AO NIVEL DA ESTACAO, HORARIA (mB)", FloatType(), True),
    StructField("PRESSAO ATMOSFERICA MAX.NA HORA ANT. (AUT) (mB)", FloatType(), True),
    StructField("PRESSAO ATMOSFERICA MIN. NA HORA ANT. (AUT) (mB)", FloatType(), True),
    StructField("RADIACAO GLOBAL (Kj/m²)", FloatType(), True),
    StructField("TEMPERATURA DO AR - BULBO SECO, HORARIA (°C)", FloatType(), True),
    StructField("TEMPERATURA DO PONTO DE ORVALHO (°C)", FloatType(), True),
    StructField("TEMPERATURA MAXIMA NA HORA ANT. (AUT) (°C)", FloatType(), True),
    StructField("TEMPERATURA MINIMA NA HORA ANT. (AUT) (°C)", FloatType(), True),
    StructField("TEMPERATURA ORVALHO MAX. NA HORA ANT. (AUT) (°C)", FloatType(), True),
    StructField("TEMPERATURA ORVALHO MIN. NA HORA ANT. (AUT) (°C)", FloatType(), True),
    StructField("UMIDADE REL. MAX. NA HORA ANT. (AUT) (%)", FloatType(), True),
    StructField("UMIDADE REL. MIN. NA HORA ANT. (AUT) (%)", FloatType(), True),
    StructField("UMIDADE RELATIVA DO AR, HORARIA (%)", FloatType(), True),
    StructField("VENTO, DIRECAO HORARIA (gr) (° (gr))", FloatType(), True),
    StructField("VENTO, RAJADA MAXIMA (m/s)", FloatType(), True),
    StructField("VENTO, VELOCIDADE HORARIA (m/s)", FloatType(), True)
])

# ...

# 3. Ler cada arquivo CSV e mesclar em um DataFrame
csv_parent_dir = '/hdfs/data/order/tmp/dados_temp'

# Lista os diretórios no HDFS
csv_dirs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())\
    .listStatus(spark._jvm.org.apache.hadoop.fs.Path(csv_parent_dir))

merged_df = None

for csv_dir_status in csv_dirs:
    csv_dir_path = csv_dir_status.getPath().toString()
    
    # Verifica se o caminho é um diretório
    if csv_dir_status.isDirectory():
        # Lê todos os arquivos CSV no diretório
        df = spark.read.option("delimiter", ";").schema(schema).csv(csv_dir_path + "/input")
        
        # Renomeia as colunas com alias substituindo caracteres inválidos
        for col_name in df.columns:
            new_col_name = ''.join(c if c.isalnum() or c == '_' else '_' for c in col_name.lower())
            df = df.withColumnRenamed(col_name, new_col_name)
        
        # Combina DataFrames
        if merged_df is None:
            merged_df = df
        else:
            merged_df = merged_df.union(df)

# 4. Enviar o DataFrame para o Hive
hive_database_name = 'meterio'
hive_table_name = 'table_2000'  # Alterado para um nome válido no Hive

# Salvando o DataFrame como uma tabela permanente no Hive
merged_df.write.mode('overwrite').saveAsTable(f"{hive_database_name}.{hive_table_name}")

# 5. Encerrar a sessão Spark
spark.stop()
