import pytz
import os
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import explode, col

# Inicializa a sessão Spark
spark = SparkSession.builder.appName("ProcessRawToTrusted").getOrCreate()

# Configura o acesso ao MinIO
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "datalake")
hadoop_conf.set("fs.s3a.secret.key", "datalake")
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

spark

# Defina o caminho de entrada e saída com base na data
fuso_horario_brasilia = pytz.timezone('America/Sao_Paulo')
brasilia_time = datetime.now(fuso_horario_brasilia)
today = brasilia_time.strftime('%Y-%m-%d')

raw_path = f"s3a://raw/busdata/{today}"

routes_trusted_path = f"s3a://trusted/busroutes/{today}/"
positions_trusted_path = f"s3a://trusted/buspositions/{today}/"

# Exibe a lista de arquivos
files_df = spark.read.format("binaryFile").load(raw_path)

# Pega o primeiro arquivo para gerar o df_bus_lines
first_raw_file_path = files_df.collect()[0]["path"]
file_name = os.path.basename(first_raw_file_path).replace(".json", "")
                                                          
df_raw = spark.read.json(first_raw_file_path)

# Explodir o array "l" para acessar as informações de cada linha de ônibus
df_lines = df_raw.select(explode(col("l")).alias("linha"))

# DataFrame com as informações de "c" até "qv" (linhas de ônibus)
df_bus_lines = df_lines.select(
    col("linha.cl").alias("codigo_trajeto"),
    col("linha.sl").alias("sentido"),
    col("linha.c").alias("letreiro"),
    col("linha.lt0").alias("terminal_primario"),
    col("linha.lt1").alias("terminal_secundario"),
    col("linha.qv").alias("qnt_veiculos")
)
# Salva o df_bus_lines no caminho apenas uma vez
# Define o caminho de destino usando o nome do arquivo original
destination_path = os.path.join(routes_trusted_path, file_name)
    
# Salva o DataFrame processado no formato Parquet
#df_bus_lines.write.mode("overwrite").parquet(destination_path)
df_bus_lines.write.mode("overwrite").json(routes_trusted_path)
df_bus_lines.show()

# Itera sobre os arquivos para processar as posições dos veículos
for row in files_df.collect():
    raw_file_path = row["path"]
    raw_file_date = raw_file_path.split('_')[-1].split('.')[0]
    df_raw = spark.read.json(raw_file_path)
    
    # Explodir o array "l" para acessar as informações de cada linha de ônibus
    df_lines = df_raw.select(explode(col("l")).alias("linha"))

    # Explodir o array "vs" para acessar as informações de cada veículo dentro da linha de ônibus
    df_vehicles = df_lines.select(
        col("linha.cl").alias("codigo_trajeto"),
        col("linha.sl").alias("sentido"),
        explode(col("linha.vs")).alias("vehicle")
    )

    # DataFrame com as informações de "p" até "px" (posições dos veículos)
    df_vehicles_position = df_vehicles.select(
        col("codigo_trajeto"),
        col("sentido"),
        col("vehicle.p").alias("prefixo_veiculo"),
        col("vehicle.py").alias("latitude"),
        col("vehicle.px").alias("longitude")
    )
    
    # Salva as posições dos veículos no caminho de saída
    df_vehicles_position.write.mode("overwrite").json(positions_trusted_path + 'positions_' + raw_file_date)

df_vehicles_position.show()