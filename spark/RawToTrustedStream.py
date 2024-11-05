import pytz
import os
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.functions import explode, col, from_json
from pyspark.sql.types import StringType, StructType, StructField

# Inicializa a sessão Spark
spark = SparkSession.builder \
    .appName("KafkaMinioSparkProcessor") \
    .getOrCreate()

# Configura o acesso ao MinIO
hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.access.key", "datalake")
hadoop_conf.set("fs.s3a.secret.key", "datalake")
hadoop_conf.set("fs.s3a.endpoint", "http://minio:9000")
hadoop_conf.set("fs.s3a.path.style.access", "true")
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

# Configurações do Kafka
KAFKA_TOPIC = "minio_events"
KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"

# Lê o stream de eventos do Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .load()

# Extrai a mensagem do Kafka e converte para string
df_kafka_parsed = df_kafka.selectExpr("CAST(value AS STRING) as message")

# Define um schema para o evento do Minio
schema = StructType([
    StructField("EventName", StringType(), True),
    StructField("Records", StructType([
        StructField("s3", StructType([
            StructField("object", StructType([
                StructField("key", StringType(), True)
            ]))
        ]))
    ]))
])

# Faz o parsing dos eventos do Kafka usando o schema
df_parsed = df_kafka_parsed.withColumn("parsed_value", from_json(col("message"), schema))

# Extrai o caminho do arquivo do evento
df_file_paths = df_parsed.select(col("parsed_value.Records.s3.object.key").alias("file_path"))

# Função para processar o arquivo recebido
def process_file(df, epoch_id):
    # Converte o DataFrame do Spark para pandas
    file_paths = [row["file_path"] for row in df.collect()]

    for file_path in file_paths:
        # Lê o arquivo JSON do Minio
        file_path_full = f"s3a://raw/{file_path}"
        df_raw = spark.read.json(file_path_full)

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

        # Define o caminho de destino usando o nome do arquivo original para as rotas
        file_name = os.path.basename(file_path).replace(".json", "")
        routes_trusted_path = f"s3a://trusted/busroutes/{file_name}"
        
        # Salva o DataFrame processado no formato JSON no bucket 'trusted'
        df_bus_lines.write.mode("overwrite").json(routes_trusted_path)

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

        # Define o caminho de destino usando o nome do arquivo original para as posições
        positions_trusted_path = f"s3a://trusted/buspositions/{file_name}"
        
        # Salva o DataFrame processado no formato JSON no bucket 'trusted'
        df_vehicles_position.write.mode("overwrite").json(positions_trusted_path)

# Configura o processamento contínuo dos arquivos
query = df_file_paths.writeStream \
    .foreachBatch(process_file) \
    .outputMode("update") \
    .start()

query.awaitTermination()