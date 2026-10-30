from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count
from pyspark.sql.types import StructType, StructField, StringType

# Inicializar sesión de Spark con los conectores necesarios para Kafka
spark = SparkSession.builder \
    .appName("SparkStreamingAccidentalidad") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("=== Servidor Spark Streaming Escuchando Flujo en Vivo ===")

# Definir el esquema basado en las columnas principales esperadas del CSV
schema = StructType([
    StructField("departamento", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("fecha", StringType(), True)
])

# Conectarse al flujo continuo de Kafka
df_streaming = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "accidentes_colombia") \
    .load()

# Convertir el valor binario de Kafka a texto estructurado JSON
df_json = df_streaming.selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*")

# Procesamiento analítico en tiempo real: Contar accidentes acumulados por departamento
conteo_departamentos = df_json.groupBy("departamento").agg(count("*").alias("total_en_vivo"))

# Mostrar los resultados continuamente en la consola de la VM
query = conteo_departamentos.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
