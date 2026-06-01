from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# 1. Definimos la misma estructura que tienen tus JSON del generador
schema = StructType([
    StructField("id_transaccion", IntegerType(), True),
    StructField("id_usuario", IntegerType(), True),
    StructField("nombre", StringType(), True),
    StructField("monto", DoubleType(), True),
    StructField("ciudad", StringType(), True),
    StructField("timestamp", StringType(), True)
])

# 2. Inicializamos la sesión de Spark de manera local
spark = SparkSession.builder \
    .appName("SparkKafkaLocalConsumer") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")
print("🚀 Spark iniciado y escuchando de manera local...\n")

# 3. Nos conectamos al streaming de Kafka
df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transacciones") \
    .load()

# 4. Convertimos el valor binario de Kafka a texto (JSON) y le aplicamos nuestra estructura
df_json = df_kafka.selectExpr("CAST(value AS STRING) as json_string") \
    .select(from_json(col("json_string"), schema).alias("data")) \
    .select("data.*")

# 5. Pintamos el resultado directo en la terminal conforme van llegando los datos
query = df_json.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()