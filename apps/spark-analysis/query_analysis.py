from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, avg, count, desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType


BROKERS_KAFKA = "100.115.62.37:9092,100.123.126.75:9092,100.72.209.77:9092"

TOPICOS = (
    "datos-usuarios-zona1,"
    "datos-usuarios-zona2,"
    "datos-usuarios-zona3,"
    "datos-usuarios-zona4,"
    "datos-usuarios-zona5"
)


def iniciar_analisis_spark_kafka():
    print("=========================================================")
    print("   ANALISIS EN VIVO CON APACHE SPARK Y KAFKA             ")
    print("=========================================================")

    spark = SparkSession.builder \
        .appName("AnalisisKafkaSparkEnVivo") \
        .master("spark://100.72.209.77:7077") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("Sesion de Spark iniciada correctamente.")
    print("Master Spark: spark://100.72.209.77:7077")
    print(f"Brokers Kafka: {BROKERS_KAFKA}")
    print(f"Topicos Kafka: {TOPICOS}")

    esquema = StructType([
        StructField("id_persona", IntegerType(), True),
        StructField("demo_id", StringType(), True),
        StructField("topic_destino", StringType(), True),
        StructField("zona", StringType(), True),
        StructField("nombre", StringType(), True),
        StructField("apellido", StringType(), True),
        StructField("edad", IntegerType(), True),
        StructField("genero", StringType(), True),
        StructField("ciudad", StringType(), True),
        StructField("estado", StringType(), True),
        StructField("ocupacion", StringType(), True),
        StructField("nivel_estudios", StringType(), True),
        StructField("ingreso_mensual", DoubleType(), True),
        StructField("antiguedad_anos", IntegerType(), True),
        StructField("activo", BooleanType(), True),
        StructField("fecha_registro", StringType(), True),
    ])

    print("\nConectando Spark directamente a Kafka...")

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BROKERS_KAFKA) \
        .option("subscribe", TOPICOS) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    datos = kafka_df.select(
        col("topic"),
        col("partition"),
        col("offset"),
        from_json(col("value").cast("string"), esquema).alias("data")
    ).select(
        "topic",
        "partition",
        "offset",
        "data.*"
    )

    print("\n=========================================================")
    print("CONSULTA EN VIVO 1: TOTAL DE REGISTROS POR ZONA")
    print("=========================================================")

    conteo_por_zona = datos.groupBy("zona", "topic_destino") \
        .agg(count("id_persona").alias("total_registros")) \
        .orderBy("zona")

    query_zona = conteo_por_zona.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "20") \
        .queryName("conteo_por_zona") \
        .start()

    print("\n=========================================================")
    print("CONSULTA EN VIVO 2: PROMEDIO DE INGRESOS POR ESTADO")
    print("=========================================================")

    ingresos_estado = datos.groupBy("estado") \
        .agg(
            avg("ingreso_mensual").alias("promedio_ingreso"),
            count("id_persona").alias("total_personas")
        ) \
        .orderBy(desc("promedio_ingreso"))

    query_estado = ingresos_estado.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .option("numRows", "10") \
        .queryName("ingresos_por_estado") \
        .start()

    print("\n=========================================================")
    print("SPARK ESTA ESCUCHANDO KAFKA EN TIEMPO REAL")
    print("Ahora ejecuta el producer para enviar datos.")
    print("Para detener Spark usa CTRL + C.")
    print("=========================================================")

    query_zona.awaitTermination()
    query_estado.awaitTermination()


if __name__ == "__main__":
    iniciar_analisis_spark_kafka()