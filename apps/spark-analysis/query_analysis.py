import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc

def iniciar_analisis_spark():
    print("=========================================================")
    print("   Iniciando Motor Analítico Apache Spark (Fase 2)       ")
    print("=========================================================")

    # Creamos la sesión apuntando al Master central del módem
    spark = SparkSession.builder \
        .appName("AnaliticaPoblacionUAA") \
        .master("spark://192.168.0.103:7077") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print("¡Sesión de Spark inicializada con éxito!")

    # Ruta fija unificada dentro del volumen compartido de Docker
    ruta_dataset = "/opt/spark/shared-data/dataset.json"
    
    if not os.path.exists(ruta_dataset):
        ruta_dataset = "dataset.json"

    print(f"Cargando registros desde: {ruta_dataset}")
    print("Cargando los 100,000 registros generados por Kafka en el DataFrame...")
    
    df = spark.read.json(ruta_dataset)
    
    print("\n--- ESQUEMA DE 10 CAMPOS DETECTADO POR EL CLÚSTER ---")
    df.printSchema()
    print("-----------------------------------------------------")

    # -----------------------------------------------------------------
    # CONSULTAS ANALÍTICAS EXIGIDAS EN LA RÚBRICA
    # -----------------------------------------------------------------
    
    print("\n[Consulta 1] Calculando promedio de ingresos por Estado...")
    ingresos_por_estado = df.groupBy("estado") \
        .agg(avg("ingreso_mensual").alias("promedio_ingreso")) \
        .orderBy(desc("promedio_ingreso"))
    ingresos_por_estado.show(10)

    print("\n[Consulta 2] Conteo de usuarios activos vs inactivos...")
    usuarios_estatus = df.groupBy("activo") \
        .agg(count("id_persona").alias("total_usuarios"))
    usuarios_estatus.show()

    print("\n[Consulta 3] Las 5 profesiones más lucrativas en Aguascalientes...")
    top_profesiones_ags = df.filter(col("estado") == "Aguascalientes") \
        .groupBy("ocupacion") \
        .agg(avg("ingreso_mensual").alias("ingreso_medio"), count("id_persona").alias("total_personas")) \
        .orderBy(desc("ingreso_medio"))
    top_profesiones_ags.show(5)

    print("=========================================================")
    print("Análisis finalizado en el Clúster Distribuido. Cerrando...")
    print("=========================================================")
    spark.stop()

if __name__ == "__main__":
    iniciar_analisis_spark()