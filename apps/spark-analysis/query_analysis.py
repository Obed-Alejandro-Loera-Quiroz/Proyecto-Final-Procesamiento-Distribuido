import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc

def iniciar_analisis_spark():
    print("=========================================================")
    print("   Iniciando Motor Analítico Apache Spark (Fase 2)       ")
    print("=========================================================")

    # Creamos la sesión apuntando a tu IP Fija de Tailscale (Tú eres el Master)
    spark = SparkSession.builder \
        .appName("AnaliticaPoblacionUAA") \
        .master("spark://100.72.209.77:7077") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
    print("¡Sesión de Spark inicializada con éxito vía Tailscale!")

    # Ruta fija unificada dentro del volumen compartido de Docker
    ruta_dataset = "/opt/spark/shared-data/dataset.json"
    
    if not os.path.exists(ruta_dataset):
        ruta_dataset = "dataset.json"

    print(f"Cargando registros desde: {ruta_dataset}")
    print("Cargando los 100,000 registros generados por Kafka en el DataFrame...")
    
    # Cargamos el dataset generado
    df = spark.read.json(ruta_dataset)
    
    # EXIGENCIA DEL PROFESOR: Presentar claramente la estructura de los datos generados
    print("\n--- ESQUEMA DE DATOS DETECTADO POR EL CLÚSTER ---")
    df.printSchema()
    print("-----------------------------------------------------")
    
    print("Ejemplo de registro (Primeros 3 elementos):")
    df.show(3, truncate=False)

    # 💡 HERRAMIENTA CLAVE PARA MODIFICACIÓN EN VIVO:
    # Registramos el DataFrame como una tabla SQL virtual para responder consultas rápidas del profesor
    df.createOrReplaceTempView("personas")

    # -----------------------------------------------------------------
    # CONSULTAS ANALÍTICAS EXIGIDAS EN LA RÚBRICA
    # -----------------------------------------------------------------
    
    print("\n[Consulta 1] Calculando promedio de ingresos por Estado...")
    ingresos_por_estado = df.groupBy("estado") \
        .agg(avg("ingreso_mensual").alias("promedio_income")) \
        .orderBy(desc("promedio_income"))
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

    # -----------------------------------------------------------------
    # 🚀 ESPACIO DE MODIFICACIÓN EN VIVO (Para las peticiones del profesor)
    # -----------------------------------------------------------------
    print("\n=========================================================")
    print("  ZONA DE CONSULTAS IMPROVISADAS (EXAMEN EN VIVO)       ")
    print("=========================================================")
    
    # Ejemplo de consulta SQL directa que pueden modificar al instante si el profesor la pide:
    # query_profesor = "SELECT * FROM personas WHERE estado = 'Aguascalientes' AND edad < 25"
    # spark.sql(query_profesor).show(10)
    
    print("Listo para modificar y ejecutar consultas adicionales en tiempo real.")
    print("=========================================================")

    # Cerramos sesión de forma limpia
    spark.stop()

if __name__ == "__main__":
    iniciar_analisis_spark()