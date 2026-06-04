import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, desc

def iniciar_analisis_spark():
    print("=========================================================")
    print("   Iniciando Motor Analítico Apache Spark (Fase 2)       ")
    print("=========================================================")

    # 1. Creamos la sesión de Spark
    # En tu casa corre en modo 'local', pero en la Uni apuntará al "Master" de la red
    spark = SparkSession.builder \
        .appName("AnaliticaPoblacionUAA") \
        .master("local[*]") \
        .getOrCreate()

    # Reducimos el ruido de logs en la consola para ver solo los resultados
    spark.sparkContext.setLogLevel("ERROR")
    
    print("¡Sesión de Spark inicializada con éxito!")

    # 2. RUTA DINÁMICA CORREGIDA Y BLINDADA
    # Detecta exactamente dónde está parado este archivo query_analysis.py en tu Linux
    directorio_script = os.path.dirname(os.path.abspath(__file__))
    
    # Subimos correctamente los niveles necesarios para llegar a la raíz del Monorepo
    raiz_proyecto = os.path.abspath(os.path.join(directorio_script, "..", ".."))
    
    # Definimos las posibles rutas por si la carpeta se llama shared-data o data
    ruta_opcion1 = os.path.join(raiz_proyecto, "shared-data", "dataset.json")
    ruta_opcion2 = os.path.join(raiz_proyecto, "data", "dataset.json")
    
    # Evaluamos cuál de las rutas existe físicamente en tu computadora
    if os.path.exists(ruta_opcion1):
        ruta_dataset = ruta_opcion1
    elif os.path.exists(ruta_opcion2):
        ruta_dataset = ruta_opcion2
    else:
        print("\n=========================================================")
        print("❌ ERROR CRÍTICO: No se encontró el archivo dataset.json")
        print("=========================================================")
        print(f"Buscado sin éxito en:\n1. {ruta_opcion1}\n2. {ruta_opcion2}")
        print("\nPor favor, verifica que el archivo de los 100,000 registros esté")
        print("guardado en la raíz de tu proyecto dentro de la carpeta de datos.")
        spark.stop()
        return

    print(f"Cargando registros desde: {ruta_dataset}")
    print("Cargando los 100,000 registros en un DataFrame de Spark...")
    
    # Spark lee el JSON masivo de forma optimizada y deduce el esquema solo
    df = spark.read.json(ruta_dataset)
    
    print("\n--- ESQUEMA DETECTADO POR SPARK ---")
    df.printSchema()
    print("-----------------------------------")

    # -----------------------------------------------------------------
    # CONSULTAS ANALÍTICAS EXIGIDAS (SPARK SQL / DATAFRAMES)
    # -----------------------------------------------------------------
    
    print("\n[Consulta 1] Calculando promedio de ingresos por Estado...")
    # Agrupamos por estado y sacamos la media del ingreso mensual
    ingresos_por_estado = df.groupBy("estado") \
        .agg(avg("ingreso_mensual").alias("promedio_ingreso")) \
        .orderBy(desc("promedio_ingreso"))
    ingresos_por_estado.show(10)

    print("\n[Consulta 2] Conteo de usuarios activos vs inactivos...")
    # SE CORRIGE: Cambiamos "id" por el nombre de columna real "id_persona"
    usuarios_estatus = df.groupBy("activo") \
        .agg(count("id_persona").alias("total_usuarios"))
    usuarios_estatus.show()

    print("\n[Consulta 3] Las 5 profesiones más lucrativas en Aguascalientes...")
    # Filtramos por el estado local, agrupamos por ocupación y promediamos su economía
    top_profesiones_ags = df.filter(col("estado") == "Aguascalientes") \
        .groupBy("ocupacion") \
        .agg(avg("ingreso_mensual").alias("ingreso_medio"), count("id_persona").alias("total_personas")) \
        .orderBy(desc("ingreso_medio"))
    top_profesiones_ags.show(5)

    # 3. Cerramos la sesión de Spark limpiamente al terminar los cálculos
    print("=========================================================")
    print("Análisis finalizado de forma local. Cerrando Spark...")
    print("=========================================================")
    spark.stop()

if __name__ == "__main__":
    iniciar_analisis_spark()