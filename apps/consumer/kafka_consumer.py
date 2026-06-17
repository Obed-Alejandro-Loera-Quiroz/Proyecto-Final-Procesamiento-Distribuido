import json
import os
from kafka import KafkaConsumer

def iniciar_consumidor():
    print("=========================================================")
    print("Iniciando Consumidor: CAPTURA MULTI-FORMATO (JSON, CSV, SQL)")
    print("=========================================================")

    # CLÚSTER REMOTO: IPs de Tailscale actualizadas
    BROKERS_CLUSTER = [
        '100.115.62.37:9092',  # Osvaldo (Nodo 1)
        '100.65.178.22:9092',  # Brayan (Nodo 2 - Temporal)
        '100.72.209.77:9092'   # Obed (Nodo 3)
    ]

    ruta_base = "/opt/spark/shared-data/"
    if not os.path.exists(ruta_base):
        ruta_base = ""

    # Definición de las 3 rutas de archivos requeridos
    ruta_json = os.path.join(ruta_base, "dataset.json")
    ruta_csv = os.path.join(ruta_base, "dataset_respaldo.csv")
    ruta_sql = os.path.join(ruta_base, "dataset_inserts.sql")

    try:
        # Suscripción explícita a los 3 tópicos de zonas en Tailscale
        consumer = KafkaConsumer(
            'datos-usuarios-zona1',
            'datos-usuarios-zona2',
            'datos-usuarios-zona3',
            bootstrap_servers=BROKERS_CLUSTER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='grupo-final-uaa',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("¡Conexión Exitosa! Escuchando los 3 topics en paralelo...")
        print(f"-> Formato JSON en: {ruta_json}")
        print(f"-> Formato CSV (12 columnas) en: {ruta_csv}")
        print(f"-> Formato SQL (Sentencias INSERT) en: {ruta_sql}\n")
    except Exception as e:
        print(f"❌ Error al conectar el consumidor: {e}")
        return

    total_recibidos = 0

    # Abrimos los 3 archivos al mismo tiempo para escritura eficiente
    with open(ruta_json, 'w', encoding='utf-8') as f_json, \
         open(ruta_csv, 'w', encoding='utf-8') as f_csv, \
         open(ruta_sql, 'w', encoding='utf-8') as f_sql:
        
        # Cabecera para el archivo CSV
        f_csv.write("id_persona,nombre,apellido,edad,genero,ciudad,estado,ocupacion,nivel_estudios,ingreso_mensual,antiguedad_anos,activo\n")
        
        # Cabecera inicial de contexto para el script SQL
        f_sql.write("-- =========================================================\n")
        f_sql.write("-- DATASET GENERADO AUTOMÁTICAMENTE POR KAFKA (100K REGISTROS)\n")
        f_sql.write("-- =========================================================\n\n")
        
        try:
            for mensaje in consumer:
                registro = mensaje.value
                
                # 1. ESCRITURA EN FORMATO JSON
                f_json.write(json.dumps(registro, ensure_ascii=False) + "\n")
                
                # 2. ESCRITURA EN FORMATO CSV (Limpiando comas de la ciudad)
                ciudad_limpia = registro['ciudad'].replace(",", "")
                linea_csv = f"{registro['id_persona']},{registro['nombre']},{registro['apellido']},{registro['edad']},{registro['genero']},{ciudad_limpia},{registro['estado']},{registro['ocupacion']},{registro['nivel_estudios']},{registro['ingreso_mensual']},{registro['antiguedad_anos']},{registro['activo']}\n"
                f_csv.write(linea_csv)
                
                # 3. ESCRITURA EN FORMATO SQL (Sentencias estructuradas INSERT INTO)
                # Escapamos comillas simples en strings por si Faker genera nombres con apóstrofes
                nom_sql = registro['nombre'].replace("'", "''")
                ape_sql = registro['apellido'].replace("'", "''")
                ciu_sql = ciudad_limpia.replace("'", "''")
                est_sql = registro['estado'].replace("'", "''")
                ocu_sql = registro['ocupacion'].replace("'", "''")
                esc_sql = registro['nivel_estudios'].replace("'", "''")
                act_sql = 1 if registro['activo'] else 0  # Convertimos True/False a 1/0 binario para SQL
                
                linea_sql = f"INSERT INTO personas (id_persona, nombre, apellido, edad, genero, ciudad, estado, ocupacion, nivel_estudios, ingreso_mensual,固定_anos, activo) VALUES ({registro['id_persona']}, '{nom_sql}', '{ape_sql}', {registro['edad']}, '{registro['genero']}', '{ciu_sql}', '{est_sql}', '{ocu_sql}', '{esc_sql}', {registro['ingreso_mensual']}, {registro['antiguedad_anos']}, {act_sql});\n"
                f_sql.write(linea_sql)
                
                total_recibidos += 1
                
                if total_recibidos % 10000 == 0:
                    print(f"📥 [Clúster Tailscale] {total_recibidos} registros Faker atrapados y respaldados en JSON, CSV y SQL.")
                    
                if total_recibidos >= 100000:
                    print("\n=========================================================")
                    print(" ¡META COMPLETADA! 100,000 registros guardados en los 3 formatos.")
                    print("=========================================================")
                    break

        except KeyboardInterrupt:
            print("\nConsumidor detenido manualmente.")
        finally:
            consumer.close()

if __name__ == "__main__":
    iniciar_consumidor()