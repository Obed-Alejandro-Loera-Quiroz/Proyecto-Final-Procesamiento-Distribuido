import json
import os
import sys
from kafka import KafkaConsumer

def iniciar_consumidor():
    print("=========================================================")
    print("Iniciando Consumidor: CAPTURA MULTI-FORMATO EN TIEMPO REAL")
    print("=========================================================")

    # CLÚSTER REMOTO: Mapeo de puertos exteriores 9094
    BROKERS_CLUSTER = [
        '100.115.62.37:9094',  # Osvaldo (Nodo 1)
        '100.123.126.75:9094', # Brayan (Nodo 2)
        '100.72.209.77:9094'   # Obed (Nodo 3)
    ]

    ruta_base = "/opt/spark/shared-data/"
    if not os.path.exists(ruta_base):
        ruta_base = ""

    # Definición de las 3 rutas solicitadas por el Dr. Galván
    ruta_json = os.path.join(ruta_base, "dataset.json")
    ruta_csv = os.path.join(ruta_base, "dataset_respaldo.csv")
    ruta_sql = os.path.join(ruta_base, "dataset_inserts.sql")

    try:
        print("Enlazando con los Brokers de Tailscale en puerto 9094...")
        # Configuración tolerante a variaciones de micro-cortes residenciales
        consumer = KafkaConsumer(
            'datos-usuarios-zona1',
            'datos-usuarios-zona2',
            'datos-usuarios-zona3',
            bootstrap_servers=BROKERS_CLUSTER,
            auto_offset_reset='earliest',  # Atrapa todo lo acumulado desde el inicio
            enable_auto_commit=True,
            group_id='grupo-final-uaa',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            request_timeout_ms=60000,      # Tiempo de gracia de red extendido
            session_timeout_ms=30000
        )
        print("\n¡CONEXIÓN ESTABLECIDA CON ÉXITO!")
        print(f"-> Guardando JSON en: {ruta_json if ruta_json else 'Carpeta de la App'}")
        print(f"-> Guardando CSV en:  {ruta_csv if ruta_csv else 'Carpeta de la App'}")
        print(f"-> Guardando SQL en:  {ruta_sql if ruta_sql else 'Carpeta de la App'}\n")
        print("Escuchando el canal distribuido, esperando inyección de datos...\n")
    except Exception as e:
        print(f"❌ Error al iniciar el receptor: {e}")
        return

    total_recibidos = 0

    # Apertura y escritura simultánea multi-formato
    with open(ruta_json, 'w', encoding='utf-8') as f_json, \
         open(ruta_csv, 'w', encoding='utf-8') as f_csv, \
         open(ruta_sql, 'w', encoding='utf-8') as f_sql:
        
        f_csv.write("id_persona,nombre,apellido,edad,genero,ciudad,estado,ocupacion,nivel_estudios,ingreso_mensual,antiguedad_anos,activo\n")
        f_sql.write("-- =========================================================\n")
        f_sql.write("-- DATASET GENERADO AUTOMÁTICAMENTE POR KAFKA (100K REGISTROS)\n")
        f_sql.write("-- =========================================================\n\n")
        
        try:
            for mensaje in consumer:
                registro = mensaje.value
                total_recibidos += 1
                
                # 1. Escritura JSON
                f_json.write(json.dumps(registro, ensure_ascii=False) + "\n")
                
                # 2. Escritura CSV (Limpieza de delimitadores internos)
                ciudad_limpia = registro['ciudad'].replace(",", "")
                linea_csv = f"{registro['id_persona']},{registro['nombre']},{registro['apellido']},{registro['edad']},{registro['genero']},{ciudad_limpia},{registro['estado']},{registro['ocupacion']},{registro['nivel_estudios']},{registro['ingreso_mensual']},{registro['antiguedad_anos']},{registro['activo']}\n"
                f_csv.write(linea_csv)
                
                # 3. Escritura SQL
                nom_sql = registro['nombre'].replace("'", "''")
                ape_sql = registro['apellido'].replace("'", "''")
                ciu_sql = ciudad_limpia.replace("'", "''")
                est_sql = registro['estado'].replace("'", "''")
                ocu_sql = registro['ocupacion'].replace("'", "''")
                esc_sql = registro['nivel_estudios'].replace("'", "''")
                act_sql = 1 if registro['activo'] else 0
                
                linea_sql = f"INSERT INTO personas (id_persona, nombre, apellido, edad, genero, ciudad, estado, ocupacion, nivel_estudios, ingreso_mensual, anticuad_anos, activo) VALUES ({registro['id_persona']}, '{nom_sql}', '{ape_sql}', {registro['edad']}, '{registro['genero']}', '{ciu_sql}', '{est_sql}', '{ocu_sql}', '{esc_sql}', {registro['ingreso_mensual']}, {registro['antiguedad_anos']}, {act_sql});\n"
                f_sql.write(linea_sql)
                
                # Muestreo dinámico en consola para ver pasar las ráfagas en vivo (1 de cada 50)
                if total_recibidos % 50 == 0:
                    sys.stdout.write(f"\r📥 [Canal: {mensaje.topic}] Procesando Registro #{registro['id_persona']}: {registro['nombre']} ({registro['ocupacion']})")
                    sys.stdout.flush()
                
                if total_recibidos % 10000 == 0:
                    print(f"\n\n📈 [HITO] {total_recibidos} objetos validados y persistidos en el disco local.\n")
                    
                if total_recibidos >= 100000:
                    print("\n\n=========================================================")
                    print("🎯 ¡META COMPLETADA! 100,000 objetos guardados en JSON, CSV y SQL.")
                    print("=========================================================")
                    break

        except KeyboardInterrupt:
            print("\nConsumidor detenido voluntariamente.")
        finally:
            consumer.close()

if __name__ == "__main__":
    iniciar_consumidor()