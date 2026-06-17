import json
import os
from kafka import KafkaConsumer

def iniciar_consumidor():
    print("=========================================================")
    print("Iniciando Consumidor: CAPTURA MULTI-FORMATO DESDE 3 ZONAS")
    print("=========================================================")

    # CLÚSTER REMOTO: Mismas IPs de Tailscale
    BROKERS_CLUSTER = [
        ':9092',
        ':9092',
        '100.72.209.77:9092'
    ]

    ruta_base = "/opt/spark/shared-data/"
    if not os.path.exists(ruta_base):
        ruta_base = ""

    ruta_json = os.path.join(ruta_base, "dataset.json")
    ruta_csv = os.path.join(ruta_base, "dataset_respaldo.csv")

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
        print(f"-> Formato CSV (12 columnas) en: {ruta_csv}\n")
    except Exception as e:
        print(f"❌ Error al conectar el consumidor: {e}")
        return

    total_recibidos = 0

    with open(ruta_json, 'w', encoding='utf-8') as f_json, open(ruta_csv, 'w', encoding='utf-8') as f_csv:
        # Cabecera con las 12 columnas generadas por el productor
        f_csv.write("id_persona,nombre,apellido,edad,genero,ciudad,estado,ocupacion,nivel_estudios,ingreso_mensual,antiguedad_anos,activo\n")
        
        try:
            for mensaje in consumer:
                registro = mensaje.value
                
                # 1. Formato JSON
                f_json.write(json.dumps(registro, ensure_ascii=False) + "\n")
                
                # 2. Formato CSV
                ciudad_limpia = registro['ciudad'].replace(",", "")
                linea_csv = f"{registro['id_persona']},{registro['nombre']},{registro['apellido']},{registro['edad']},{registro['genero']},{ciudad_limpia},{registro['estado']},{registro['ocupacion']},{registro['nivel_estudios']},{registro['ingreso_mensual']},{registro['antiguedad_anos']},{registro['activo']}\n"
                f_csv.write(linea_csv)
                
                total_recibidos += 1
                
                if total_recibidos % 10000 == 0:
                    print(f" [Clúster Tailscale] {total_recibidos} registros Faker atrapados y respaldados en JSON y CSV.")
                    
                if total_recibidos >= 100000:
                    print("\n=========================================================")
                    print(" META COMPLETADA! 100,000 registros guardados desde los 3 topics.")
                    print("=========================================================")
                    break

        except KeyboardInterrupt:
            print("\nConsumidor detenido manualmente.")
        finally:
            consumer.close()

if __name__ == "__main__":
    iniciar_consumidor()