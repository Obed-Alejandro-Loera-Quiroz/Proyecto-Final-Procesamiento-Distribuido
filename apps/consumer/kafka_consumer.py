import json
import os
from kafka import KafkaConsumer

def iniciar_consumidor():
    print("=========================================================")
    print("Iniciando Consumidor: CAPTURA MULTI-FORMATO DESDE 5 TOPICS")
    print("=========================================================")

    BROKERS_CLUSTER = [
        '192.168.0.101:9092',
        '192.168.0.102:9092',
        '192.168.0.103:9092'
    ]

    ruta_base = "/opt/spark/shared-data/"
    if not os.path.exists(ruta_base):
        ruta_base = ""

    ruta_json = os.path.join(ruta_base, "dataset.json")
    ruta_csv = os.path.join(ruta_base, "dataset_respaldo.csv")

    try:
        # Suscripción explícita a los 5 tópicos creados en bash
        consumer = KafkaConsumer(
            'personas-bloque-A',
            'personas-bloque-B',
            'personas-bloque-C',
            'personas-bloque-D',
            'personas-bloque-E',
            bootstrap_servers=BROKERS_CLUSTER,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='grupo-final-uaa',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("¡Conexión Exitosa! Escuchando los 5 topics en paralelo...")
        print(f"-> Formato JSON en: {ruta_json}")
        print(f"-> Formato CSV (10 columnas) en: {ruta_csv}\n")
    except Exception as e:
        print(f"❌ Error al conectar el consumidor: {e}")
        return

    total_recibidos = 0

    with open(ruta_json, 'w', encoding='utf-8') as f_json, open(ruta_csv, 'w', encoding='utf-8') as f_csv:
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
                    print(f"📥 [Clúster] {total_recibidos} registros Faker atrapados y respaldados en JSON y CSV.")
                    
                if total_recibidos >= 100000:
                    print("\n=========================================================")
                    print("🎯 ¡META COMPLETADA! 100,000 registros guardados desde los 5 topics.")
                    print("=========================================================")
                    break

        except KeyboardInterrupt:
            print("\nConsumidor detenido manualmente.")
        finally:
            consumer.close()

if __name__ == "__main__":
    iniciar_consumidor()