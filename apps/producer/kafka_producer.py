import json
import time
import random
from kafka import KafkaProducer
from faker import Faker

ultimo_meta = {"topic": "", "partition": -1, "node_id": "Calculando..."}

def al_recibir_meta(metadata):
    global ultimo_meta
    node_id = getattr(metadata, 'node_id', 'Principal')
    ultimo_meta = {
        "topic": metadata.topic,
        "partition": metadata.partition,
        "node_id": node_id
    }

def iniciar_productor():
    print("=========================================================")
    print("Iniciando Productor: GENERACIÓN CON FAKER (5 TOPICS REALES)")
    print("=========================================================")

    fake = Faker('es_MX')

    BROKERS_CLUSTER = [
        '192.168.0.101:9092',  # Osvaldo
        '192.168.0.102:9092',  # Pamela
        '192.168.0.103:9092'   # Obed
    ]

    try:
        producer = KafkaProducer(
            bootstrap_servers=BROKERS_CLUSTER,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks='all',
            retries=5,
            max_block_ms=5000
        )
        print("¡Conexión exitosa con el clúster de Kafka!")
    except Exception as e:
        print(f"❌ Error al conectar con el clúster: {e}")
        return

    estados = ["Aguascalientes", "Jalisco", "Zacatecas", "Guanajuato", "Queretaro", "Nuevo Leon", "Puebla", "Yucatan", "CDMX", "Chihuahua"]
    profesiones = ["Desarrollador", "Ingeniero", "Médico", "Administrador", "Contador", "Abogado", "Diseñador", "Docente"]
    estudios = ["Bachillerato", "Licenciatura", "Maestría", "Doctorado"]

    print("Generando 100,000 registros únicos distribuidos equitativamente en 5 topics...")
    tiempo_inicio = time.time()

    for conteo in range(100000):
        genero_random = random.choice(["M", "F"])
        registro_faker = {
            "id_persona": conteo + 1,
            "nombre": fake.first_name_female() if genero_random == "F" else fake.first_name_male(),
            "apellido": fake.last_name(),
            "edad": random.randint(18, 65),
            "genero": genero_random,
            "ciudad": fake.city(),
            "estado": random.choice(estados),
            "ocupacion": random.choice(profesiones),
            "nivel_estudios": random.choice(estudios),
            "ingreso_mensual": random.randint(8000, 65000),
            "antiguedad_anos": random.randint(1, 15),
            "activo": random.choice([True, False])
        }
        
        # Reparto cíclico en los 5 tópicos obligatorios
        if conteo % 5 == 0:
            producer.send('personas-bloque-A', value=registro_faker).add_callback(al_recibir_meta)
        elif conteo % 5 == 1:
            producer.send('personas-bloque-B', value=registro_faker).add_callback(al_recibir_meta)
        elif conteo % 5 == 2:
            producer.send('personas-bloque-C', value=registro_faker).add_callback(al_recibir_meta)
        elif conteo % 5 == 3:
            producer.send('personas-bloque-D', value=registro_faker).add_callback(al_recibir_meta)
        else:
            producer.send('personas-bloque-E', value=registro_faker).add_callback(al_recibir_meta)

        if (conteo + 1) % 10000 == 0:
            print(f"-> {conteo + 1} registros enviados. [Último topic: {ultimo_meta['topic']} | Nodo Broker ID: {ultimo_meta['node_id']}]")

    print("\nVaciando buffers de red (Flush)...")
    producer.flush()
    producer.close()

    tiempo_total = time.time() - tiempo_inicio
    print("=========================================================")
    print(f"¡Éxito total! 100,000 registros inyectados en los 5 topics.")
    print(f"Tiempo de transmisión: {round(tiempo_total, 2)} segundos.")
    print("=========================================================")

if __name__ == "__main__":
    iniciar_productor()