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
    print("Iniciando Productor: GENERANDO LOS DATOS")
    print("=========================================================")

    fake = Faker('es_MX')

    # CLÚSTER REMOTO: IPs fijas asignadas por Tailscale para el proyecto
    BROKERS_CLUSTER = [
        '100.115.62.37:9092',  # Osvaldo (Nodo 1)
        '100.123.126.75:9092', # Brayan (Nodo 2)
        '100.72.209.77:9092'   # Obed (Nodo 3 - Tu máquina)
    ]

    try:
        # 🔥 OPTIMIZADO PARA ENTORNOS REMOTOS / TAILSCALE 🔥
        producer = KafkaProducer(
            bootstrap_servers=BROKERS_CLUSTER,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks='all',
            retries=5,
            max_block_ms=60000,         # Aumentado a 60 seg para dar tiempo a la sincronización inicial
            request_timeout_ms=30000,    # 30 seg de tolerancia para confirmaciones de red
            batch_size=16384,            # Agrupar registros en bloques pequeños antes de enviar
            linger_ms=10                 # Esperar 10ms para juntar ráfagas y no saturar el canal VPN
        )
        print("¡Conexión exitosa con el clúster de Kafka vía Tailscale!")
    except Exception as e:
        print(f"❌ Error al conectar con el clúster: {e}")
        return

    estados = ["Aguascalientes", "Jalisco", "Zacatecas", "Guanajuato", "Queretaro", "Nuevo Leon", "Puebla", "Yucatan", "CDMX", "Chihuahua"]
    profesiones = ["Desarrollador", "Ingeniero", "Médico", "Administrador", "Contador", "Abogado", "Diseñador", "Docente"]
    estudios = ["Bachillerato", "Licenciatura", "Maestría", "Doctorado"]

    print("Generando 100,000 registros únicos distribuidos equitativamente en 3 topics de zonas...")
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
        
        # Reparto cíclico en los 3 tópicos requeridos por el profesor
        if conteo % 3 == 0:
            producer.send('datos-usuarios-zona1', value=registro_faker).add_callback(al_recibir_meta)
        elif conteo % 3 == 1:
            producer.send('datos-usuarios-zona2', value=registro_faker).add_callback(al_recibir_meta)
        else:
            producer.send('datos-usuarios-zona3', value=registro_faker).add_callback(al_recibir_meta)

        if (conteo + 1) % 10000 == 0:
            print(f"-> {conteo + 1} registros enviados. [Último topic: {ultimo_meta['topic']} | Nodo Broker ID: {ultimo_meta['node_id']}]")

    print("\nVaciando buffers de red (Flush)...")
    producer.flush()
    producer.close()

    tiempo_total = time.time() - tiempo_inicio
    print("=========================================================")
    print(f"¡Éxito total! 100,000 registros inyectados en los 3 topics.")
    print(f"Tiempo de transmisión: {round(tiempo_total, 2)} segundos.")
    print("=========================================================")

if __name__ == "__main__":
    iniciar_productor()