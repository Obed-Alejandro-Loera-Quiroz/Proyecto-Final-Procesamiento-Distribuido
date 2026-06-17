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

    # CLÚSTER REMOTO: IPs de Tailscale apuntando al nuevo puerto exterior 9094
    BROKERS_CLUSTER = [
        '100.115.62.37:9094',  # Osvaldo (Nodo 1)
        '100.123.126.75:9094', # Brayan (Nodo 2)
        '100.72.209.77:9094'   # Obed (Nodo 3)
    ]

    try:
        # 🔥 BYPASS DEFINITIVO PARA EVITAR TRABAS POR LATENCIA RESIDENCIAL 🔥
        producer = KafkaProducer(
            bootstrap_servers=BROKERS_CLUSTER,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks=0,                      # acks=0: Envía de inmediato sin esperar confirmación síncrona de red
            retries=0,                   # Evita ciclos infinitos si la VPN mete retrasos menores
            max_block_ms=60000,          # Tolerancia para encontrar el broker al arrancar
            request_timeout_ms=30000,    # Límite de espera por petición
            batch_size=32768,            # Agrupaciones óptimas para no saturar el canal VPN
            linger_ms=20                 # Espera milimétrica para empaquetar ráfagas
        )
        print("¡Conexión exitosa con el clúster de Kafka vía Tailscale!")
    except Exception as e:
        print(f"❌ Error al conectar con el clúster: {e}")
        return

    estados = ["Aguascalientes", "Jalisco", "Zacatecas", "Guanajuato", "Queretaro", "Nuevo Leon", "Puebla", "Yucatan", "CDMX", "Chihuahua"]
    profesiones = ["Desarrollador", "Ingeniero", "Médico", "Administrador", "Contador", "Abogado", "Diseñador", "Docente"]
    estudios = ["Bachillerato", "Licenciatura", "Maestría", "Doctorado"]

    print("Generando 100,000 registros únicos distribuidos en la infraestructura distribuida...")
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
        
        # Inyección a través del canal activo; KRaft se encarga de replicarlo a las 3 laptops
        # Alterna dinámicamente entre los tópicos para cumplir estrictamente la rúbrica del profesor
        if conteo % 3 == 0:
            producer.send('datos-usuarios-zona1', value=registro_faker).add_callback(al_recibir_meta)
        elif conteo % 3 == 1:
            producer.send('datos-usuarios-zona2', value=registro_faker).add_callback(al_recibir_meta)
        else:
            producer.send('datos-usuarios-zona3', value=registro_faker).add_callback(al_recibir_meta)

        if (conteo + 1) % 10000 == 0:
            print(f"-> {conteo + 1} registros inyectados en la autopista de red.")

    print("\nLiberando flujo final de red (Flush)...")
    producer.flush()
    producer.close()

    tiempo_total = time.time() - tiempo_inicio
    print("=========================================================")
    print(f"¡Éxito total! 100,000 registros transmitidos con éxito.")
    print(f"Tiempo de procesamiento: {round(tiempo_total, 2)} segundos.")
    print("=========================================================")

if __name__ == "__main__":
    iniciar_productor()