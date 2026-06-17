import json
import time
import random
from kafka import KafkaProducer
from faker import Faker


BROKERS_CLUSTER = [
    "100.72.209.77:9092"    # Obed - Nodo 3
]

TOPICOS = [
    "datos-usuarios-zona1",
    "datos-usuarios-zona2",
    "datos-usuarios-zona3"
]

# Particiones cuyo líder es Obed según el describe:
# zona1 particion 2 -> Leader 3
# zona2 particion 1 -> Leader 3
# zona3 particion 2 -> Leader 3
PARTICIONES_FIJAS = {
    "datos-usuarios-zona1": 2,
    "datos-usuarios-zona2": 1,
    "datos-usuarios-zona3": 2
}

errores_envio = 0
ultimo_meta = {
    "topic": "",
    "partition": -1
}


def al_recibir_meta(metadata):
    global ultimo_meta

    ultimo_meta = {
        "topic": metadata.topic,
        "partition": metadata.partition
    }


def al_error_envio(error):
    global errores_envio

    errores_envio += 1
    print(f"\nError al enviar mensaje a Kafka: {error}")


def crear_registro(fake, id_persona):
    estados = [
        "Aguascalientes",
        "Jalisco",
        "Zacatecas",
        "Guanajuato",
        "Queretaro",
        "Nuevo Leon",
        "Puebla",
        "Yucatan",
        "CDMX",
        "Chihuahua"
    ]

    profesiones = [
        "Desarrollador",
        "Ingeniero",
        "Medico",
        "Administrador",
        "Contador",
        "Abogado",
        "Disenador",
        "Docente"
    ]

    estudios = [
        "Bachillerato",
        "Licenciatura",
        "Maestria",
        "Doctorado"
    ]

    genero = random.choice(["M", "F"])

    return {
        "id_persona": id_persona,
        "nombre": fake.first_name_female() if genero == "F" else fake.first_name_male(),
        "apellido": fake.last_name(),
        "edad": random.randint(18, 65),
        "genero": genero,
        "ciudad": fake.city(),
        "estado": random.choice(estados),
        "ocupacion": random.choice(profesiones),
        "nivel_estudios": random.choice(estudios),
        "ingreso_mensual": round(random.uniform(8000, 65000), 2),
        "antiguedad_anos": random.randint(1, 15),
        "activo": random.choice([True, False])
    }


def obtener_topico(conteo):
    if conteo % 3 == 0:
        return "datos-usuarios-zona1"
    elif conteo % 3 == 1:
        return "datos-usuarios-zona2"
    else:
        return "datos-usuarios-zona3"


def iniciar_productor():
    global errores_envio

    print("=========================================================")
    print("Iniciando Productor: GENERANDO LOS DATOS")
    print("=========================================================")

    fake = Faker("es_MX")

    try:
        print("Conectando con el cluster de Kafka por Tailscale en puerto 9092...")

        producer = KafkaProducer(
            bootstrap_servers=BROKERS_CLUSTER,
            security_protocol="PLAINTEXT",
            api_version=(3, 5, 0),

            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),

            acks=1,
            retries=3,
            retry_backoff_ms=1000,

            request_timeout_ms=60000,
            delivery_timeout_ms=90000,
            max_block_ms=60000,

            batch_size=8192,
            linger_ms=5,
            max_in_flight_requests_per_connection=1
        )

        print("Conexion exitosa con el cluster de Kafka.")

    except Exception as e:
        print(f"Error al conectar con el cluster: {e}")
        return

    total_registros = 100000
    tiempo_inicio = time.time()

    print(f"Generando {total_registros} registros distribuidos en 3 topicos...")

    try:
        for conteo in range(total_registros):
            id_persona = conteo + 1
            registro = crear_registro(fake, id_persona)

            topic = obtener_topico(conteo)
            partition = PARTICIONES_FIJAS[topic]

            future = producer.send(
                topic,
                value=registro,
                partition=partition
            )

            future.add_callback(al_recibir_meta)
            future.add_errback(al_error_envio)

            # Imprime más seguido para saber que sí está avanzando.
            if id_persona % 1000 == 0:
                producer.flush(timeout=60)
                print(
                    f"-> {id_persona} registros enviados. "
                    f"Ultimo topico: {ultimo_meta['topic']} | "
                    f"Particion: {ultimo_meta['partition']}"
                )

            # Pequeña pausa para no saturar Tailscale.
            if id_persona % 5000 == 0:
                time.sleep(0.2)

        print("\nLiberando flujo final de red...")

        producer.flush(timeout=120)
        print("Flush final completado correctamente.")

    except KeyboardInterrupt:
        print("\nProductor detenido manualmente.")

    except Exception as e:
        print(f"\nError durante la produccion de datos: {e}")

    finally:
        producer.close(timeout=10)

    tiempo_total = time.time() - tiempo_inicio

    print("=========================================================")

    if errores_envio > 0:
        print(f"Proceso terminado con {errores_envio} errores de envio.")
    else:
        print("Todos los mensajes fueron enviados sin errores reportados.")

    print(f"Tiempo total: {round(tiempo_total, 2)} segundos.")
    print("=========================================================")


if __name__ == "__main__":
    iniciar_productor()