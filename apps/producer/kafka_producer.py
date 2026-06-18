import json
import time
import random
import os
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker


BROKERS_CLUSTER = [
    "100.123.126.75:9092"  # Pamila - Nodo 2
]

# Particiones actuales con Leader 2:
# zona1 -> particion 1
# zona2 -> particion 0
# zona3 -> particion 1
TOPICOS_DESTINO = [
    {
        "topic": "datos-usuarios-zona1",
        "partition": 1,
        "zona": "zona1"
    },
    {
        "topic": "datos-usuarios-zona2",
        "partition": 0,
        "zona": "zona2"
    },
    {
        "topic": "datos-usuarios-zona3",
        "partition": 1,
        "zona": "zona3"
    }
]

DEMO_ID = os.environ.get("DEMO_ID", "presentacion_1")
TOTAL_REGISTROS = int(os.environ.get("TOTAL_REGISTROS", "100000"))

# Pausa para que se pueda ver la desconexion/reconexion en clase.
PAUSA_CADA = int(os.environ.get("PAUSA_CADA", "100"))
SEGUNDOS_PAUSA = float(os.environ.get("SEGUNDOS_PAUSA", "0.10"))

errores_envio = 0

conteo_por_topico = {
    "datos-usuarios-zona1": 0,
    "datos-usuarios-zona2": 0,
    "datos-usuarios-zona3": 0
}


def al_error_envio(error):
    global errores_envio

    errores_envio += 1
    print(f"\nError al enviar mensaje a Kafka: {error}")


def seleccionar_destino(conteo):
    return TOPICOS_DESTINO[conteo % len(TOPICOS_DESTINO)]


def crear_registro(fake, id_persona, destino):
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
        "demo_id": DEMO_ID,
        "topic_destino": destino["topic"],
        "zona": destino["zona"],
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
        "activo": random.choice([True, False]),
        "fecha_registro": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }


def crear_productor():
    return KafkaProducer(
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


def iniciar_productor():
    global errores_envio

    print("=========================================================")
    print("PRODUCTOR KAFKA - GENERACION DISTRIBUIDA")
    print("=========================================================")
    print(f"Demo ID: {DEMO_ID}")
    print(f"Total de registros: {TOTAL_REGISTROS}")
    print("Broker inicial: Pamila - Nodo 2")
    print("Conectando con Kafka por Tailscale en puerto 9092...")

    fake = Faker("es_MX")
    producer = None
    tiempo_inicio = time.time()

    try:
        producer = crear_productor()

        print("Conexion exitosa con el cluster de Kafka.")
        print("Enviando mensajes...\n")

        for conteo in range(TOTAL_REGISTROS):
            id_persona = conteo + 1
            destino = seleccionar_destino(conteo)
            registro = crear_registro(fake, id_persona, destino)

            future = producer.send(
                destino["topic"],
                value=registro,
                partition=destino["partition"]
            )

            future.add_errback(al_error_envio)
            conteo_por_topico[destino["topic"]] += 1

            if id_persona % PAUSA_CADA == 0:
                time.sleep(SEGUNDOS_PAUSA)

            if id_persona % 1000 == 0:
                producer.flush(timeout=60)

                print(
                    f"-> {id_persona} registros enviados | "
                    f"zona1: {conteo_por_topico['datos-usuarios-zona1']} | "
                    f"zona2: {conteo_por_topico['datos-usuarios-zona2']} | "
                    f"zona3: {conteo_por_topico['datos-usuarios-zona3']}"
                )

        print("\nLiberando flujo final de red...")
        producer.flush(timeout=120)
        print("Flush final completado correctamente.")

    except KeyboardInterrupt:
        print("\nProductor detenido manualmente.")

        if producer is not None:
            try:
                print("Enviando mensajes pendientes antes de cerrar...")
                producer.flush(timeout=60)
                print("Mensajes pendientes liberados.")
            except Exception as e:
                print(f"No se pudieron liberar todos los mensajes pendientes: {e}")

    except Exception as e:
        print(f"\nError durante la produccion de datos: {e}")

    finally:
        if producer is not None:
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