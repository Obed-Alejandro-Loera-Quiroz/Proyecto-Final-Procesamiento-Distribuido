import json
import time
import random
from kafka import KafkaProducer
from faker import Faker


BROKERS_CLUSTER = [
    "100.115.62.37:9094",   # Osvaldo - Nodo 1
    "100.123.126.75:9094",  # Pamila - Nodo 2
    "100.72.209.77:9094"    # Obed - Nodo 3
]

TOPICOS = [
    "datos-usuarios-zona1",
    "datos-usuarios-zona2",
    "datos-usuarios-zona3"
]

ultimo_meta = {
    "topic": "",
    "partition": -1
}

errores_envio = 0


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


def iniciar_productor():
    global errores_envio

    print("=========================================================")
    print("Iniciando Productor: GENERANDO LOS DATOS")
    print("=========================================================")

    fake = Faker("es_MX")

    try:
        print("Conectando con el cluster de Kafka por Tailscale...")

        producer = KafkaProducer(
            bootstrap_servers=BROKERS_CLUSTER,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),

            # Para pruebas estables por Tailscale.
            # acks=1 confirma que el lider del topico recibio el mensaje.
            acks=1,

            retries=5,
            retry_backoff_ms=1000,
            max_block_ms=60000,
            request_timeout_ms=60000,
            batch_size=32768,
            linger_ms=20
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

            if conteo % 3 == 0:
                topic = "datos-usuarios-zona1"
            elif conteo % 3 == 1:
                topic = "datos-usuarios-zona2"
            else:
                topic = "datos-usuarios-zona3"

            future = producer.send(topic, value=registro)
            future.add_callback(al_recibir_meta)
            future.add_errback(al_error_envio)

            if id_persona % 10000 == 0:
                try:
                    producer.flush(timeout=60)
                    print(
                        f"-> {id_persona} registros confirmados en Kafka. "
                        f"Ultimo topico: {ultimo_meta['topic']} | "
                        f"Particion: {ultimo_meta['partition']}"
                    )
                except Exception as e:
                    print(f"\nError durante flush parcial en registro {id_persona}: {e}")
                    break

        print("\nLiberando flujo final de red...")

        try:
            producer.flush(timeout=120)
            print("Flush final completado correctamente.")
        except Exception as e:
            print(f"Error durante el flush final: {e}")

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