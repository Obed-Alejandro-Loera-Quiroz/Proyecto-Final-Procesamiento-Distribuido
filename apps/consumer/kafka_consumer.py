import csv
import json
import os
import sys
import time
from kafka import KafkaConsumer, TopicPartition


# Usamos Obed como broker inicial.
# Kafka obtiene la metadata de los demas nodos automaticamente.
BROKERS_CLUSTER = [
    "100.72.209.77:9092"    # Obed - Nodo 3
]

TOPICOS = [
    "datos-usuarios-zona1",
    "datos-usuarios-zona2",
    "datos-usuarios-zona3"
]


def obtener_rutas_archivos():
    ruta_base = os.environ.get("DATA_DIR")

    if not ruta_base:
        ruta_base = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "..", "shared-data")
        )

    os.makedirs(ruta_base, exist_ok=True)

    return (
        os.path.join(ruta_base, "dataset.json"),
        os.path.join(ruta_base, "dataset_respaldo.csv"),
        os.path.join(ruta_base, "dataset_inserts.sql")
    )


def limpiar_sql(valor):
    return str(valor).replace("'", "''")


def crear_consumer():
    return KafkaConsumer(
        bootstrap_servers=BROKERS_CLUSTER,
        security_protocol="PLAINTEXT",
        api_version=(3, 5, 0),

        value_deserializer=lambda x: json.loads(x.decode("utf-8")),

        # No usamos group_id para evitar problemas con offsets viejos.
        group_id=None,
        enable_auto_commit=False,
        auto_offset_reset="earliest",

        request_timeout_ms=120000,
        max_poll_records=2000,
        consumer_timeout_ms=10000
    )


def asignar_particiones(consumer):
    particiones_asignadas = []

    print("Buscando particiones de los topics...")

    for topico in TOPICOS:
        particiones = None

        for intento in range(10):
            particiones = consumer.partitions_for_topic(topico)

            if particiones:
                break

            print(f"Esperando metadata del topic {topico}... intento {intento + 1}/10")
            time.sleep(2)

        if not particiones:
            raise Exception(f"No se pudieron obtener particiones del topic: {topico}")

        for particion in particiones:
            particiones_asignadas.append(TopicPartition(topico, particion))

    consumer.assign(particiones_asignadas)
    consumer.seek_to_beginning(*particiones_asignadas)

    print("\nParticiones asignadas manualmente:")
    for tp in particiones_asignadas:
        print(f"-> {tp.topic} | particion {tp.partition}")

    return particiones_asignadas


def iniciar_consumidor():
    print("=========================================================")
    print("Iniciando Consumidor: CAPTURA MULTI-FORMATO EN TIEMPO REAL")
    print("=========================================================")

    ruta_json, ruta_csv, ruta_sql = obtener_rutas_archivos()

    print("Conectando con brokers Kafka por Tailscale en puerto 9092...")

    try:
        consumer = crear_consumer()
        asignar_particiones(consumer)

        print("\nConexion establecida con exito.")
        print(f"Guardando JSON en: {ruta_json}")
        print(f"Guardando CSV en:  {ruta_csv}")
        print(f"Guardando SQL en:  {ruta_sql}")
        print("\nEsperando datos desde Kafka...\n")

    except Exception as e:
        print(f"Error al iniciar el consumidor: {e}")
        return

    total_recibidos = 0

    campos = [
        "id_persona",
        "nombre",
        "apellido",
        "edad",
        "genero",
        "ciudad",
        "estado",
        "ocupacion",
        "nivel_estudios",
        "ingreso_mensual",
        "antiguedad_anos",
        "activo"
    ]

    try:
        with open(ruta_json, "w", encoding="utf-8") as f_json, \
             open(ruta_csv, "w", encoding="utf-8", newline="") as f_csv, \
             open(ruta_sql, "w", encoding="utf-8") as f_sql:

            escritor_csv = csv.DictWriter(f_csv, fieldnames=campos)
            escritor_csv.writeheader()

            f_sql.write("-- =========================================================\n")
            f_sql.write("-- DATASET GENERADO AUTOMATICAMENTE POR KAFKA\n")
            f_sql.write("-- PROYECTO FINAL DE PROCESAMIENTO DISTRIBUIDO\n")
            f_sql.write("-- =========================================================\n\n")

            f_sql.write("DROP TABLE IF EXISTS personas;\n\n")

            f_sql.write("""
CREATE TABLE personas (
    id_persona INT PRIMARY KEY,
    nombre VARCHAR(100),
    apellido VARCHAR(100),
    edad INT,
    genero VARCHAR(10),
    ciudad VARCHAR(100),
    estado VARCHAR(100),
    ocupacion VARCHAR(100),
    nivel_estudios VARCHAR(100),
    ingreso_mensual DECIMAL(10,2),
    antiguedad_anos INT,
    activo BOOLEAN
);

""")

            while total_recibidos < 100000:
                mensajes = consumer.poll(timeout_ms=3000, max_records=2000)

                if not mensajes:
                    print("Sin mensajes nuevos por ahora, esperando...")
                    continue

                for tp, lista_mensajes in mensajes.items():
                    for mensaje in lista_mensajes:
                        registro = mensaje.value
                        total_recibidos += 1

                        f_json.write(json.dumps(registro, ensure_ascii=False) + "\n")

                        escritor_csv.writerow({
                            "id_persona": registro["id_persona"],
                            "nombre": registro["nombre"],
                            "apellido": registro["apellido"],
                            "edad": registro["edad"],
                            "genero": registro["genero"],
                            "ciudad": registro["ciudad"],
                            "estado": registro["estado"],
                            "ocupacion": registro["ocupacion"],
                            "nivel_estudios": registro["nivel_estudios"],
                            "ingreso_mensual": registro["ingreso_mensual"],
                            "antiguedad_anos": registro["antiguedad_anos"],
                            "activo": registro["activo"]
                        })

                        activo_sql = 1 if registro["activo"] else 0

                        linea_sql = (
                            "INSERT INTO personas "
                            "(id_persona, nombre, apellido, edad, genero, ciudad, estado, "
                            "ocupacion, nivel_estudios, ingreso_mensual, antiguedad_anos, activo) "
                            f"VALUES ("
                            f"{registro['id_persona']}, "
                            f"'{limpiar_sql(registro['nombre'])}', "
                            f"'{limpiar_sql(registro['apellido'])}', "
                            f"{registro['edad']}, "
                            f"'{limpiar_sql(registro['genero'])}', "
                            f"'{limpiar_sql(registro['ciudad'])}', "
                            f"'{limpiar_sql(registro['estado'])}', "
                            f"'{limpiar_sql(registro['ocupacion'])}', "
                            f"'{limpiar_sql(registro['nivel_estudios'])}', "
                            f"{registro['ingreso_mensual']}, "
                            f"{registro['antiguedad_anos']}, "
                            f"{activo_sql}"
                            f");\n"
                        )

                        f_sql.write(linea_sql)

                        if total_recibidos % 50 == 0:
                            sys.stdout.write(
                                f"\rCanal: {mensaje.topic} | "
                                f"Particion: {mensaje.partition} | "
                                f"Registro #{registro['id_persona']} | "
                                f"{registro['nombre']} | "
                                f"Total: {total_recibidos}"
                            )
                            sys.stdout.flush()

                        if total_recibidos % 10000 == 0:
                            f_json.flush()
                            f_csv.flush()
                            f_sql.flush()
                            print(f"\n\nHITO: {total_recibidos} registros guardados correctamente.\n")

                        if total_recibidos >= 100000:
                            break

                    if total_recibidos >= 100000:
                        break

            f_json.flush()
            f_csv.flush()
            f_sql.flush()

            print("\n\n=========================================================")
            print("META COMPLETADA: 100,000 registros guardados en JSON, CSV y SQL.")
            print("=========================================================")

    except KeyboardInterrupt:
        print("\nConsumidor detenido manualmente.")

    except Exception as e:
        print(f"\nError durante el consumo de datos: {e}")

    finally:
        consumer.close()
        print("Consumidor cerrado correctamente.")


if __name__ == "__main__":
    iniciar_consumidor()