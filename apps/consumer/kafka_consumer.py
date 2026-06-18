import csv
import json
import os
import sys
import getpass
from kafka import KafkaConsumer, TopicPartition


BROKERS_CLUSTER = [
    "100.123.126.75:9092"  # Pamila - Nodo 2
]

# Particiones actuales con Leader 2:
# zona1 -> particion 1
# zona2 -> particion 0
# zona3 -> particion 1
PARTICIONES_LECTURA = [
    TopicPartition("datos-usuarios-zona1", 1),
    TopicPartition("datos-usuarios-zona2", 0),
    TopicPartition("datos-usuarios-zona3", 1)
]

DEMO_ID = os.environ.get("DEMO_ID", "presentacion_1")


def obtener_nombre_consumidor():
    if len(sys.argv) > 1:
        return sys.argv[1].strip().lower()

    return os.environ.get("CONSUMER_NAME", getpass.getuser()).strip().lower()


NOMBRE_CONSUMIDOR = obtener_nombre_consumidor()


CAMPOS = [
    "id_persona",
    "demo_id",
    "topic_destino",
    "zona",
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
    "activo",
    "fecha_registro"
]


def obtener_rutas_archivos():
    ruta_base = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..", "shared-data", "demo")
    )

    os.makedirs(ruta_base, exist_ok=True)

    ruta_json = os.path.join(ruta_base, f"dataset_demo_{DEMO_ID}_{NOMBRE_CONSUMIDOR}.json")
    ruta_csv = os.path.join(ruta_base, f"dataset_demo_{DEMO_ID}_{NOMBRE_CONSUMIDOR}.csv")
    ruta_sql = os.path.join(ruta_base, f"dataset_demo_{DEMO_ID}_{NOMBRE_CONSUMIDOR}.sql")
    ruta_offsets = os.path.join(ruta_base, f"offsets_{DEMO_ID}_{NOMBRE_CONSUMIDOR}.json")

    return ruta_json, ruta_csv, ruta_sql, ruta_offsets


def clave_offset(tp):
    return f"{tp.topic}-{tp.partition}"


def cargar_offsets(ruta_offsets):
    if not os.path.exists(ruta_offsets):
        return {}

    try:
        with open(ruta_offsets, "r", encoding="utf-8") as archivo:
            return json.load(archivo)
    except Exception:
        return {}


def guardar_offsets(ruta_offsets, offsets):
    with open(ruta_offsets, "w", encoding="utf-8") as archivo:
        json.dump(offsets, archivo, indent=4)


def limpiar_sql(valor):
    return str(valor).replace("'", "''")


def archivo_vacio_o_no_existe(ruta):
    return not os.path.exists(ruta) or os.path.getsize(ruta) == 0


def normalizar_registro(registro, topic):
    return {
        "id_persona": registro.get("id_persona", 0),
        "demo_id": registro.get("demo_id", DEMO_ID),
        "topic_destino": registro.get("topic_destino", topic),
        "zona": registro.get("zona", topic),
        "nombre": registro.get("nombre", ""),
        "apellido": registro.get("apellido", ""),
        "edad": registro.get("edad", 0),
        "genero": registro.get("genero", ""),
        "ciudad": registro.get("ciudad", ""),
        "estado": registro.get("estado", ""),
        "ocupacion": registro.get("ocupacion", ""),
        "nivel_estudios": registro.get("nivel_estudios", ""),
        "ingreso_mensual": registro.get("ingreso_mensual", 0),
        "antiguedad_anos": registro.get("antiguedad_anos", 0),
        "activo": registro.get("activo", False),
        "fecha_registro": registro.get("fecha_registro", "")
    }


def crear_consumer():
    return KafkaConsumer(
        bootstrap_servers=BROKERS_CLUSTER,
        security_protocol="PLAINTEXT",
        api_version=(3, 5, 0),

        value_deserializer=lambda x: json.loads(x.decode("utf-8")),

        # No usamos group_id porque estaba dando timeout en el cluster por Tailscale.
        # La reconexion se controla con archivo local de offsets.
        group_id=None,

        auto_offset_reset="latest",
        enable_auto_commit=False,

        request_timeout_ms=120000,
        max_poll_records=500
    )


def guardar_sql(f_sql, registro):
    activo_sql = 1 if registro["activo"] else 0

    linea_sql = (
        "INSERT INTO personas_demo "
        "(id_persona, demo_id, topic_destino, zona, nombre, apellido, edad, genero, ciudad, estado, "
        "ocupacion, nivel_estudios, ingreso_mensual, antiguedad_anos, activo, fecha_registro) "
        f"VALUES ("
        f"{registro['id_persona']}, "
        f"'{limpiar_sql(registro['demo_id'])}', "
        f"'{limpiar_sql(registro['topic_destino'])}', "
        f"'{limpiar_sql(registro['zona'])}', "
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
        f"{activo_sql}, "
        f"'{limpiar_sql(registro['fecha_registro'])}'"
        f");\n"
    )

    f_sql.write(linea_sql)


def iniciar_consumidor():
    print("=========================================================")
    print("CONSUMIDOR KAFKA - DEMO DISTRIBUIDA")
    print("=========================================================")
    print(f"Consumidor: {NOMBRE_CONSUMIDOR}")
    print(f"Demo ID: {DEMO_ID}")
    print("Modo: reconexion con offsets locales")
    print("Broker inicial: Pamila - Nodo 2")
    print("Conectando con Kafka por Tailscale en puerto 9092...")

    ruta_json, ruta_csv, ruta_sql, ruta_offsets = obtener_rutas_archivos()
    offsets = cargar_offsets(ruta_offsets)

    try:
        consumer = crear_consumer()
        consumer.assign(PARTICIONES_LECTURA)

        print("\nParticiones asignadas:")
        for tp in PARTICIONES_LECTURA:
            key = clave_offset(tp)

            if key in offsets:
                consumer.seek(tp, int(offsets[key]))
                print(f"-> {tp.topic} | particion {tp.partition} | retomando desde offset {offsets[key]}")
            else:
                print(f"-> {tp.topic} | particion {tp.partition} | esperando mensajes nuevos")

        print("\nConexion establecida con exito.")
        print("Si se desconecta, al volver usara el archivo de offsets.")
        print(f"Archivo de offsets: {ruta_offsets}")
        print(f"Guardando JSON en: {ruta_json}")
        print(f"Guardando CSV en:  {ruta_csv}")
        print(f"Guardando SQL en:  {ruta_sql}")
        print("\nEsperando mensajes...\n")

    except Exception as e:
        print(f"Error al iniciar el consumidor: {e}")
        return

    total_recibidos = 0

    try:
        escribir_header_csv = archivo_vacio_o_no_existe(ruta_csv)
        escribir_header_sql = archivo_vacio_o_no_existe(ruta_sql)

        with open(ruta_json, "a", encoding="utf-8") as f_json, \
             open(ruta_csv, "a", encoding="utf-8", newline="") as f_csv, \
             open(ruta_sql, "a", encoding="utf-8") as f_sql:

            escritor_csv = csv.DictWriter(f_csv, fieldnames=CAMPOS)

            if escribir_header_csv:
                escritor_csv.writeheader()

            if escribir_header_sql:
                f_sql.write("-- =========================================================\n")
                f_sql.write("-- DATASET DEMO GENERADO POR APACHE KAFKA\n")
                f_sql.write("-- PROYECTO FINAL DE PROCESAMIENTO DISTRIBUIDO\n")
                f_sql.write("-- =========================================================\n\n")
                f_sql.write("""
CREATE TABLE IF NOT EXISTS personas_demo (
    id_persona INT,
    demo_id VARCHAR(100),
    topic_destino VARCHAR(100),
    zona VARCHAR(100),
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
    activo BOOLEAN,
    fecha_registro VARCHAR(30)
);

""")

            while True:
                mensajes = consumer.poll(timeout_ms=3000, max_records=500)

                if not mensajes:
                    print("Sin mensajes nuevos por ahora, esperando...")
                    continue

                for tp, lista_mensajes in mensajes.items():
                    for mensaje in lista_mensajes:
                        registro = normalizar_registro(mensaje.value, mensaje.topic)
                        total_recibidos += 1

                        f_json.write(json.dumps(registro, ensure_ascii=False) + "\n")
                        escritor_csv.writerow(registro)
                        guardar_sql(f_sql, registro)

                        offsets[clave_offset(tp)] = mensaje.offset + 1

                        if total_recibidos % 50 == 0:
                            f_json.flush()
                            f_csv.flush()
                            f_sql.flush()
                            guardar_offsets(ruta_offsets, offsets)

                            print(
                                f"{NOMBRE_CONSUMIDOR} recibio {total_recibidos} mensajes en esta sesion | "
                                f"Topic: {mensaje.topic} | "
                                f"Particion: {mensaje.partition} | "
                                f"Offset: {mensaje.offset} | "
                                f"Registro: {registro['id_persona']}"
                            )

    except KeyboardInterrupt:
        print("\nConsumidor detenido manualmente.")
        print("Guardando offsets antes de cerrar...")

        try:
            guardar_offsets(ruta_offsets, offsets)
            print("Offsets guardados correctamente.")
        except Exception as e:
            print(f"No se pudieron guardar offsets: {e}")

    except Exception as e:
        print(f"\nError durante el consumo de datos: {e}")

    finally:
        consumer.close()
        print("Consumidor cerrado correctamente.")


if __name__ == "__main__":
    iniciar_consumidor()