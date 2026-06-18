import csv
import json
import os
import sys
import getpass
from kafka import KafkaConsumer


BROKERS_CLUSTER = [
    "100.115.62.37:9092",  # Osvaldo - Nodo 1
    "100.123.126.75:9092", # Pamila - Nodo 2
    "100.72.209.77:9092"   # Obed - Nodo 3
]

TOPICOS = [
    "datos-usuarios-zona1",
    "datos-usuarios-zona2",
    "datos-usuarios-zona3",
    "datos-usuarios-zona4",
    "datos-usuarios-zona5"
]

DEMO_ID = os.environ.get("DEMO_ID", "presentacion_1")


def obtener_nombre_consumidor():
    if len(sys.argv) > 1:
        return sys.argv[1].strip().lower()

    return os.environ.get("CONSUMER_NAME", getpass.getuser()).strip().lower()


NOMBRE_CONSUMIDOR = obtener_nombre_consumidor()

# Cada laptop tiene su propio grupo.
# Así Obed, Pamila y Osvaldo reciben todos los datos.
# Si uno se desconecta y vuelve con el mismo DEMO_ID y nombre, Kafka continúa desde donde iba.
GROUP_ID = f"grupo-{DEMO_ID}-{NOMBRE_CONSUMIDOR}"


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

    ruta_json = os.path.join(
        ruta_base,
        f"dataset_demo_{DEMO_ID}_{NOMBRE_CONSUMIDOR}.json"
    )

    ruta_csv = os.path.join(
        ruta_base,
        f"dataset_demo_{DEMO_ID}_{NOMBRE_CONSUMIDOR}.csv"
    )

    ruta_sql = os.path.join(
        ruta_base,
        f"dataset_demo_{DEMO_ID}_{NOMBRE_CONSUMIDOR}.sql"
    )

    return ruta_json, ruta_csv, ruta_sql


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
        *TOPICOS,

        bootstrap_servers=BROKERS_CLUSTER,
        security_protocol="PLAINTEXT",
        api_version=(3, 5, 0),

        value_deserializer=lambda x: json.loads(x.decode("utf-8")),

        group_id=GROUP_ID,

        # Si el grupo es nuevo, espera mensajes nuevos.
        # Si el grupo ya existe, Kafka continúa desde el último offset confirmado.
        auto_offset_reset="latest",

        # Confirmamos manualmente después de guardar en archivos.
        enable_auto_commit=False,

        request_timeout_ms=120000,
        session_timeout_ms=45000,
        heartbeat_interval_ms=15000,
        max_poll_records=500
    )


def escribir_encabezado_sql(f_sql):
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
    print(f"Group ID: {GROUP_ID}")
    print("Brokers iniciales:")
    for broker in BROKERS_CLUSTER:
        print(f"-> {broker}")

    print("\nTopicos:")
    for topico in TOPICOS:
        print(f"-> {topico}")

    print("\nConectando con Kafka por Tailscale...")

    ruta_json, ruta_csv, ruta_sql = obtener_rutas_archivos()

    try:
        consumer = crear_consumer()

        print("\nConexion establecida con exito.")
        print("Si este consumidor se desconecta, Kafka conserva los mensajes pendientes.")
        print("Al reconectarse con el mismo DEMO_ID y nombre, continua desde donde iba.")
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
                escribir_encabezado_sql(f_sql)

            for mensaje in consumer:
                registro = normalizar_registro(mensaje.value, mensaje.topic)
                total_recibidos += 1

                f_json.write(json.dumps(registro, ensure_ascii=False) + "\n")
                escritor_csv.writerow(registro)
                guardar_sql(f_sql, registro)

                if total_recibidos % 50 == 0:
                    f_json.flush()
                    f_csv.flush()
                    f_sql.flush()

                    try:
                        consumer.commit()
                        estado_commit = "offset guardado"
                    except Exception as e:
                        estado_commit = f"no se pudo guardar offset: {e}"

                    print(
                        f"{NOMBRE_CONSUMIDOR} recibio {total_recibidos} mensajes en esta sesion | "
                        f"Topic: {mensaje.topic} | "
                        f"Particion: {mensaje.partition} | "
                        f"Offset: {mensaje.offset} | "
                        f"Registro: {registro['id_persona']} | "
                        f"{estado_commit}"
                    )

    except KeyboardInterrupt:
        print("\nConsumidor detenido manualmente.")
        print("Guardando ultimo offset antes de cerrar...")

        try:
            consumer.commit()
            print("Offset guardado correctamente.")
        except Exception as e:
            print(f"No se pudo guardar el offset final: {e}")

    except Exception as e:
        print(f"\nError durante el consumo de datos: {e}")

    finally:
        consumer.close()
        print("Consumidor cerrado correctamente.")


if __name__ == "__main__":
    iniciar_consumidor()