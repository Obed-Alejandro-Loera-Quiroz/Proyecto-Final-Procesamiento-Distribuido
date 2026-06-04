import json
import time
import os
from kafka import KafkaProducer

# Variable global para monitorear dinámicamente los metadatos del clúster
ultimo_meta = {"topic": "", "partition": -1, "node_id": "Calculando..."}

def al_recibir_meta(metadata):
    """ Función que captura los metadatos directamente devueltos por el módem """
    global ultimo_meta
    # Extraemos el ID del broker/nodo que aceptó el mensaje físicamente en su disco duro
    node_id = getattr(metadata, 'node_id', 'Principal')
    ultimo_meta = {
        "topic": metadata.topic,
        "partition": metadata.partition,
        "node_id": node_id
    }

def iniciar_productor():
    print("=========================================================")
    print("Iniciando Productor de Kafka Distribuidor con Monitoreo...")
    print("=========================================================")

    # IPs de todo el equipo en el módem TP-Link
    BROKERS_CLUSTER = [
        '192.168.0.101:9092',  # Osvaldo (Nodo 1)
        '192.168.0.102:9092',  # Brayan/Pamela (Nodo 2)
        '192.168.0.103:9092'   # Tú - Obed (Nodo 3)
    ]

    try:
        # Inicializamos el productor conectado a las 3 laptops
        producer = KafkaProducer(
            bootstrap_servers=BROKERS_CLUSTER,
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            acks='all',  # Garantiza réplicas en las 3 laps para tolerancia a fallos
            retries=5,
            max_block_ms=5000  # Evita que se congele eternamente si la red falla
        )
        print("¡Conexión exitosa con el clúster de Kafka!")
    except Exception as e:
        print(f"❌ Error al conectar con el clúster: {e}")
        return

    # Buscamos el dataset de 100,000 registros en la carpeta compartida
    ruta_dataset = os.path.join("..", "..", "shared-data", "dataset.json")
    
    if not os.path.exists(ruta_dataset):
        print(f"❌ Error: No se encontró el archivo en {ruta_dataset}")
        return

    print("Enviando ráfagas de datos a los 5 tópicos distribuidos...")
    conteo = 0
    tiempo_inicio = time.time()

    with open(ruta_dataset, 'r', encoding='utf-8') as f:
        for linea in f:
            if not linea.strip():
                continue
                
            registro = json.loads(linea.strip())
            
            # -----------------------------------------------------------------
            # CLASIFICACIÓN Y MONITOREO EN TIEMPO REAL
            # -----------------------------------------------------------------
            
            # Al agregar .add_callback(al_recibir_meta), interceptamos a qué nodo viaja el dato
            producer.send('personas-registro', value=registro).add_callback(al_recibir_meta)
            
            if registro.get("activo") == True:
                producer.send('personas-activas', value=registro)
                
            if registro.get("ingreso_mensual", 0) > 25000:
                producer.send('personas-ingresos', value=registro)
                
            if registro.get("estado") == "Aguascalientes":
                producer.send('personas-geografia', value=registro)
                
            if registro.get("ocupacion") in ["Ingeniero", "Desarrollador", "Médico"]:
                producer.send('personas-metricas', value=registro)

            conteo += 1
            # Cada 10,000 registros mostramos el estado de la red del clúster
            if conteo % 10000 == 0:
                print(f"-> {conteo} registros enviados. [Último lote -> Partición: {ultimo_meta['partition']} | Enrutado al Broker ID: {ultimo_meta['node_id']}]")

    # Forzamos la salida de datos de la RAM hacia la red
    print("\nVaciando buffers de red (Flush)...")
    producer.flush()
    producer.close()

    tiempo_total = time.time() - tiempo_inicio
    print("=========================================================")
    print(f"¡Éxito total! Se transmitieron los 100,000 registros.")
    print(f"Tiempo de transmisión distribuida: {round(tiempo_total, 2)} segundos.")
    print("=========================================================")

if __name__ == "__main__":
    iniciar_productor()