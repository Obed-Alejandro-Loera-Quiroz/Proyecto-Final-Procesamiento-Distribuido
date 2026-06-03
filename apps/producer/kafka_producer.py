import json
import time
import os
from kafka import KafkaProducer

def iniciar_productor():
    print("=========================================================")
    print("Iniciando Productor de Kafka Distribuidor...")
    print("=========================================================")

    # IPs de todo el equipo en el módem TP-Link para mañana
    BROKERS_CLUSTER = [
        '192.168.0.101:9092',  # Osvaldo (Nodo 1)
        '192.168.0.102:9092',  # Brayan/Pamela (Nodo 2)
        '192.168.0.103:9092'   # Tú - Obed (Nodo 3)
    ]

    try:
        # Inicializamos el productor conectado a las 3 laptops
        producer = KafkaProducer(
            bootstrap_servers=BROKERS_CLUSTER,
            # Transforma los diccionarios de Python a texto JSON limpio y luego a bytes
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
            # acks=all garantiza que el dato se replique en las 3 laptops para cumplir la tolerancia a fallos
            acks='all',
            retries=5
        )
        print("¡Conexión exitosa con el clúster de Kafka!")
    except Exception as e:
        print(f"Error al conectar con el clúster: {e}")
        return

    # Buscamos el dataset de 100,000 registros del Paso 1 en la carpeta compartida
    ruta_dataset = os.path.join("..", "..", "shared-data", "dataset.json")
    
    if not os.path.exists(ruta_dataset):
        print(f"Error: No se encontró el archivo en {ruta_dataset}")
        return

    print("Enviando ráfagas de datos a los 5 tópicos distribuidos...")
    conteo = 0
    tiempo_inicio = time.time()

    with open(ruta_dataset, 'r', encoding='utf-8') as f:
        for linea in f:
            if not linea.strip():
                continue
                
            # Cargamos la línea como un JSON (diccionario de la persona)
            registro = json.loads(linea.strip())
            
            # -----------------------------------------------------------------
            # CLASIFICACIÓN Y DISTRIBUCIÓN EN TIEMPO REAL (REQUERIMIENTO DEL PROFE)
            # -----------------------------------------------------------------
            
            # Tópico 1: Registro general de toda la población
            producer.send('personas-registro', value=registro)
            
            # Tópico 2: Filtro analítico de usuarios activos
            if registro.get("activo") == True:
                producer.send('personas-activas', value=registro)
                
            # Tópico 3: Segmentación financiera de ingresos altos (ingreso > 25,000)
            if registro.get("ingreso_mensual", 0) > 25000:
                producer.send('personas-ingresos', value=registro)
                
            # Tópico 4: Segmentación geográfica regional (Aguascalientes)
            if registro.get("estado") == "Aguascalientes":
                producer.send('personas-geografia', value=registro)
                
            # Tópico 5: Métricas de profesiones estratégicas de alta demanda
            if registro.get("ocupacion") in ["Ingeniero", "Desarrollador", "Médico"]:
                producer.send('personas-metricas', value=registro)

            conteo += 1
            if conteo % 10000 == 0:
                print(f"-> {conteo} registros enviados al clúster...")

    # Forzamos a que todo lo que esté en la memoria RAM salga disparado por el cable hacia las otras laps
    print("Vaciando buffers de red (Flush)...")
    producer.flush()
    producer.close()

    tiempo_total = time.time() - tiempo_inicio
    print("=========================================================")
    print(f"¡Éxito total! Se transmitieron los 100,000 registros.")
    print(f"Tiempo de transmisión distribuida: {round(tiempo_total, 2)} segundos.")
    print("=========================================================")

if __name__ == "__main__":
    iniciar_productor()