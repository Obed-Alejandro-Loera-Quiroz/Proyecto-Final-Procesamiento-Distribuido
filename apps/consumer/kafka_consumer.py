import json
from kafka import KafkaConsumer

def iniciar_consumidor():
    print("=========================================================")
    print("Iniciando Consumidor Analítico de Kafka (Escucha Activa)...")
    print("=========================================================")

    # Mismo mapa de IPs distribuidas para conectarse mañana en la escuela
    BROKERS_CLUSTER = [
        '192.168.0.101:9092',
        '192.168.0.102:9092',
        '192.168.0.103:9092'
    ]

    try:
        # El consumidor se suscribe a las 5 categorías al mismo tiempo
        consumer = KafkaConsumer(
            'personas-registro',
            'personas-activas',
            'personas-ingresos',
            'personas-geografia',
            'personas-metricas',
            bootstrap_servers=BROKERS_CLUSTER,
            auto_offset_reset='earliest',  # Lee desde el inicio del stream
            enable_auto_commit=True,       # Confirma lecturas automáticamente
            group_id='grupo-analitica-uaa', # ID del grupo de consumidores
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("¡Conexión Exitosa! Escuchando los datos del módem en tiempo real...")
    except Exception as e:
        print(f"Error al conectar el consumidor: {e}")
        return

    # Variables de control para las métricas analíticas
    totales = 0
    activos = 0
    ingresos_altos = 0
    en_aguascalientes = 0
    profesiones_clave = 0

    try:
        # Este ciclo se queda eternamente despierto atrapando lo que caiga en la red
        for mensaje in consumer:
            topico_origen = mensaje.topic
            
            # Sumamos al contador del tópico correspondiente
            if topico_origen == 'personas-registro':
                totales += 1
            elif topico_origen == 'personas-activas':
                activos += 1
            elif topico_origen == 'personas-ingresos':
                ingresos_altos += 1
            elif topico_origen == 'personas-geografia':
                en_aguascalientes += 1
            elif topico_origen == 'personas-metricas':
                profesiones_clave += 1

            # Cada 5,000 eventos procesados en total, arroja el reporte en la consola
            total_procesados = totales + activos + ingresos_altos + en_aguascalientes + profesiones_clave
            if total_procesados % 5000 == 0:
                print("\n=========================================================")
                print("         MÉTRICAS DEL CLÚSTER EN TIEMPO REAL             ")
                print("=========================================================")
                print(f" -> [Tópico Registro] Total de Población: {totales}")
                print(f" -> [Tópico Activos] Usuarios Disponibles: {activos}")
                print(f" -> [Tópico Finanzas] Ingresos Mayores a $25K: {ingresos_altos}")
                print(f" -> [Tópico Geografía] Ubicados en Aguascalientes: {en_aguascalientes}")
                print(f" -> [Tópico Métricas] Profesiones Estratégicas: {profesiones_clave}")
                print(f" Total de mensajes procesados en el bus de red: {total_procesados}")
                print("=========================================================\n")

    except KeyboardInterrupt:
        print("\nConsumidor apagado manualmente por el usuario.")
    finally:
        consumer.close()

if __name__ == "__main__":
    iniciar_consumidor()