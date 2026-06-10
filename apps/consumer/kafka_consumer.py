import json
from kafka import KafkaConsumer

def iniciar_consumidor():
    # =========================================================================
    # CONFIGURACIÓN DEL ROL (¡Cambiar esto en cada laptop mañana!)
    # Opciones: 'OSVALDO', 'BRAYAN', 'OBED'
    # =========================================================================
    COMPAÑERO = 'OSVALDO' 

    # Mapeo de responsabilidades de tópicos por compañero para balancear la carga
    if COMPAÑERO == 'OSVALDO':
        topicos_asignados = ['personas-registro', 'personas-activas']
        group_id_asignado = 'grupo-analitica-osvaldo'
    elif COMPAÑERO == 'BRAYAN':
        topicos_asignados = ['personas-ingresos', 'personas-geografia']
        group_id_asignado = 'grupo-analitica-brayan'
    else:  # Tu rol: OBED
        topicos_asignados = ['personas-metricas']
        group_id_asignado = 'grupo-analitica-obed'

    print("=========================================================")
    print(f"Iniciando Consumidor de Kafka - Rol: [{COMPAÑERO}]")
    print(f"Escuchando tópicos: {topicos_asignados}")
    print(f"Group ID: {group_id_asignado}")
    print("=========================================================")

    BROKERS_CLUSTER = [
        '192.168.0.101:9092',
        '192.168.0.102:9092',
        '192.168.0.103:9092'
    ]

    try:
        # El consumidor se suscribe únicamente a sus tópicos asignados
        consumer = KafkaConsumer(
            *topicos_asignados,
            bootstrap_servers=BROKERS_CLUSTER,
            auto_offset_reset='earliest',  
            enable_auto_commit=True,       
            group_id=group_id_asignado,   
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )
        print("¡Conexión Exitosa! Esperando ráfaga de datos distribuidos...")
    except Exception as e:
        print(f"❌ Error al conectar el consumidor: {e}")
        return

    # Contadores analíticos locales
    totales = 0
    activos = 0
    ingresos_altos = 0
    en_aguascalientes = 0
    profesiones_clave = 0
    total_procesados_local = 0

    try:
        for mensaje in consumer:
            topico_origen = mensaje.topic
            total_procesados_local += 1
            
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

            # Reporte en consola dinámico cada 2,500 mensajes recibidos por este nodo
            if total_procesados_local % 2500 == 0:
                print("\n=========================================================")
                print(f"      MÉTRICAS PARCIALES EN TIEMPO REAL - NODO {COMPAÑERO} ")
                print("=========================================================")
                if 'personas-registro' in topicos_asignados:
                    print(f" -> [Tópico Registro] Total de Población: {totales}")
                if 'personas-activas' in topicos_asignados:
                    print(f" -> [Tópico Activos] Usuarios Disponibles: {activos}")
                if 'personas-ingresos' in topicos_asignados:
                    print(f" -> [Tópico Finanzas] Ingresos Mayores a $25K: {ingresos_altos}")
                if 'personas-geografia' in topicos_asignados:
                    print(f" -> [Tópico Geografía] Ubicados en Aguascalientes: {en_aguascalientes}")
                if 'personas-metricas' in topicos_asignados:
                    print(f" -> [Tópico Métricas] Profesiones Estratégicas: {profesiones_clave}")
                print(f" Total mensajes procesados por este script: {total_procesados_local}")
                print("=========================================================\n")

    except KeyboardInterrupt:
        print("\nConsumidor apagado manualmente.")
    finally:
        consumer.close()

if __name__ == "__main__":
    iniciar_consumidor()