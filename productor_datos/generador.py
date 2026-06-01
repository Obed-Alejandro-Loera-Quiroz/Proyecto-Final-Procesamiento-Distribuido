import json
import time
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

# 1. Inicializamos el generador de datos falsos
fake = Faker('es_MX')

# 2. Conectamos el script con nuestro contenedor de Kafka local
# Usamos localhost:9092 porque estamos haciendo pruebas internas en tu compu
try:
    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
    )
    print("Conectado con éxito a Apache Kafka local.\n")
except Exception as e:
    print(f"Error al conectar con Kafka: {e}")
    print("Asegúrate de que el contenedor de Kafka esté encendido.")
    exit(1)

print("--- Iniciando envío de datos en tiempo real (Streaming) ---\n")

# Simulamos enviar 20 registros de prueba para validar la conexión
for i in range(1, 21):
    transaccion = {
        "id_transaccion": i,
        "id_usuario": fake.random_int(min=1000, max=9999),
        "nombre": fake.name(),
        "monto": round(fake.pyfloat(left_digits=3, right_digits=2, positive=True, min_value=10, max_value=500), 2),
        "ciudad": fake.city(),
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }
    
    # Mandamos el dato al tópico llamado 'transacciones'
    producer.send('transacciones', value=transaccion)
    
    print(f"Registro #{i} enviado al tópico 'transacciones'")
    
    # Esperamos medio segundo entre registros para simular el flujo continuo
    time.sleep(0.5)

# Aseguramos que se envíen todos los datos antes de cerrar
producer.flush()
print("\n--- Envío de prueba finalizado con éxito ---")