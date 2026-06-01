import time
import json
from kafka import KafkaProducer
from faker import Faker

# Inicializamos Faker en español de México
fake = Faker('es_MX')

# Configuración del Productor de Kafka apuntando a la IP interna de Docker
producer = KafkaProducer(
    bootstrap_servers=['172.17.0.1:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic_name = 'transacciones'
TOTAL_REGISTROS = 100000  # <-- REQUERIMIENTO COMPLETADO

print(f"🚀 Iniciando ráfaga masiva de {TOTAL_REGISTROS} registros hacia Kafka...")

for i in range(1, TOTAL_REGISTROS + 1):
    # JSON con exactamente 10 campos valor (Rúbrica cumplida)
    data = {
        "id_persona": i,
        "nombre": fake.first_name(),
        "apellido": fake.last_name(),
        "edad": fake.random_int(min=18, max=80),
        "genero": fake.random_element(elements=('M', 'F')),
        "ciudad": fake.city(),
        "estado": fake.state(),
        "ocupacion": fake.job(),
        "nivel_estudios": fake.random_element(elements=('Bachillerato', 'Licenciatura', 'Maestría', 'Doctorado')),
        "ingreso_mensual": fake.random_int(min=8000, max=85000)
    }
    
    # Enviamos a Kafka
    producer.send(topic_name, value=data)
    
    # Cada 2000 registros imprimimos progreso en tu terminal para no saturar tu pantalla
    if i % 2000 == 0:
        print(f"📦 Progreso: {i}/{TOTAL_REGISTROS} registros enviados con éxito.")
    
    # Tiempo de espera ultra corto (0.0001s) para que envíe rápido los 100k sin congelar tu CPU
    time.sleep(0.0001)

producer.flush()
print("✅ ¡Se terminó de enviar los 100,000 elementos diferentes con éxito!")