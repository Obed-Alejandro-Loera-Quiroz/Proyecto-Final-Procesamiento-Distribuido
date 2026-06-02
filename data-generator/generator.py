import json
import random
from datetime import datetime, timedelta
import os
from faker import Faker

def generar_dataset_con_faker(total_registros=100000):
    print(f"Iniciando la generación de {total_registros} registros usando Faker...")

    # Inicializamos Faker configurado para México
    fake = Faker('es_MX')

    # Ciudades y sus estados correspondientes
    ubicaciones = [
        {"ciudad": "Aguascalientes", "estado": "Aguascalientes"},
        {"ciudad": "Jesús María", "estado": "Aguascalientes"},
        {"ciudad": "Calvillo", "estado": "Aguascalientes"},
        {"ciudad": "Guadalajara", "estado": "Jalisco"},
        {"ciudad": "Monterrey", "estado": "Nuevo León"},
        {"ciudad": "CDMX", "estado": "CDMX"},
        {"ciudad": "Querétaro", "estado": "Querétaro"},
        {"ciudad": "Zacatecas", "estado": "Zacatecas"},
        {"ciudad": "San Luis Potosí", "estado": "San Luis Potosí"},
        {"ciudad": "León", "estado": "Guanajuato"}
    ]
    
    ocupaciones = ["Estudiante", "Ingeniero", "Maestro", "Médico", "Abogado", "Comerciante", "Administrador", "Contador", "Desarrollador", "Desempleado"]
    niveles_estudio = ["Secundaria", "Preparatoria", "Licenciatura", "Maestría", "Doctorado"]

    # Definimos la ruta de salida en el Monorepo
    ruta_salida = os.path.join("..", "shared-data")
    if not os.path.exists(ruta_salida):
        os.makedirs(ruta_salida)
        
    archivo_final = os.path.join(ruta_salida, "dataset.json")
    start_time = datetime.now()

    with open(archivo_final, 'w', encoding='utf-8') as f:
        for i in range(1, total_registros + 1):
            # Faker genera dinámicamente perfiles realistas
            genero = random.choice(["F", "M"])
            if genero == "F":
                nombre = fake.first_name_female()
            else:
                nombre = fake.first_name_male()
                
            # Generamos dos apellidos reales mexicanos
            apellido = f"{fake.last_name()} {fake.last_name()}"
            
            edad = random.randint(18, 70)
            ubica = random.choice(ubicaciones)
            ocupacion = random.choice(ocupaciones) if edad > 23 else "Estudiante"
            nivel = random.choice(niveles_estudio)
            
            # Lógica para ingresos lógicos basados en ocupación
            if ocupacion == "Estudiante":
                ingreso = round(random.uniform(1500.0, 5000.0), 2)
                nivel = random.choice(["Preparatoria", "Licenciatura"])
            elif nivel in ["Maestría", "Doctorado"]:
                ingreso = round(random.uniform(25000.0, 80000.0), 2)
            else:
                ingreso = round(random.uniform(8000.0, 30000.0), 2)
                
            activo = random.choice([True, False])
            
            # Faker genera fechas aleatorias en un rango determinado
            fecha_reg = fake.date_time_between(start_date='-2y', end_date='now').strftime("%Y-%m-%d %H:%M:%S")

            # Construcción del registro (12 campos en total)
            registro = {
                "id_persona": i,
                "nombre": nombre,
                "apellido": apellido,
                "edad": edad,
                "genero": genero,
                "ciudad": ubica["ciudad"],
                "estado": ubica["estado"],
                "ocupacion": ocupacion,
                "nivel_estudios": nivel,
                "ingreso_mensual": ingreso,
                "activo": activo,
                "fecha_registro": fecha_reg
            }
            
            f.write(json.dumps(registro, ensure_ascii=False) + "\n")
            
            if i % 20000 == 0:
                print(f"Progreso: {i} registros generados...")

    end_time = datetime.now()
    print(f"\n¡Éxito! Archivo guardado con Faker en: {archivo_final}")
    print(f"Tiempo total de generación: {end_time - start_time} segundos.")

if __name__ == "__main__":
    generar_dataset_con_faker(100000)