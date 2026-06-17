#!/bin/bash

# 🔥 CORRECCIÓN CRÍTICA: Apuntamos localmente a la interfaz interna del contenedor.
# Como el comando se ejecuta 'dentro' de Docker, 127.0.0.1:9092 es la vía directa más rápida.
BROKER="127.0.0.1:9092"

echo "========================================================="
echo "Creando los Tópicos Distribuidos en Apache Kafka (KRaft)"
echo "========================================================="

crear_topico() {
    NOMBRE_TOPICO=$1
    echo "-> Creando tópico: ${NOMBRE_TOPICO}..."
    
    # Se usa docker exec apuntando a tu contenedor del nodo 3
    # Particiones: 3 (una para cada máquina física de la UAA)
    # Factor de replicación: 3 (los datos se copian en las 3 laptops por tolerancia a fallos)
    docker exec kafka-cluster-nodo-3 kafka-topics \
        --bootstrap-server $BROKER \
        --create \
        --topic $NOMBRE_TOPICO \
        --partitions 3 \
        --replication-factor 3
}

# --- TÓPICOS ALINEADOS A LOS REQUERIMIENTOS DEL PROFESOR ---
# Creación de los 3 tópicos principales para segmentar los datos masivos
crear_topico "datos-usuarios-zona1"
crear_topico "datos-usuarios-zona2"
crear_topico "datos-usuarios-zona3"

echo "========================================================="
echo "¡Estructura de tópicos inicializada con éxito!"
echo "========================================================="

echo "Tópicos activos en tu clúster actual:"
docker exec kafka-cluster-nodo-3 kafka-topics --bootstrap-server $BROKER --list