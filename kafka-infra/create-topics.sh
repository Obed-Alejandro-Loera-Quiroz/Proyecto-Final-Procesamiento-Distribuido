#!/bin/bash

# Apuntamos a tu IP local fija del módem
BROKER="192.168.0.103:9092"

echo "========================================================="
echo "Creando los 5 Tópicos Distribuidos en Apache Kafka (KRaft)"
echo "========================================================="

crear_topico() {
    NOMBRE_TOPICO=$1
    echo "-> Creando tópico: ${NOMBRE_TOPICO}..."
    
    # Se usa docker exec directo apuntando al contenedor real: kafka-cluster-nodo-3
    docker exec kafka-cluster-nodo-3 kafka-topics \
        --bootstrap-server $BROKER \
        --create \
        --topic $NOMBRE_TOPICO \
        --partitions 3 \
        --replication-factor 3
}

# --- LOS 5 TÓPICOS REQUERIDOS ALINEADOS A LA GENERACIÓN FAKER ---
crear_topico "personas-bloque-A"
crear_topico "personas-bloque-B"
crear_topico "personas-bloque-C"
crear_topico "personas-bloque-D"
crear_topico "personas-bloque-E"

echo "========================================================="
echo "¡Estructura de 5 tópicos inicializada con éxito!"
echo "========================================================="

echo "Tópicos activos en tu clúster actual:"
docker exec kafka-cluster-nodo-3 kafka-topics --bootstrap-server $BROKER --list