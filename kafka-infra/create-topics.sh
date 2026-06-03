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

# --- LOS 5 TÓPICOS SOLICITADOS ---
crear_topico "personas-registro"
crear_topico "personas-activas"
crear_topico "personas-ingresos"
crear_topico "personas-geografia"
crear_topico "personas-metricas"

echo "========================================================="
echo "¡Estructura de tópicos inicializada localmente!"
echo "========================================================="

echo "Tópicos activos en tu clúster actual:"
docker exec kafka-cluster-nodo-3 kafka-topics --bootstrap-server $BROKER --list