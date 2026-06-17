#!/bin/bash

set -e

if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

BROKER="${BROKER:-127.0.0.1:9092}"
NODO_ID="${NODO_ID:-3}"
CONTAINER_NAME="${CONTAINER_NAME:-kafka-cluster-nodo-${NODO_ID}}"

echo "========================================================="
echo "Creando topicos distribuidos en Apache Kafka KRaft"
echo "========================================================="
echo "Contenedor usado: ${CONTAINER_NAME}"
echo "Broker usado: ${BROKER}"
echo "========================================================="

echo "Verificando contenedor..."
docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$" || {
    echo "Error: el contenedor ${CONTAINER_NAME} no esta activo."
    echo "Primero ejecuta: docker compose up -d"
    exit 1
}

crear_topico() {
    NOMBRE_TOPICO=$1

    echo ""
    echo "Creando topico: ${NOMBRE_TOPICO}"

    docker exec "${CONTAINER_NAME}" kafka-topics \
        --bootstrap-server "${BROKER}" \
        --create \
        --if-not-exists \
        --topic "${NOMBRE_TOPICO}" \
        --partitions 3 \
        --replication-factor 3
}

crear_topico "datos-usuarios-zona1"
crear_topico "datos-usuarios-zona2"
crear_topico "datos-usuarios-zona3"

echo ""
echo "========================================================="
echo "Topicos inicializados."
echo "========================================================="

echo ""
echo "Lista de topicos activos:"
docker exec "${CONTAINER_NAME}" kafka-topics \
    --bootstrap-server "${BROKER}" \
    --list

echo ""
echo "Descripcion de topicos:"
docker exec "${CONTAINER_NAME}" kafka-topics \
    --bootstrap-server "${BROKER}" \
    --describe

echo ""
echo "========================================================="
echo "Proceso terminado."
echo "========================================================="