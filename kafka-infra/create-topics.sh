#!/bin/bash

set -e

if [ ! -f .env ]; then
    echo "Error: no existe el archivo .env en kafka-infra"
    echo "Crea primero el .env correspondiente a esta laptop."
    exit 1
fi

set -a
source .env
set +a

BROKER="${BROKER:-127.0.0.1:9092}"
CONTAINER_NAME="kafka-cluster-nodo-${NODO_ID}"

echo "========================================================="
echo "CREACION DE TOPICOS - APACHE KAFKA KRAFT"
echo "========================================================="
echo "Nodo actual: ${NODO_ID}"
echo "IP actual: ${LAPTOP_IP}"
echo "Contenedor: ${CONTAINER_NAME}"
echo "Broker: ${BROKER}"
echo "========================================================="

echo ""
echo "Verificando contenedor Kafka..."

docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$" || {
    echo "Error: el contenedor ${CONTAINER_NAME} no esta activo."
    echo "Primero levanta Kafka con:"
    echo "docker compose up -d"
    exit 1
}

echo "Contenedor activo."

echo ""
echo "Esperando a que Kafka responda..."

for i in {1..20}; do
    if docker exec "${CONTAINER_NAME}" kafka-topics \
        --bootstrap-server "${BROKER}" \
        --list >/dev/null 2>&1; then
        echo "Kafka esta listo."
        break
    fi

    echo "Kafka aun no responde... intento ${i}/20"
    sleep 3

    if [ "$i" -eq 20 ]; then
        echo "Error: Kafka no respondio despues de varios intentos."
        exit 1
    fi
done

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
crear_topico "datos-usuarios-zona4"
crear_topico "datos-usuarios-zona5"

echo ""
echo "========================================================="
echo "TOPICOS CREADOS"
echo "========================================================="

echo ""
echo "Lista de topicos:"
docker exec "${CONTAINER_NAME}" kafka-topics \
    --bootstrap-server "${BROKER}" \
    --list

echo ""
echo "Descripcion de topicos del proyecto:"
docker exec "${CONTAINER_NAME}" kafka-topics \
    --bootstrap-server "${BROKER}" \
    --describe \
    --topic datos-usuarios-zona1

docker exec "${CONTAINER_NAME}" kafka-topics \
    --bootstrap-server "${BROKER}" \
    --describe \
    --topic datos-usuarios-zona2

docker exec "${CONTAINER_NAME}" kafka-topics \
    --bootstrap-server "${BROKER}" \
    --describe \
    --topic datos-usuarios-zona3

docker exec "${CONTAINER_NAME}" kafka-topics \
    --bootstrap-server "${BROKER}" \
    --describe \
    --topic datos-usuarios-zona4

docker exec "${CONTAINER_NAME}" kafka-topics \
    --bootstrap-server "${BROKER}" \
    --describe \
    --topic datos-usuarios-zona5

echo ""
echo "========================================================="
echo "PROCESO TERMINADO"
echo "========================================================="