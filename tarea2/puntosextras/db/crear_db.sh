#!/bin/bash 
set -e

# Eliminar cualquier contenedor anterior llamado bigdata-db si existe
docker rm -f bigdata-db || true

# Crear y ejecutar un nuevo contenedor PostgreSQL
docker run --name bigdata-db \
  -e POSTGRES_PASSWORD=testPassword \
  -p 5433:5432 \
  -d postgres

# Esperar unos segundos para que PostgreSQL se inicie completamente
echo "Esperando a que el contenedor PostgreSQL se inicie..."
sleep 10

# Crear la base de datos y las tablas necesarias usando el archivo initialize.sql
docker exec -i bigdata-db psql -U postgres -f /initialize.sql
