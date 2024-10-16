#!/bin/bash
# Eliminar cualquier contenedor anterior llamado bigdata-db
docker rm -f bigdata-db

# Crear y ejecutar un contenedor PostgreSQL
docker run --name bigdata-db \
  -e POSTGRES_PASSWORD=testPassword \
  -p 5433:5432 \
  -d postgres
