#!/bin/bash
set -e

# Iniciar el servicio de PostgreSQL
service postgresql start

# Configurar PostgreSQL: crear base de datos, usuarios y establecer permisos
su - postgres -c "psql -c \"CREATE USER postgres WITH PASSWORD 'testPassword';\""
su - postgres -c "psql -c \"ALTER USER postgres WITH SUPERUSER;\""
su - postgres -c "psql -c \"CREATE DATABASE bigdata_db;\""

# Modificar el archivo de configuración para permitir la autenticación
echo "host all all 0.0.0.0/0 md5" >> /etc/postgresql/17/main/pg_hba.conf
echo "listen_addresses='*'" >> /etc/postgresql/17/main/postgresql.conf

# Reiniciar el servicio para aplicar los cambios
service postgresql restart
