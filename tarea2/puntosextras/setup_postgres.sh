#!/bin/bash
set -e


service postgresql start

# Configuramos PostgreSQL
su - postgres -c "psql -c \"CREATE USER postgres WITH PASSWORD 'testPassword';\""
su - postgres -c "psql -c \"ALTER USER postgres WITH SUPERUSER;\""
su - postgres -c "psql -c \"CREATE DATABASE bigdata_db;\""

# Modificamos el archivo de configuración para permitir la autenticacionnn
echo "host all all 0.0.0.0/0 md5" >> /etc/postgresql/17/main/pg_hba.conf
echo "listen_addresses='*'" >> /etc/postgresql/17/main/postgresql.conf

# Reiniciamos el servicio para aplicar los cambios
service postgresql restart
