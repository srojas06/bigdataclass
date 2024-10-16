FROM python:3.7-alpine

# Instalar dependencias necesarias
RUN apk update && \
    apk add --no-cache bash nano postgresql-client gcc musl-dev postgresql-dev krb5-libs krb5

# Instalar las bibliotecas de Python
RUN pip3 install --upgrade pip && \
    pip3 install pyspark pytest pyyaml psycopg2-binary

# Establecer el directorio de trabajo
WORKDIR /src

# Copiar todos los archivos a la imagen de Docker
COPY . /src
