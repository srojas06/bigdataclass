FROM openjdk:14-jdk-alpine

# Establecer variables de entorno para Java y Spark
ENV JAVA_HOME=/usr/lib/jvm/java-1.14-openjdk
ENV SPARK_HOME=/usr/lib/python3.7/site-packages/pyspark
ENV PATH=$JAVA_HOME/bin:$PATH

# Instalar herramientas necesarias, incluyendo procps
RUN apk add --no-cache \
    bash \
    nano \
    postgresql-client \
    python3 \
    py3-pip \
    gcc \
    musl-dev \
    postgresql-dev \
    procps && \  # Añadimos procps para corregir el error con ps
    pip3 install --upgrade pip && \
    pip3 install pyspark pytest pyyaml psycopg2-binary && \  # Cambiado psycopg2 por psycopg2-binary
    ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /src

COPY . /src
