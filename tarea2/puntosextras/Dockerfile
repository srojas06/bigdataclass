# Dockerfile para contenedor principal con Spark y PostgreSQL
FROM openjdk:14-slim-buster

WORKDIR /src

RUN apt-get update && \
    apt-get install -y \
    bash \
    nano \
    postgresql postgresql-contrib \
    python3 \
    python3-pip \
    gcc \
    libpq-dev \
    wget \
    procps && \
    pip3 install --upgrade pip && \
    pip3 install pyspark pytest pyyaml psycopg2-binary && \
    ln -s /usr/bin/python3 /usr/bin/python

# Establecer el entorno JAVA
ENV JAVA_HOME=/opt/openjdk-14
ENV PATH="$JAVA_HOME/bin:$PATH"

# Descargar e instalar Apache Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz -O spark-3.4.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.4.0-bin-hadoop3.tgz -C /opt && \
    mv /opt/spark-3.4.0-bin-hadoop3 /opt/spark && \
    rm spark-3.4.0-bin-hadoop3.tgz

# Establecer el entorno SPARK
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# Copiar los archivos de trabajo al contenedor
COPY . /src

# Iniciar PostgreSQL
RUN service postgresql start

# Comando por defecto
CMD ["/bin/bash"]

