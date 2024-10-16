FROM openjdk:14-slim-buster

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN apt-get update && \
    apt-get install -y bash nano postgresql-client python3 python3-pip gcc libpq-dev wget && \
    pip3 install --upgrade pip && \
    pip3 install pytest pyyaml psycopg2-binary && \
    ln -s /usr/bin/python3 /usr/bin/python

# Descargar y configurar Spark
RUN wget https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.4.0-bin-hadoop3.tgz -C /opt && \
    mv /opt/spark-3.4.0-bin-hadoop3 /opt/spark && \
    rm spark-3.4.0-bin-hadoop3.tgz

WORKDIR /src

COPY . /src
