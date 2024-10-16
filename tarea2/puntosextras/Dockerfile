FROM openjdk:14-jdk

# Establecer las variables de entorno
ENV JAVA_HOME=/usr/local/openjdk-14
ENV SPARK_HOME=/usr/local/lib/python3.7/site-packages/pyspark
ENV PATH=$JAVA_HOME/bin:$PATH

# Instalar herramientas necesarias, incluyendo procps para que el comando ps funcione
RUN apt-get update && apt-get install -y \
    bash \
    nano \
    postgresql-client \
    python3 \
    python3-pip \
    gcc \
    libpq-dev \
    procps && \  # Instalar procps para solucionar el problema con el comando `ps`
    pip3 install --upgrade pip && \
    pip3 install pyspark pytest pyyaml psycopg2-binary && \  # Instalar psycopg2-binary en lugar de psycopg2
    ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /src

COPY . /src

