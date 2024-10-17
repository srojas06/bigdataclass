# Utilizar la imagen de OpenJDK 14 como base
FROM openjdk:14-slim-buster

# Directorio de trabajo
WORKDIR /src

# Actualizar los repositorios e instalar las dependencias necesarias
RUN apt-get update && \
    apt-get install -y \
    gnupg \
    lsb-release \
    wget && \
    echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list && \
    wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    apt-get update

# Instalar dependencias necesarias para el proyecto
RUN apt-get install -y \
    bash \
    nano \
    postgresql-13 \
    python3 \
    python3-pip \
    gcc \
    libpq-dev \
    procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    pip3 install --upgrade pip && \
    pip3 install pyspark pytest pyyaml psycopg2-binary && \
    ln -s /usr/bin/python3 /usr/bin/python

# Descargar e instalar OpenJDK 14 si no está ya presente
RUN wget https://github.com/AdoptOpenJDK/openjdk14-binaries/releases/download/jdk-14.0.2%2B12/OpenJDK14U-jdk_x64_linux_hotspot_14.0.2_12.tar.gz -O openjdk-14.0.2_linux-x64_bin.tar.gz && \
    mkdir -p /opt/openjdk-14 && \
    tar -xzf openjdk-14.0.2_linux-x64_bin.tar.gz -C /opt/openjdk-14 --strip-components=1 && \
    rm openjdk-14.0.2_linux-x64_bin.tar.gz

# Establecer JAVA_HOME y agregarlo al PATH
ENV JAVA_HOME=/opt/openjdk-14
ENV PATH="$JAVA_HOME/bin:$PATH"

# Verificar que JAVA_HOME esté configurado correctamente
RUN echo "JAVA_HOME is set to: $JAVA_HOME" && \
    $JAVA_HOME/bin/java -version

# Descargar e instalar Apache Spark desde un espejo de la versión oficial
RUN wget https://archive.apache.org/dist/spark/spark-3.4.0/spark-3.4.0-bin-hadoop3.tgz -O spark-3.4.0-bin-hadoop3.tgz && \
    tar -xzf spark-3.4.0-bin-hadoop3.tgz -C /opt && \
    mv /opt/spark-3.4.0-bin-hadoop3 /opt/spark && \
    rm spark-3.4.0-bin-hadoop3.tgz

# Establecer el SPARK_HOME y agregar Spark al PATH
ENV SPARK_HOME=/opt/spark
ENV PATH="$SPARK_HOME/bin:$PATH"

# Copiar los archivos de trabajo al contenedor
COPY . /src

# Exponer el puerto para PostgreSQL
EXPOSE 5432

# Comando por defecto para mantener el contenedor ejecutándose
CMD ["postgres"]

