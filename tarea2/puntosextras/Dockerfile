# Usar una imagen base con Java 14 para Apache Spark
FROM openjdk:14-alpine

# Establecer el valor de la variable JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-14-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Definir el directorio donde se ejecutará el contenedor
WORKDIR /src

# Instalar las dependencias necesarias
RUN apk add --no-cache \
    bash \
    nano \
    postgresql-client \
    python3 \
    py3-pip \
    gcc \
    musl-dev \
    postgresql-dev \
    openjdk14 && \
    pip3 install --upgrade pip && \
    pip3 install pyspark pytest pyyaml psycopg2-binary && \
    ln -s /usr/bin/python3 /usr/bin/python

# Copiar todos los archivos al directorio de trabajo del contenedor
COPY . /src

# Definir el comando de inicio por defecto (bash)
CMD ["bash"]
