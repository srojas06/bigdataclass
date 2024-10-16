FROM openjdk:14-alpine

# Instalar las herramientas necesarias
RUN apk add bash && \
  apk add nano && \
  apk add postgresql-client && \
  apk add python3 && \
  apk add gcc musl-dev postgresql-dev && \  # Necesario para compilar psycopg2
  pip3 install --upgrade pip && \
  pip3 install pyspark && \
  pip3 install pytest && \
  pip3 install pyyaml && \
  pip3 install psycopg2-binary && \  # Instalar psycopg2-binary para evitar problemas de dependencias
  ln -s /usr/bin/python3 /usr/bin/python

# Configurar variables de entorno
ENV JAVA_HOME=/usr/lib/jvm/default-jvm
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# Establecer el directorio de trabajo
WORKDIR /src

# Copiar todos los archivos al contenedor
COPY . /src

