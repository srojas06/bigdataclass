FROM openjdk:14-alpine

# Establecer el entorno para Java y Spark
ENV SPARK_HOME=/usr/local/lib/python3.7/site-packages/pyspark
ENV JAVA_HOME=/usr/lib/jvm/java-1.14-openjdk
ENV PATH="${JAVA_HOME}/bin:${PATH}"

RUN apk add --no-cache bash nano postgresql-client python3 \
  gcc musl-dev postgresql-dev openjdk14 \
  && pip3 install --upgrade pip \
  && pip3 install pyspark pytest pyyaml psycopg2-binary \
  && ln -sf /usr/bin/python3 /usr/bin/python

WORKDIR /src

COPY . /src
