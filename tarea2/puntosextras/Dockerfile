FROM openjdk:14

ENV JAVA_HOME=/usr/local/openjdk-14
ENV SPARK_HOME=/usr/local/lib/python3.7/site-packages/pyspark
ENV PATH=$JAVA_HOME/bin:$PATH

RUN apt-get update && apt-get install -y \
    bash \
    nano \
    postgresql-client \
    python3 \
    python3-pip \
    gcc \
    libpq-dev && \
    pip3 install --upgrade pip && \
    pip3 install pyspark pytest pyyaml psycopg2 && \
    ln -s /usr/bin/python3 /usr/bin/python

WORKDIR /src

COPY . /src
